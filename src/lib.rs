mod disk_table;
mod kmerge_join;
mod mem_table;
mod observable_set;

use std::collections::BTreeSet;

use crate::disk_table::DiskTable;
use crate::kmerge_join::KMergeJoinBy;
use core::mem;

use crate::observable_set::ObservableSet;
use crossbeam_channel::{unbounded, Sender};
use mem_table::MemTable;
use std::fs::{create_dir_all, File};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock, RwLockWriteGuard};
use std::thread;
use uuid::Uuid;

#[derive(Clone)]
pub struct MemTableConfig {
    pub max_size: usize, // max size of mem table in bytes
}

impl MemTableConfig {
    pub fn default() -> MemTableConfig {
        MemTableConfig {
            max_size: 1_000_000,
        }
    }

    pub fn max_size(mut self, size: usize) -> Self {
        self.max_size = size;
        self
    }
}

#[derive(Clone)]
pub struct DiskTableConfig {
    pub block_size: usize, // max size of mem table in bytes
}

impl DiskTableConfig {
    pub fn default() -> DiskTableConfig {
        DiskTableConfig {
            block_size: 64 * 1024,
        }
    }

    pub fn block_size(mut self, block_size: usize) -> Self {
        self.block_size = block_size;
        self
    }
}

#[derive(Clone)]
pub struct DatabaseConfig {
    pub mem_table_config: MemTableConfig,
    pub disk_table_config: DiskTableConfig,
    pub directory: Option<PathBuf>,
    pub num_flushing_threads: usize,
}

impl DatabaseConfig {
    pub fn default() -> DatabaseConfig {
        DatabaseConfig {
            mem_table_config: MemTableConfig::default(),
            disk_table_config: DiskTableConfig::default(),
            directory: None,
            num_flushing_threads: 2,
        }
    }

    pub fn mem_table_config(mut self, mem_table_config: MemTableConfig) -> Self {
        self.mem_table_config = mem_table_config;
        self
    }

    pub fn disk_table_config(mut self, disk_table_config: DiskTableConfig) -> Self {
        self.disk_table_config = disk_table_config;
        self
    }

    pub fn directory(mut self, directory: PathBuf) -> Self {
        self.directory = Some(directory);
        self
    }

    pub fn num_flushing_threads(mut self, num_flushing_threads: usize) -> Self {
        self.num_flushing_threads = num_flushing_threads;
        self
    }
}

pub struct Database {
    mem_table: RwLock<MemTable>,
    full_mem_tables: RwLock<BTreeSet<Arc<MemTable>>>,
    disk_tables: RwLock<BTreeSet<DiskTable>>,
    compaction_lock: Mutex<u8>,
    config: DatabaseConfig,
    age: AtomicU64,
    flushing_channel: Sender<Arc<MemTable>>,
    finished_flushing: ObservableSet,
}

impl Database {
    pub fn open() -> Arc<Database> {
        Database::open_with_config(DatabaseConfig::default())
    }

    pub fn open_with_config(config: DatabaseConfig) -> Arc<Database> {
        let (sender, receiver) = unbounded();
        let db = Arc::new(Database {
            mem_table: RwLock::new(MemTable::new(config.mem_table_config.clone(), 0)),
            full_mem_tables: RwLock::new(BTreeSet::new()),
            disk_tables: RwLock::new(BTreeSet::new()),
            compaction_lock: Mutex::new(0),
            config,
            age: AtomicU64::new(0),
            flushing_channel: sender,
            finished_flushing: ObservableSet::new(),
        });
        // Create directory
        if !db.is_read_only() {
            create_dir_all(db.config.directory.clone().unwrap().as_path())
                .expect("Failed to create dirs");
        }
        for _ in 0..db.config.num_flushing_threads {
            let r = receiver.clone();
            let db_ref = db.clone();
            thread::spawn(move || loop {
                let result = r.recv();
                if result.is_err() {
                    return;
                }
                db_ref.flush_mem_table(result.unwrap());
            });
        }
        db
    }

    fn swap_new_mem_table<'a>(&self, map: &mut RwLockWriteGuard<MemTable>) -> Arc<MemTable> {
        let mut full_maps = self.full_mem_tables.write().expect("RwLock poisoned");
        let new_map = MemTable::new(
            map.config.clone(),
            self.age.fetch_add(1, Ordering::SeqCst) + 1,
        );
        let old_map: MemTable = mem::replace(&mut (*map), new_map);
        let old_map_arc = Arc::new(old_map);
        full_maps.insert(old_map_arc.clone());
        if !self.is_read_only() {
            self.flushing_channel.send(old_map_arc.clone());
        }
        old_map_arc
    }

    pub fn insert<K: AsRef<[u8]>, V: AsRef<[u8]>>(&self, key: K, value: V) {
        let mut map = self.mem_table.write().expect("RwLock poisoned");
        while !map.insert(
            key.as_ref().to_vec().into_boxed_slice(),
            value.as_ref().to_vec().into_boxed_slice(),
        ) {
            self.swap_new_mem_table(&mut map);
        }
    }

    pub fn get<K: AsRef<[u8]>>(&self, key: K) -> Option<Box<[u8]>> {
        let map = self.mem_table.read().expect("RwLock poisoned");
        let key_ref = key.as_ref();
        map.get(key_ref)
            .or_else(|| {
                {
                    let full_maps = self.full_mem_tables.read().expect("RwLock poisoned");
                    for full_map in full_maps.iter() {
                        let ret = full_map.get(key_ref);
                        if ret.is_some() {
                            return ret;
                        }
                    }
                }
                {
                    let disk_tables = self.disk_tables.read().expect("RwLock poisoned");
                    for disk_table in disk_tables.iter() {
                        let ret = disk_table.get(key_ref);
                        if ret.is_some() {
                            return ret;
                        }
                    }
                }
                None
            })
            .map(|v| v.clone())
    }

    fn is_read_only(&self) -> bool {
        self.config.directory.is_none() // TODO also have an read only config option so we can support readonly on disk
    }

    fn new_sst(&self) -> PathBuf {
        let mut new_file = self.config.directory.clone().unwrap();
        new_file.push(Uuid::new_v4().to_string() + ".sst");
        return new_file;
    }

    fn flush_mem_table(&self, mem_table: Arc<MemTable>) {
        // Creating the disk table should not happen while list needed for gets are locked
        let disk_table = DiskTable::create(
            self.new_sst().as_path(),
            mem_table.iter(),
            &self.config.disk_table_config,
            mem_table.age,
        );

        // Lock both and swap
        let mut disk_tables = self.disk_tables.write().expect("RWLock poisoned");
        let mut mem_tables = self.full_mem_tables.write().expect("RWLock poisoned");
        disk_tables.insert(disk_table);
        mem_tables.remove(&mem_table);
        self.finished_flushing.add(mem_table.age);
    }

    pub fn flush(&self) {
        if self.is_read_only() {
            return;
        }
        self.flush_internal();
    }

    fn flush_internal(&self) -> Option<u64> {
        let age = {
            // Flush in progress mem table
            let mut map = self.mem_table.write().expect("RwLock poisoned");
            if map.len() > 0 {
                self.swap_new_mem_table(&mut map).age
            } else {
                if map.age == 0 {
                    return None;
                } else {
                    map.age - 1
                }
            }
        };
        self.finished_flushing.wait_for(age);
        Some(age)
    }

    pub fn compact(&self) {
        let age_option = self.flush_internal();
        if age_option.is_none() {
            return;
        }
        let age = age_option.unwrap();
        let _flush_guard = self.compaction_lock.lock().expect("Lock Poisoned");

        // Create compacted file
        let disk_tables = self.disk_tables.read().expect("RWLock poisoned");
        if disk_tables.len() == 0 {
            return;
        }
        let iter = disk_tables
            .iter()
            .filter(|t| t.age <= age)
            .map(|t| t.iter())
            .kmerge_join_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
        let mut merged = DiskTable::create(
            self.new_sst().as_path(),
            iter,
            &self.config.disk_table_config,
            age,
        );
        drop(disk_tables);

        let mut disk_tables = self.disk_tables.write().expect("RWLock poisoned");
        for to_delete in disk_tables.iter() {
            to_delete.delete();
        }
        disk_tables.clear();
        disk_tables.insert(merged);
    }
}

#[cfg(test)]
mod tests {
    use crate::{mem_table, Database, DatabaseConfig, DiskTableConfig, MemTableConfig};
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn basic() {
        let db = Database::open();
        let key = vec![8];
        let value = vec![9, 10];
        db.insert(&key, &value);
        let retrieved_value = db.get(&key);
        assert_eq!(retrieved_value.as_ref().map(|v| &v[..]), Some(&value[..]));
    }

    #[test]
    fn basic_strings() {
        let db = Database::open();
        let key = String::from("key");
        let value = String::from("value");
        db.insert(key.as_bytes(), value.as_bytes());
        assert_eq!(
            db.get(key.as_bytes()).as_ref().map(|v| &(v[..])),
            Some(value.as_bytes())
        );
    }

    #[test]
    fn overwriting() {
        let mut directory = PathBuf::new();
        directory.push("test_db");
        let db = Database::open_with_config(DatabaseConfig::default().directory(directory));
        let key = String::from("key");

        for v in 0..9 {
            let value = v.to_string();
            db.insert(key.as_bytes(), value.as_bytes());
            db.flush();
            assert_eq!(
                db.get(key.as_bytes())
                    .as_ref()
                    .map(|v| String::from_utf8(v.to_vec()).unwrap()),
                Some(value)
            );
        }

        let value = 5.to_string();
        db.insert(key.as_bytes(), value.as_bytes());
        db.flush();
        assert_eq!(
            db.get(key.as_bytes())
                .as_ref()
                .map(|v| String::from_utf8(v.to_vec()).unwrap()),
            Some(value.clone())
        );

        db.compact();
        assert_eq!(
            db.get(key.as_bytes())
                .as_ref()
                .map(|v| String::from_utf8(v.to_vec()).unwrap()),
            Some(value)
        );
    }

    #[test]
    fn multi_threaded() {
        const NTHREADS: u32 = 10;
        let mut threads = vec![];

        let db = Database::open();
        for i in 0..NTHREADS {
            let db_ref = db.clone();
            threads.push(thread::spawn(move || {
                let value = String::from("value");
                db_ref.insert(i.to_be_bytes(), value.as_bytes());
            }));
        }

        for thread in threads {
            let _ = thread.join();
        }
    }

    #[test]
    fn overflow_mem_table() {
        let mem_table_config = MemTableConfig::default().max_size(20);
        let db = Database::open_with_config(
            DatabaseConfig::default().mem_table_config(mem_table_config),
        );

        let first_key = String::from("first_key");
        let first_value = String::from("first_value");
        db.insert(first_key.as_bytes(), first_value.as_bytes());

        for i in 0..100 {
            let key = format!("key{}", i);
            let value = format!("value{}", i);
            db.insert(key.as_bytes(), value.as_bytes());
            assert_eq!(
                db.get(key.as_bytes()).as_ref().map(|v| &(v[..])),
                Some(value.as_bytes())
            );
            assert_eq!(
                db.get(first_key.as_bytes()).as_ref().map(|v| &(v[..])),
                Some(first_value.as_bytes())
            );
        }
    }

    #[test]
    fn disk() {
        let mem_table_config = MemTableConfig::default().max_size(20);
        let disk_table_config = DiskTableConfig::default().block_size(1024);
        let mut directory = PathBuf::new();
        directory.push("test_db");
        let db = Database::open_with_config(
            DatabaseConfig::default()
                .mem_table_config(mem_table_config)
                .disk_table_config(disk_table_config)
                .directory(directory),
        );

        // Make sure compaction of empty db doesn't do anything
        db.compact();
        {
            let disk_tables = db.disk_tables.read().expect("RWLock poisoned");
            assert_eq!(disk_tables.len(), 0);
        }

        let first_key = String::from("first_key");
        let first_value = String::from("first_value");
        db.insert(first_key.as_bytes(), first_value.as_bytes());

        for i in 0..100 {
            let key = format!("key{}", i);
            let value = format!("value{}", i);
            db.insert(key.as_bytes(), value.as_bytes());
            assert_eq!(
                db.get(key.as_bytes()).as_ref().map(|v| &(v[..])),
                Some(value.as_bytes())
            );
            assert_eq!(
                db.get(first_key.as_bytes()).as_ref().map(|v| &(v[..])),
                Some(first_value.as_bytes())
            );
        }

        db.flush();

        assert_eq!(
            db.get(first_key.as_bytes()).as_ref().map(|v| &(v[..])),
            Some(first_value.as_bytes())
        );
        for i in 0..100 {
            let key = format!("key{}", i);
            let value = format!("value{}", i);
            assert_eq!(
                db.get(key.as_bytes()).as_ref().map(|v| &(v[..])),
                Some(value.as_bytes())
            );
        }

        db.compact();
        {
            let disk_tables = db.disk_tables.read().expect("RWLock poisoned");
            assert_eq!(disk_tables.len(), 1);
        }

        assert_eq!(
            db.get(first_key.as_bytes()).as_ref().map(|v| &(v[..])),
            Some(first_value.as_bytes())
        );
        for i in 0..100 {
            let key = format!("key{}", i);
            let value = format!("value{}", i);
            assert_eq!(
                db.get(key.as_bytes()).as_ref().map(|v| &(v[..])),
                Some(value.as_bytes())
            );
        }
    }
}
