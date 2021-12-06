use core::mem;
use std::collections::BTreeSet;
use std::fs::create_dir_all;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock, RwLockWriteGuard};
use std::{fs, thread};

use crossbeam_channel::{unbounded, Sender};
use uuid::Uuid;

use error::Result;
use mem_table::MemTable;

use crate::disk_table::DiskTable;
use crate::kmerge_join::KMergeJoinBy;
use crate::observable_set::ObservableSet;

mod disk_table;
pub mod error;
mod kmerge_join;
mod mem_table;
mod observable_set;

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
    pub max_mem_tables: usize,
}

impl DatabaseConfig {
    pub fn default() -> DatabaseConfig {
        DatabaseConfig {
            mem_table_config: MemTableConfig::default(),
            disk_table_config: DiskTableConfig::default(),
            directory: None,
            num_flushing_threads: 2,
            max_mem_tables: 10,
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

    pub fn max_mem_tables(mut self, max_mem_tables: usize) -> Self {
        self.max_mem_tables = max_mem_tables;
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
    pub fn open() -> Result<Arc<Database>> {
        Database::open_with_config(DatabaseConfig::default())
    }

    pub fn open_with_config(config: DatabaseConfig) -> Result<Arc<Database>> {
        let mut disk_tables = BTreeSet::new();
        let mut age = 0;
        if let Some(directory) = config.directory.clone() {
            // Look for existing .sst files
            if let Ok(files) = fs::read_dir(directory.as_path()) {
                files.filter_map(|f| f.ok()).for_each(|f| {
                    if f.path().extension().and_then(|s| s.to_str()) == Some("sst") {
                        let disk_table = DiskTable::open(f.path().as_path()).unwrap();
                        if disk_table.age >= age {
                            age = disk_table.age + 1;
                        }
                        disk_tables.insert(disk_table);
                    }
                });
            }
        }
        let finished_flushing = if age > 0 {
            ObservableSet::new_with_value(age - 1)
        } else {
            ObservableSet::new()
        };

        let (sender, receiver) = unbounded();
        let db = Arc::new(Database {
            mem_table: RwLock::new(MemTable::new(config.mem_table_config.clone(), age)),
            full_mem_tables: RwLock::new(BTreeSet::new()),
            disk_tables: RwLock::new(disk_tables),
            compaction_lock: Mutex::new(0),
            config,
            age: AtomicU64::new(age),
            flushing_channel: sender,
            finished_flushing,
        });
        // Create directory
        if !db.is_read_only() {
            // Create directory if needed
            create_dir_all(db.config.directory.clone().unwrap().as_path())?;
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
        Ok(db)
    }

    fn swap_new_mem_table(&self, map: &mut RwLockWriteGuard<MemTable>) -> Arc<MemTable> {
        let mut full_maps = self.full_mem_tables.write().expect("RwLock poisoned");
        // If we have too many mem tables we need to wait for at least one to have been written
        while full_maps.len() >= self.config.max_mem_tables {
            drop(full_maps);
            self.finished_flushing.wait_for_any();
            full_maps = self.full_mem_tables.write().expect("RwLock poisoned");
        }
        let new_map = MemTable::new(
            map.config.clone(),
            self.age.fetch_add(1, Ordering::SeqCst) + 1,
        );
        let old_map: MemTable = mem::replace(&mut (*map), new_map);
        let old_map_arc = Arc::new(old_map);
        full_maps.insert(old_map_arc.clone());
        if !self.is_read_only() {
            self.flushing_channel
                .send(old_map_arc.clone())
                .expect("Flushing channel broken");
        }
        old_map_arc
    }

    pub fn insert<K: AsRef<[u8]>, V: AsRef<[u8]>>(&self, key: K, value: V) -> Result<()> {
        let mut map = self.mem_table.write().expect("RwLock poisoned");
        while !map.insert(
            key.as_ref().to_vec().into_boxed_slice(),
            value.as_ref().to_vec().into_boxed_slice(),
        )? {
            self.swap_new_mem_table(&mut map);
        }
        Ok(())
    }

    pub fn remove<K: AsRef<[u8]>>(&self, key: K) -> Result<Option<Box<[u8]>>> {
        let key_ref = key.as_ref();
        let value = self.get(key_ref);
        if value.is_some() {
            self.insert(key_ref, vec![].into_boxed_slice())?;
        }
        Ok(value)
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
                        let res = disk_table.get(key_ref);
                        if res.is_err() {
                            return None;
                        }
                        let ret = res.unwrap();
                        if ret.is_some() {
                            return ret;
                        }
                    }
                }
                None
            })
            .filter(|x| !x.is_empty())
    }

    fn is_read_only(&self) -> bool {
        self.config.directory.is_none() // TODO also have an read only config option so we can support readonly on disk
    }

    fn new_sst(&self) -> PathBuf {
        let mut new_file = self.config.directory.clone().unwrap();
        new_file.push(Uuid::new_v4().to_string() + ".sst");
        new_file
    }

    fn flush_mem_table(&self, mem_table: Arc<MemTable>) {
        // Creating the disk table should not happen while list needed for gets are locked
        let disk_table = DiskTable::create(
            self.new_sst().as_path(),
            mem_table.iter(),
            &self.config.disk_table_config,
            mem_table.age,
            true,
        )
        .expect("Failed to create DiskTable");

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
            } else if map.age == 0 {
                return None;
            } else {
                map.age - 1
            }
        };
        self.finished_flushing.wait_for(age);
        Some(age)
    }

    pub fn compact(&self) {
        if self.is_read_only() {
            return;
        }
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
        let merged = DiskTable::create(
            self.new_sst().as_path(),
            iter,
            &self.config.disk_table_config,
            age,
            false,
        )
        .expect("Failed to create DiskTable");
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
    use std::thread;

    use crate::{Database, DatabaseConfig, DiskTableConfig, MemTableConfig};

    #[test]
    fn basic() {
        let db = Database::open().unwrap();
        let key = vec![8];
        let value = vec![9, 10];
        db.insert(&key, &value).unwrap();
        let retrieved_value = db.get(&key);
        assert_eq!(retrieved_value.as_ref().map(|v| &v[..]), Some(&value[..]));
    }

    #[test]
    fn basic_strings() {
        let db = Database::open().unwrap();
        let key = String::from("key");
        let value = String::from("value");
        db.insert(key.as_bytes(), value.as_bytes()).unwrap();
        assert_eq!(
            db.get(key.as_bytes()).as_ref().map(|v| &(v[..])),
            Some(value.as_bytes())
        );
    }

    #[test]
    fn write_too_large() {
        let tmpdir = tempfile::tempdir().unwrap();
        let mem_table_config = MemTableConfig::default().max_size(20);
        let disk_table_config = DiskTableConfig::default().block_size(15);
        let db = Database::open_with_config(
            DatabaseConfig::default()
                .mem_table_config(mem_table_config)
                .disk_table_config(disk_table_config)
                .directory(tmpdir.into_path()),
        )
        .unwrap();

        // Test a write larger than the mem table max, this should return an error
        let key = String::from("key");
        let value = String::from("value larger than mem table size");
        let result = db.insert(key.as_bytes(), value.as_bytes());
        assert!(result.is_err());

        assert_eq!(
            db.get(key.as_bytes())
                .as_ref()
                .map(|v| String::from_utf8(v.to_vec()).unwrap()),
            None
        );

        // Test a write larger than block size but smaller than mem table max
        let key = String::from("key");
        let value = String::from("value larger ");
        let result = db.insert(key.as_bytes(), value.as_bytes());
        assert!(result.is_ok());

        db.flush();
        {
            let disk_tables = db.disk_tables.read().expect("RWLock poisoned");
            assert_eq!(disk_tables.len(), 1);
        }

        assert_eq!(
            db.get(key.as_bytes())
                .as_ref()
                .map(|v| String::from_utf8(v.to_vec()).unwrap()),
            Some(value.clone())
        );
    }

    #[test]
    fn remove() {
        let db = Database::open().unwrap();
        let key = String::from("key");
        let value = String::from("value");
        db.insert(key.as_bytes(), value.as_bytes()).unwrap();
        assert_eq!(
            db.get(key.as_bytes()).as_ref().map(|v| &(v[..])),
            Some(value.as_bytes())
        );
        let removed_result = db.remove(key.as_bytes());
        assert!(removed_result.is_ok());
        let removed = removed_result.unwrap();
        assert_eq!(removed.as_ref().map(|v| &(v[..])), Some(value.as_bytes()));
        assert_eq!(db.get(key.as_bytes()).as_ref().map(|v| &(v[..])), None);
    }

    #[test]
    fn remove_disk() {
        let tmpdir = tempfile::tempdir().unwrap();
        let db =
            Database::open_with_config(DatabaseConfig::default().directory(tmpdir.into_path()))
                .unwrap();
        let key = String::from("key");
        let value = String::from("value");
        db.insert(key.as_bytes(), value.as_bytes()).unwrap();
        assert_eq!(
            db.get(key.as_bytes()).as_ref().map(|v| &(v[..])),
            Some(value.as_bytes())
        );

        db.flush();

        let removed = db.remove(key.as_bytes()).unwrap();
        assert_eq!(removed.as_ref().map(|v| &(v[..])), Some(value.as_bytes()));
        assert_eq!(db.get(key.as_bytes()).as_ref().map(|v| &(v[..])), None);

        db.flush();

        assert_eq!(db.get(key.as_bytes()).as_ref().map(|v| &(v[..])), None);

        db.compact();

        assert_eq!(db.get(key.as_bytes()).as_ref().map(|v| &(v[..])), None);

        {
            let disk_tables = db.disk_tables.read().expect("RWLock poisoned");
            assert_eq!(disk_tables.len(), 1);
            let disk_table = disk_tables.iter().next().unwrap();
            // test that we didn't store the delete after compaction
            assert_eq!(
                disk_table
                    .get(key.as_bytes())
                    .unwrap()
                    .as_ref()
                    .map(|v| &(v[..])),
                None
            );
        }
    }

    #[test]
    fn overwriting() {
        let tmpdir = tempfile::tempdir().unwrap();
        let db = Database::open_with_config(
            DatabaseConfig::default().directory(tmpdir.path().to_path_buf()),
        )
        .unwrap();
        let key = String::from("key");

        for v in 0..9 {
            let value = v.to_string();
            db.insert(key.as_bytes(), value.as_bytes()).unwrap();
            db.flush();
            assert_eq!(
                db.get(key.as_bytes())
                    .as_ref()
                    .map(|v| String::from_utf8(v.to_vec()).unwrap()),
                Some(value)
            );
        }

        let value = 5.to_string();
        db.insert(key.as_bytes(), value.as_bytes()).unwrap();
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
        drop(db);

        let db = Database::open_with_config(
            DatabaseConfig::default().directory(tmpdir.path().to_path_buf()),
        )
        .unwrap();
        let value = 6.to_string();
        db.insert(key.as_bytes(), value.as_bytes()).unwrap();
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

        let db = Database::open().unwrap();
        for i in 0..NTHREADS {
            let db_ref = db.clone();
            threads.push(thread::spawn(move || {
                let value = String::from("value");
                db_ref.insert(i.to_be_bytes(), value.as_bytes()).unwrap();
            }));
        }

        for thread in threads {
            let _ = thread.join();
        }
    }

    #[test]
    fn overflow_mem_table() {
        let mem_table_config = MemTableConfig::default().max_size(200);
        let db = Database::open_with_config(
            DatabaseConfig::default().mem_table_config(mem_table_config),
        )
        .unwrap();

        let first_key = String::from("first_key");
        let first_value = String::from("first_value");
        db.insert(first_key.as_bytes(), first_value.as_bytes())
            .unwrap();

        for i in 0..100 {
            let key = format!("key{}", i);
            let value = format!("value{}", i);
            db.insert(key.as_bytes(), value.as_bytes()).unwrap();
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
    fn non_existing_dir() {
        let tmpdir = tempfile::tempdir().unwrap();
        let mut dir = tmpdir.path().to_path_buf();
        dir.push("nonexisting");
        let db = Database::open_with_config(DatabaseConfig::default().directory(dir));
        assert!(db.is_ok());
    }

    #[test]
    fn disk() {
        let mem_table_config = MemTableConfig::default().max_size(20);
        let disk_table_config = DiskTableConfig::default().block_size(1024);
        let tmpdir = tempfile::tempdir().unwrap();
        let db = Database::open_with_config(
            DatabaseConfig::default()
                .mem_table_config(mem_table_config)
                .disk_table_config(disk_table_config)
                .directory(tmpdir.path().to_path_buf()),
        )
        .unwrap();

        // Make sure compaction of empty db doesn't do anything
        db.compact();
        {
            let disk_tables = db.disk_tables.read().expect("RWLock poisoned");
            assert_eq!(disk_tables.len(), 0);
        }

        let first_key = String::from("first_key");
        let first_value = String::from("first_value");
        db.insert(first_key.as_bytes(), first_value.as_bytes())
            .unwrap();

        for i in 0..100 {
            let key = format!("key{}", i);
            let value = format!("value{}", i);
            db.insert(key.as_bytes(), value.as_bytes()).unwrap();
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

        drop(db);
        let db = Database::open_with_config(
            DatabaseConfig::default().directory(tmpdir.path().to_path_buf()),
        )
        .unwrap();
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
