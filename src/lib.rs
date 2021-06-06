mod mem_table;

use std::fs::File;
use std::sync::RwLock;
use mem_table::MemTable;
use core::mem;

#[derive(Clone)]
pub struct MemTableConfig {
    pub max_size: usize, // max size of mem table in bytes
}

impl MemTableConfig {
    pub fn default() -> MemTableConfig {
        MemTableConfig {
            max_size: 1_000_000
        }
    }

    pub fn max_size(mut self, size: usize) -> Self {
        self.max_size = size;
        self
    }
}

struct Database {
    mem_table: RwLock<MemTable>,
    full_mem_tables: RwLock<Vec<MemTable>>,
}

impl Database {
    fn open() -> Database {
        Database::open_with_config(MemTableConfig::default())
    }

    fn open_with_config(mem_table_config: MemTableConfig) -> Database {
        Database {
            mem_table: RwLock::new(MemTable::new(mem_table_config)),
            full_mem_tables: RwLock::new(Vec::new()),
        }
    }

    fn insert<K: AsRef<[u8]>, V: AsRef<[u8]>>(&self, key: K, value: V) {
        let mut map = self.mem_table.write().expect("RwLock poisoned");
        while !map.insert(
            key.as_ref().to_vec().into_boxed_slice(),
            value.as_ref().to_vec().into_boxed_slice(),
        ){
            let mut full_maps = self.full_mem_tables.write().expect("RwLock poisoned");
            let new_map = MemTable::new(map.config.clone());
            full_maps.push(mem::replace(&mut map, new_map));
        }
    }

    fn get<K: AsRef<[u8]>>(&self, key: K) -> Option<Box<[u8]>> {
        let map = self.mem_table.read().expect("RwLock poisoned");
        map.get(key.as_ref())
            .or_else(|| {
                let full_maps = self.full_mem_tables.read().expect("RwLock poisoned");
                for full_map in full_maps.iter() {
                    let ret = full_map.get(key.as_ref());
                    if ret.is_some() {
                        return ret;
                    }
                }
                None
            })
            .map(|v| v.clone())
    }
}

#[cfg(test)]
mod tests {
    use crate::{Database, mem_table, MemTableConfig};
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
    fn multi_threaded() {
        const NTHREADS: u32 = 10;
        let mut threads = vec![];

        let db = Arc::new(Database::open());
        for i in 0..NTHREADS {
            let db_ref = Arc::clone(&db);
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
    fn overflow_mem_table(){
        let db = Database::open_with_config(MemTableConfig::default().max_size(20));

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
}
