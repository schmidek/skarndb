use std::collections::BTreeMap;
use std::fs::File;
use std::sync::RwLock;

struct Database {
    mem_table: RwLock<BTreeMap<Box<[u8]>, Box<[u8]>>>,
}

impl Database {
    fn open() -> Database {
        Database {
            mem_table: RwLock::new(BTreeMap::new()),
        }
    }

    fn insert(&self, key: &[u8], value: &[u8]) {
        let mut map = self.mem_table.write().expect("RwLock poisoned");
        map.insert(
            key.to_vec().into_boxed_slice(),
            value.to_vec().into_boxed_slice(),
        );
    }

    fn get(&self, key: &[u8]) -> Option<Box<[u8]>> {
        let map = self.mem_table.read().expect("RwLock poisoned");
        map.get(key).map(|v| v.clone())
    }
}

#[cfg(test)]
mod tests {
    use crate::Database;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn basic() {
        let mut db = Database::open();
        let key = vec![8];
        let value = vec![9, 10];
        db.insert(&key, &value);
        let retrieved_value = db.get(&key);
        assert_eq!(retrieved_value.as_ref().map(|v| &v[..]), Some(&value[..]));
    }

    #[test]
    fn basic_strings() {
        let mut db = Database::open();
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
                db_ref.insert(&i.to_be_bytes(), value.as_bytes());
            }));
        }

        for thread in threads {
            let _ = thread.join();
        }
    }
}
