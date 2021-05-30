use std::collections::BTreeMap;
use std::fs::File;

struct Database {
    mem_table: BTreeMap<Vec<u8>,Vec<u8>>,
}

impl Database {
    fn open() -> Database {
        Database { mem_table: BTreeMap::new() }
    }

    fn insert(&mut self, key: &[u8], value: &[u8]) {
        self.mem_table.insert(key.to_vec(), value.to_vec());
    }

    fn get(&self, key: &[u8]) -> Option<&[u8]> {
        self.mem_table.get(key).map(|v| v.as_ref())
    }
}

#[cfg(test)]
mod tests {
    use crate::Database;

    #[test]
    fn basic() {
        let mut db = Database::open();
        let key = vec![8];
        let value = vec![9,10];
        db.insert(&key, &value);
        assert_eq!(db.get(&key), Some(value.as_ref()));
    }

    #[test]
    fn basic_strings() {
        let mut db = Database::open();
        let key = String::from("key");
        let value = String::from("value");
        db.insert(key.as_bytes(), value.as_bytes());
        assert_eq!(db.get(key.as_bytes()), Some(value.as_bytes()));
    }
}
