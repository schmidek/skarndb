use crate::error::Error::TooLarge;
use std::cmp::Ordering;
use std::collections::BTreeMap;

use crate::error::Result;
use crate::MemTableConfig;

pub struct MemTable {
    pub config: MemTableConfig,
    size: usize,
    entries: BTreeMap<Box<[u8]>, Box<[u8]>>,
    pub age: u64,
}

impl Ord for MemTable {
    fn cmp(&self, other: &Self) -> Ordering {
        self.age.cmp(&other.age).reverse()
    }
}

impl PartialOrd for MemTable {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for MemTable {
    fn eq(&self, other: &Self) -> bool {
        self.age == other.age
    }
}

impl Eq for MemTable {}

impl MemTable {
    pub fn new(config: MemTableConfig, age: u64) -> MemTable {
        MemTable {
            config,
            size: 0,
            entries: BTreeMap::new(),
            age,
        }
    }

    pub fn insert<K: AsRef<[u8]>, V: AsRef<[u8]>>(&mut self, key: K, value: V) -> Result<bool> {
        let entry_size: usize = key.as_ref().len() + value.as_ref().len();
        if entry_size > self.config.max_size {
            return Err(TooLarge(String::from(
                "KeyValue can't be larger than max mem table size",
            )));
        }
        let new_size = self.size + entry_size;
        if new_size > self.config.max_size {
            return Ok(false);
        }
        self.entries.insert(
            key.as_ref().to_vec().into_boxed_slice(),
            value.as_ref().to_vec().into_boxed_slice(),
        );
        self.size = new_size;
        Ok(true)
    }

    pub fn get<K: AsRef<[u8]>>(&self, key: K) -> Option<Box<[u8]>> {
        self.entries.get(key.as_ref()).cloned()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&Box<[u8]>, &Box<[u8]>)> {
        self.entries.iter()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }
}
