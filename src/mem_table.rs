use crate::MemTableConfig;
use std::collections::btree_map::Iter;
use std::collections::BTreeMap;

pub struct MemTable {
    pub config: MemTableConfig,
    size: usize,
    entries: BTreeMap<Box<[u8]>, Box<[u8]>>,
}

impl MemTable {
    pub fn new(config: MemTableConfig) -> MemTable {
        MemTable {
            config,
            size: 0,
            entries: BTreeMap::new(),
        }
    }

    pub fn insert<K: AsRef<[u8]>, V: AsRef<[u8]>>(&mut self, key: K, value: V) -> bool {
        let entry_size: usize = key.as_ref().len() + value.as_ref().len();
        if entry_size > self.config.max_size {
            panic!("KeyValue can't be larger than max mem table size!");
        }
        let new_size = self.size + entry_size;
        if new_size > self.config.max_size {
            return false;
        }
        self.entries.insert(
            key.as_ref().to_vec().into_boxed_slice(),
            value.as_ref().to_vec().into_boxed_slice(),
        );
        self.size = new_size;
        true
    }

    pub fn get<K: AsRef<[u8]>>(&self, key: K) -> Option<Box<[u8]>> {
        self.entries.get(key.as_ref()).map(|v| v.clone())
    }

    pub fn iter(&self) -> impl Iterator<Item = (&Box<[u8]>, &Box<[u8]>)> {
        self.entries.iter()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }
}
