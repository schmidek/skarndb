use crossbeam_utils::sync::{Parker, Unparker};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::RwLock;

pub struct ObservableSet {
    // the first value means the set contains all numbers up to and including that number
    values: RwLock<BTreeSet<u64>>,
    observers: RwLock<BTreeMap<u64, Vec<Unparker>>>,
}

impl ObservableSet {
    pub fn new() -> ObservableSet {
        ObservableSet {
            values: RwLock::new(BTreeSet::new()),
            observers: RwLock::new(BTreeMap::new()),
        }
    }

    /// Wait for set to contain all numbers up to and including value
    pub fn wait_for(&self, value: u64) {
        let p = Parker::new();
        let u = p.unparker().clone();
        {
            let values = self.values.read().expect("RwLock poisoned");
            if !values.is_empty() && values.iter().next().unwrap() >= &value {
                return;
            }
            let mut observers = self.observers.write().expect("RwLock poisoned");
            if !observers.contains_key(&value) {
                observers.insert(value, Vec::new());
            }
            observers.get_mut(&value).unwrap().push(u);
        }
        p.park();
    }

    pub fn add(&self, value: u64) {
        let mut values = self.values.write().expect("RwLock poisoned");
        let next_number = values.iter().next().map(|x| x + 1).unwrap_or(0);
        for i in next_number..value {
            if !values.contains(&i) {
                values.insert(value);
                return;
            }
        }
        // notify any observers
        let mut observers = self.observers.write().expect("RwLock poisoned");
        for i in next_number..=value {
            if let Some(i_observers) = observers.get(&i) {
                for i_observer in i_observers {
                    i_observer.unpark();
                }
                observers.remove(&i);
            }
        }
        values.retain(|x| x > &value);
        values.insert(value);
    }
}
