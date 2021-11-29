use std::collections::{BTreeMap, BTreeSet};
use std::sync::RwLock;

use crossbeam_utils::sync::{Parker, Unparker};

const ANY: u64 = u64::MAX;

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

    pub fn new_with_value(value: u64) -> ObservableSet {
        let mut values = BTreeSet::new();
        values.insert(value);
        ObservableSet {
            values: RwLock::new(values),
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
            // We need to hold onto the values read lock until we add our observer to make sure it doesn't get written to in the meantime
            let mut observers = self.observers.write().expect("RwLock poisoned");
            observers.entry(value).or_insert_with(Vec::new).push(u);
        }
        p.park();
    }

    pub fn wait_for_any(&self) {
        self.wait_for(ANY);
    }

    pub fn add(&self, value: u64) {
        let mut values = self.values.write().expect("RwLock poisoned");
        let next_number = values.iter().next().map(|x| x + 1).unwrap_or(0);
        values.insert(value);
        for i in next_number..value {
            if !values.contains(&i) {
                return;
            }
        }
        // notify any observers
        let mut observers = self.observers.write().expect("RwLock poisoned");
        // We have at least up to value but could be more
        let mut i = next_number;
        while values.contains(&i) {
            if let Some(i_observers) = observers.remove(&i) {
                for i_observer in i_observers {
                    i_observer.unpark();
                }
            }
            i += 1;
        }
        values.retain(|x| x >= &(i - 1));

        if let Some(any_observers) = observers.remove(&ANY) {
            for observer in any_observers {
                observer.unpark();
            }
        }
    }
}
