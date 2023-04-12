use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::RwLock;
use std::thread::{self};

use crate::crdt::crdt::{CrdtData, CrdtElem, CrdtTrait};

#[derive(Debug, Deserialize, Serialize)]
pub struct PNCounter {
    incr: RwLock<HashMap<String, i64>>,
    decr: RwLock<HashMap<String, i64>>,
}

fn clone(data: &RwLock<HashMap<String, i64>>) -> HashMap<String, i64> {
    data.read().unwrap().clone()
}

fn sum(data: &RwLock<HashMap<String, i64>>) -> i64 {
    data.read().unwrap().values().sum()
}

fn add_x(data: &RwLock<HashMap<String, i64>>, node: String, x: i64) {
    let mut data_guard = data.write().unwrap();
    if let Some(y) = data_guard.get_mut(&node) {
        *y += x;
    } else {
        data_guard.insert(node.clone(), x);
    }
}

fn merge(data: &RwLock<HashMap<String, i64>>, other: HashMap<String, i64>) {
    let mut data_guard = data.write().unwrap();
    other.iter().for_each(|(node, x)| {
        if let Some(y) = data_guard.get_mut(*&node) {
            if (*y).abs() < (*x).abs() {
                *y = *x;
            }
        } else {
            data_guard.insert(node.clone(), *x);
        }
    });
}

impl PNCounter {
    fn add_x(&self, node: String, x: i64) {
        if x >= 0 {
            add_x(&self.incr, node, x);
        } else {
            add_x(&self.decr, node, x);
        }
    }

    fn merge_data(&self, other: (HashMap<String, i64>, HashMap<String, i64>)) {
        let (other_incr, other_decr) = other;

        thread::scope(|_s| {
            merge(&self.incr, other_incr);
        });
        thread::scope(|_s| {
            merge(&self.decr, other_decr);
        });
    }
}

impl CrdtTrait for PNCounter {
    fn new(neighbors: &HashSet<String>) -> Self {
        let map: HashMap<String, i64> = neighbors
            .into_iter()
            .map(|node| (node.clone(), 0))
            .collect();
        eprintln!("PNCounter!");
        PNCounter {
            incr: RwLock::new(map.clone()),
            decr: RwLock::new(map),
        }
    }

    fn add(&self, element: CrdtElem) {
        match element {
            CrdtElem::PNCounterDelta(node, x) => {
                self.add_x(node, x);
            }
            _ => {
                panic!("Wrong element type");
            }
        }
    }

    fn data(&self) -> CrdtData {
        CrdtData::PNCounterData((clone(&self.incr), clone(&self.decr)))
    }

    fn read_json(&self) -> serde_json::Value {
        let mut sum_int = thread::scope(|_s| sum(&self.incr));
        sum_int += thread::scope(|_s| sum(&self.decr));

        return serde_json::to_value(&sum_int).unwrap();
    }

    fn merge(&self, other: CrdtData) {
        match other {
            CrdtData::PNCounterData(other) => {
                self.merge_data(other);
            }
            _ => {
                panic!("Wrong data type");
            }
        }
    }
}
