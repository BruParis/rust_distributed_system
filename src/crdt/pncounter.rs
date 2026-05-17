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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crdt::crdt::{CrdtData, CrdtElem, CrdtTrait};

    fn nodes(ns: &[&str]) -> HashSet<String> {
        ns.iter().map(|s| s.to_string()).collect()
    }

    fn extract(data: CrdtData) -> (HashMap<String, i64>, HashMap<String, i64>) {
        match data {
            CrdtData::PNCounterData(d) => d,
            _ => panic!("Expected PNCounterData"),
        }
    }

    #[test]
    fn test_increment_positive() {
        let c = PNCounter::new(&nodes(&["n1"]));
        c.add(CrdtElem::PNCounterDelta("n1".to_string(), 5));
        assert_eq!(c.read_json(), serde_json::json!(5));
    }

    #[test]
    fn test_decrement_negative() {
        let c = PNCounter::new(&nodes(&["n1"]));
        c.add(CrdtElem::PNCounterDelta("n1".to_string(), 10));
        c.add(CrdtElem::PNCounterDelta("n1".to_string(), -3));
        assert_eq!(c.read_json(), serde_json::json!(7));
    }

    #[test]
    fn test_incr_and_decr_tracked_separately() {
        let c = PNCounter::new(&nodes(&["n1"]));
        c.add(CrdtElem::PNCounterDelta("n1".to_string(), 10));
        c.add(CrdtElem::PNCounterDelta("n1".to_string(), -4));
        let (incr, decr) = extract(c.data());
        assert_eq!(*incr.get("n1").unwrap(), 10);
        assert_eq!(*decr.get("n1").unwrap(), -4);
    }

    #[test]
    fn test_merge_accumulates_across_nodes() {
        let n1 = PNCounter::new(&nodes(&["n1", "n2"]));
        n1.add(CrdtElem::PNCounterDelta("n1".to_string(), 5));

        let n2 = PNCounter::new(&nodes(&["n1", "n2"]));
        n2.add(CrdtElem::PNCounterDelta("n2".to_string(), 3));

        n1.merge(n2.data());
        assert_eq!(n1.read_json(), serde_json::json!(8));
    }

    #[test]
    fn test_merge_idempotency() {
        let c = PNCounter::new(&nodes(&["n1"]));
        c.add(CrdtElem::PNCounterDelta("n1".to_string(), 7));
        c.add(CrdtElem::PNCounterDelta("n1".to_string(), -2));
        let snapshot = c.data();
        c.merge(snapshot);
        assert_eq!(c.read_json(), serde_json::json!(5));
    }

    #[test]
    fn test_merge_commutativity() {
        let a = PNCounter::new(&nodes(&["n1", "n2"]));
        a.add(CrdtElem::PNCounterDelta("n1".to_string(), 5));
        a.add(CrdtElem::PNCounterDelta("n1".to_string(), -1));

        let b = PNCounter::new(&nodes(&["n1", "n2"]));
        b.add(CrdtElem::PNCounterDelta("n2".to_string(), 3));
        b.add(CrdtElem::PNCounterDelta("n2".to_string(), -2));

        // a merge b
        let ab = PNCounter::new(&nodes(&["n1", "n2"]));
        ab.add(CrdtElem::PNCounterDelta("n1".to_string(), 5));
        ab.add(CrdtElem::PNCounterDelta("n1".to_string(), -1));
        ab.merge(b.data());

        // b merge a
        let ba = PNCounter::new(&nodes(&["n1", "n2"]));
        ba.add(CrdtElem::PNCounterDelta("n2".to_string(), 3));
        ba.add(CrdtElem::PNCounterDelta("n2".to_string(), -2));
        ba.merge(a.data());

        assert_eq!(ab.read_json(), ba.read_json());
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
