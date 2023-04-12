use crate::crdt::crdt::{CrdtData, CrdtElem, CrdtTrait};

use std::collections::HashSet;
use std::sync::RwLock;


#[derive(Debug)]
pub struct GSet {
    data: RwLock<HashSet<usize>>,
}

impl GSet {
    fn add_element(&self, element: usize) {
        self.data.write().unwrap().insert(element);
    }

    fn merge_data(&self, other_data_guard: HashSet<usize>) {
        self.data.write().unwrap().extend(other_data_guard.iter());
    }
}

impl CrdtTrait for GSet {
    fn new(_neighbors: &HashSet<String>) -> Self {
        eprintln!("GSet!");
        GSet {
            data: RwLock::new(HashSet::new()),
        }
    }

    fn data(&self) -> CrdtData {
        CrdtData::GSetData(self.data.read().unwrap().clone())
    }

    fn add(&self, element: CrdtElem) {
        match element {
            CrdtElem::GSetElem(element) => {
                self.add_element(element);
            }
            _ => {
                panic!("Wrong element type");
            }
        }
    }

    fn read_json(&self) -> serde_json::Value {
        serde_json::to_value(&*self.data.read().unwrap()).unwrap()
    }

    fn merge(&self, other: CrdtData) {
        match other {
            CrdtData::GSetData(other) => {
                self.merge_data(other);
            }
            _ => {
                panic!("Wrong data type");
            }
        }
    }
}
