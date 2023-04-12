use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

pub enum CrdtElem {
    GSetElem(usize),
    PNCounterDelta(String, i64),
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub enum CrdtData {
    GSetData(HashSet<usize>),
    PNCounterData((HashMap<String, i64>, HashMap<String, i64>)),
}

/*impl CrdtData {
    pub fn from_json(value: serde_json::Value) -> Self {
        eprintln!("value: {:?}", value);
        match value {
            serde_json::Value::Array(arr_val) => match arr_val {
                serde_json::Value::Array(arr_val) => {
                    CrdtData::GSetData(serde_json::from_value(value).unwrap())
                }
                serde_json::Value::Object(_) => {
                    CrdtData::PNCounterData(serde_json::from_value(value).unwrap())
                }
                _ => {
                    panic!("Wrong element type");
                }
            },
            _ => {
                panic!("Wrong element type");
            }
        }
    }
}*/

pub trait CrdtTrait {
    fn new(neighbors: &HashSet<String>) -> Self;
    //fn from_json(value: serde_json::Value) -> Self;

    fn add(&self, element: CrdtElem);
    //fn to_json(&self) -> serde_json::Value;
    fn data(&self) -> CrdtData;
    fn read_json(&self) -> serde_json::Value;
    fn merge(&self, other: CrdtData);
}
