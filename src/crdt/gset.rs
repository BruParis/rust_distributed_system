use crate::crdt::crdt::{CrdtData, CrdtElem, CrdtTrait};

use std::collections::HashSet;
use std::sync::RwLock;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crdt::crdt::{CrdtData, CrdtElem, CrdtTrait};

    fn no_neighbors() -> HashSet<String> {
        HashSet::new()
    }

    fn extract(data: CrdtData) -> HashSet<usize> {
        match data {
            CrdtData::GSetData(s) => s,
            _ => panic!("Expected GSetData"),
        }
    }

    #[test]
    fn test_add_elements() {
        let g = GSet::new(&no_neighbors());
        g.add(CrdtElem::GSetElem(1));
        g.add(CrdtElem::GSetElem(2));
        g.add(CrdtElem::GSetElem(1)); // duplicate ignored
        let data = extract(g.data());
        assert_eq!(data.len(), 2);
        assert!(data.contains(&1));
        assert!(data.contains(&2));
    }

    #[test]
    fn test_merge_union() {
        let a = GSet::new(&no_neighbors());
        a.add(CrdtElem::GSetElem(1));

        let b = GSet::new(&no_neighbors());
        b.add(CrdtElem::GSetElem(2));

        a.merge(b.data());
        let result = extract(a.data());
        assert!(result.contains(&1));
        assert!(result.contains(&2));
    }

    #[test]
    fn test_merge_idempotency() {
        let g = GSet::new(&no_neighbors());
        g.add(CrdtElem::GSetElem(10));
        g.add(CrdtElem::GSetElem(20));
        let before = extract(g.data());
        g.merge(g.data());
        assert_eq!(extract(g.data()), before);
    }

    #[test]
    fn test_merge_commutativity() {
        let a = GSet::new(&no_neighbors());
        a.add(CrdtElem::GSetElem(1));
        a.add(CrdtElem::GSetElem(2));

        let b = GSet::new(&no_neighbors());
        b.add(CrdtElem::GSetElem(2));
        b.add(CrdtElem::GSetElem(3));

        // a merge b
        let ab = GSet::new(&no_neighbors());
        ab.add(CrdtElem::GSetElem(1));
        ab.add(CrdtElem::GSetElem(2));
        ab.merge(b.data());

        // b merge a
        let ba = GSet::new(&no_neighbors());
        ba.add(CrdtElem::GSetElem(2));
        ba.add(CrdtElem::GSetElem(3));
        ba.merge(a.data());

        assert_eq!(extract(ab.data()), extract(ba.data()));
    }

    #[test]
    fn test_merge_associativity() {
        let a = GSet::new(&no_neighbors());
        a.add(CrdtElem::GSetElem(1));

        let b = GSet::new(&no_neighbors());
        b.add(CrdtElem::GSetElem(2));

        let c = GSet::new(&no_neighbors());
        c.add(CrdtElem::GSetElem(3));

        // (a merge b) merge c
        let ab = GSet::new(&no_neighbors());
        ab.add(CrdtElem::GSetElem(1));
        ab.merge(b.data());
        ab.merge(c.data());

        // a merge (b merge c)
        let bc = GSet::new(&no_neighbors());
        bc.add(CrdtElem::GSetElem(2));
        bc.merge(c.data());

        let a2 = GSet::new(&no_neighbors());
        a2.add(CrdtElem::GSetElem(1));
        a2.merge(bc.data());

        assert_eq!(extract(ab.data()), extract(a2.data()));
    }
}


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
