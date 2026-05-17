use serde::de::{self, Deserializer, SeqAccess, Visitor};
use serde::{self, Deserialize, Serialize, Serializer};
use serde_json::json;
use std::fmt;

#[derive(Debug, Clone)]
pub struct TxnReadOp {
    key: usize,
    value: Vec<usize>,
}

impl TxnReadOp {
    pub fn new(key: usize, value: Vec<usize>) -> Self {
        TxnReadOp { key, value: value }
    }
}

#[derive(Debug, Clone)]
pub struct TxnAppendOp {
    key: usize,
    pub value: usize,
}

impl TxnAppendOp {
    pub fn new(key: usize, value: usize) -> Self {
        TxnAppendOp { key, value }
    }
}

#[derive(Debug, Clone)]
pub enum TxnOp {
    Read(TxnReadOp),
    Append(TxnAppendOp),
}

impl TxnOp {
    pub fn get_key(&self) -> usize {
        match self {
            TxnOp::Read(op) => op.key,
            TxnOp::Append(op) => op.key,
        }
    }
}

impl Serialize for TxnOp {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            TxnOp::Read(read_op) => {
                let value = json!(["r", read_op.key, read_op.value]);
                value.serialize(serializer)
            }
            TxnOp::Append(append_op) => {
                let value = json!(["append", append_op.key, append_op.value]);
                value.serialize(serializer)
            }
        }
    }
}

impl<'de> Deserialize<'de> for TxnOp {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct TxnOpVisitor;

        impl<'de> Visitor<'de> for TxnOpVisitor {
            type Value = TxnOp;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a sequence with a variant identifier and its fields")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let variant: String = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(0, &self))?;
                let key: usize = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(1, &self))?;
                match variant.as_str() {
                    "r" => {
                        let value_opt: Option<Vec<usize>> = seq
                            .next_element()?
                            .ok_or_else(|| de::Error::invalid_length(2, &self))?;
                        let value = value_opt.unwrap_or_else(|| vec![]);
                        Ok(TxnOp::Read(TxnReadOp { key, value }))
                    }
                    "append" => {
                        let value: usize = seq
                            .next_element()?
                            .ok_or_else(|| de::Error::invalid_length(2, &self))?;
                        Ok(TxnOp::Append(TxnAppendOp::new(key, value)))
                    }
                    _ => Err(de::Error::unknown_variant(&variant, &["r"])),
                }
            }
        }

        deserializer.deserialize_seq(TxnOpVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_op_serializes_to_array() {
        let op = TxnOp::Read(TxnReadOp::new(1, vec![10, 20]));
        let json = serde_json::to_value(&op).unwrap();
        assert_eq!(json, serde_json::json!(["r", 1, [10, 20]]));
    }

    #[test]
    fn test_append_op_serializes_to_array() {
        let op = TxnOp::Append(TxnAppendOp::new(2, 42));
        let json = serde_json::to_value(&op).unwrap();
        assert_eq!(json, serde_json::json!(["append", 2, 42]));
    }

    #[test]
    fn test_read_op_deserializes_with_value() {
        let json = r#"["r", 3, [1, 2, 3]]"#;
        let op: TxnOp = serde_json::from_str(json).unwrap();
        if let TxnOp::Read(r) = op {
            assert_eq!(r.key, 3);
            assert_eq!(r.value, vec![1, 2, 3]);
        } else {
            panic!("Expected Read op");
        }
    }

    #[test]
    fn test_read_op_deserializes_with_null() {
        // Maelstrom sends null for keys with no stored value
        let json = r#"["r", 5, null]"#;
        let op: TxnOp = serde_json::from_str(json).unwrap();
        if let TxnOp::Read(r) = op {
            assert_eq!(r.value, Vec::<usize>::new());
        } else {
            panic!("Expected Read op");
        }
    }

    #[test]
    fn test_append_op_roundtrip() {
        let original = TxnOp::Append(TxnAppendOp::new(7, 99));
        let serialized = serde_json::to_string(&original).unwrap();
        let deserialized: TxnOp = serde_json::from_str(&serialized).unwrap();
        if let TxnOp::Append(a) = deserialized {
            assert_eq!(a.key, 7);
            assert_eq!(a.value, 99);
        } else {
            panic!("Expected Append op");
        }
    }

    #[test]
    fn test_read_op_roundtrip() {
        let original = TxnOp::Read(TxnReadOp::new(4, vec![5, 6, 7]));
        let serialized = serde_json::to_string(&original).unwrap();
        let deserialized: TxnOp = serde_json::from_str(&serialized).unwrap();
        if let TxnOp::Read(r) = deserialized {
            assert_eq!(r.key, 4);
            assert_eq!(r.value, vec![5, 6, 7]);
        } else {
            panic!("Expected Read op");
        }
    }

    #[test]
    fn test_get_key() {
        assert_eq!(TxnOp::Read(TxnReadOp::new(3, vec![])).get_key(), 3);
        assert_eq!(TxnOp::Append(TxnAppendOp::new(9, 1)).get_key(), 9);
    }
}
