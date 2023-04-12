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
