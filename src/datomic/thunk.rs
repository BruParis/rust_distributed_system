use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::{HashMap, HashSet};
use std::io::{self, Write};

use crate::datomic::msg::{
    LinKvError, LinKvPayload, LinKvReadPayload, LinKvReplyValue, LinKvWritePayload,
};
use crate::datomic::node::{log, Node};

#[derive(Debug, Clone)]
pub enum ThunkWriteEnum<'a> {
    Thunk(&'a ThunkValues),
    Map(&'a ThunkMap),
    Root(String),
}

impl<'a> Serialize for ThunkWriteEnum<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            ThunkWriteEnum::Thunk(thunk) => thunk.serialize(serializer),
            ThunkWriteEnum::Map(map) => {
                let map_str = map
                    .iter()
                    .map(|(k, v)| (k.to_string(), v))
                    .collect::<HashMap<String, &Thunk<ThunkValues>>>();
                map_str.serialize(serializer)
            }
            ThunkWriteEnum::Root(root) => root.serialize(serializer),
        }
    }
}

pub trait ThunkTrait {
    fn is_empty(&self) -> bool;
    fn new() -> Self;
    fn unwrap_reply(reply: LinKvReplyValue) -> Option<Box<Self>>;
    fn to_write_value(&self) -> ThunkWriteEnum;
    fn save(&mut self, id: String, node: &Node) -> Result<Option<LinKvReplyValue>, LinKvError>;
}

#[derive(Debug, Clone)]
pub struct Thunk<V: Clone + ThunkTrait> {
    pub id: String,
    value: V,
    pub saved: bool,
}

pub type ThunkValues = Vec<usize>;

impl ThunkTrait for ThunkValues {
    fn is_empty(&self) -> bool {
        self.is_empty()
    }

    fn new() -> Self {
        vec![]
    }

    fn unwrap_reply(reply: LinKvReplyValue) -> Option<Box<Self>> {
        match reply {
            LinKvReplyValue::ReadThunkOk(r_p) => Some(Box::new(r_p.value)),
            _ => None,
        }
    }

    fn to_write_value(&self) -> ThunkWriteEnum {
        ThunkWriteEnum::Thunk(self)
    }

    fn save(&mut self, id: String, node: &Node) -> Result<Option<LinKvReplyValue>, LinKvError> {
        let write_value = self.to_write_value();
        let write_body = node.build_body(
            LinKvPayload::Write(LinKvWritePayload::new(id, &write_value)),
            None,
        );
        let promise = node.new_promise(write_body.msg_id.unwrap());
        promise.sync_rpc(node.node_id.clone(), write_body)
    }
}

// serde JSON only supports keys as strings
pub type ThunkMap = HashMap<usize, Thunk<ThunkValues>>;

impl ThunkTrait for ThunkMap {
    fn is_empty(&self) -> bool {
        self.is_empty()
    }

    fn new() -> Self {
        HashMap::new()
    }

    fn unwrap_reply(reply: LinKvReplyValue) -> Option<Box<Self>> {
        match reply {
            LinKvReplyValue::ReadMapOk(r_p) => {
                let value = r_p
                    .value
                    .into_iter()
                    .map(|(k, id)| (k.parse::<usize>().unwrap(), Thunk::from_id(id)))
                    .collect();
                Some(Box::new(value))
            }
            _ => None,
        }
    }

    fn to_write_value(&self) -> ThunkWriteEnum {
        ThunkWriteEnum::Map(self)
    }

    fn save(&mut self, id: String, node: &Node) -> Result<Option<LinKvReplyValue>, LinKvError> {
        self.iter_mut().try_for_each(|(_, v)| {
            v.save(&node)?;
            Ok(())
        })?;

        let write_value = self.to_write_value();

        let write_body = node.build_body(
            LinKvPayload::Write(LinKvWritePayload::new(id, &write_value)),
            None,
        );
        let promise = node.new_promise(write_body.msg_id.unwrap());
        promise.sync_rpc(node.node_id.clone(), write_body)
    }
}

impl<V: Clone + ThunkTrait> Thunk<V> {
    pub fn new(node_id: String, u_id: usize, value: V) -> Self {
        let id = format!("{}-{}", node_id, u_id);
        Thunk {
            id,
            value,
            saved: false,
        }
    }

    pub fn from_id(id: String) -> Self {
        Thunk {
            id,
            value: V::new(),
            saved: true,
        }
    }

    pub fn get_value(&mut self, node: &Node) -> Result<V, LinKvError> {
        if self.value.is_empty() {
            let read_body = node.build_body(
                LinKvPayload::Read(LinKvReadPayload::new(self.id.to_owned())),
                None,
            );
            let promise_msg_id = read_body.msg_id.unwrap();
            let promise = node.new_promise(promise_msg_id);

            let promise_res = promise.sync_rpc(node.node_id.clone(), read_body)?;
            if let Some(reply) = V::unwrap_reply(promise_res.unwrap()) {
                self.value = *reply;
            } else {
                log(&format!("Unable to read value for thunk {}", self.id));
            }
        }

        return Ok(self.value.clone());
    }

    pub fn save(&mut self, node: &Node) -> Result<(), LinKvError> {
        if !self.saved {
            self.get_value(node)?;

            let save_res = self.value.save(self.id.to_owned(), node)?;
            if let Some(LinKvReplyValue::WriteOk()) = save_res {
                self.saved = true;
            } else {
                log(&format!("Unable to save thunk {}", self.id));
            }
        }
        Ok(())
    }
}

impl<V: Clone + ThunkTrait> Serialize for Thunk<V> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.id.serialize(serializer)
    }
}

impl<'de, V: Clone + ThunkTrait> Deserialize<'de> for Thunk<V> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let id = String::deserialize(deserializer)?;
        Ok(Thunk {
            id,
            value: V::new(),
            saved: false,
        })
    }
}
