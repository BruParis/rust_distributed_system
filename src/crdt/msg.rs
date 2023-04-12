use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::io::{self, Write};

use super::crdt::CrdtData;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct Message<P: PayloadTrait> {
    pub src: String,
    pub dest: String,
    pub body: Body<P>,
}

impl<P> Message<P>
where
    P: PayloadTrait + Serialize,
{
    pub fn new(dest: String, body: Body<P>, src: String) -> Self {
        Message { src, dest, body }
    }

    pub fn send(&self) {
        let mut stdout = io::stdout().lock();
        serde_json::to_writer(&mut stdout, &self).unwrap();
        stdout.write_all(b"\n").unwrap();
        stdout.flush().unwrap();
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct InitPayload {
    pub node_id: String,
    pub node_ids: HashSet<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AddPayload {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub element: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delta: Option<i64>,
}

impl AddPayload {
    pub fn new(element: Option<usize>, delta: Option<i64>) -> Self {
        AddPayload { element, delta }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct InitOkPayload {
    in_reply_to: usize,
}

impl InitOkPayload {
    pub fn new(in_reply_to: usize) -> Self {
        InitOkPayload { in_reply_to }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ReadOkPayload {
    in_reply_to: usize,
    value: serde_json::Value,
}

impl ReadOkPayload {
    pub fn new(in_reply_to: usize, value: serde_json::Value) -> Self {
        ReadOkPayload { in_reply_to, value }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AddOkPayload {
    pub in_reply_to: usize,
}

impl AddOkPayload {
    pub fn new(in_reply_to: usize) -> Self {
        AddOkPayload { in_reply_to }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ReplicatePayload {
    pub data: CrdtData,
}

impl ReplicatePayload {
    pub fn new(data: CrdtData) -> Self {
        ReplicatePayload { data }
    }
}

pub trait PayloadTrait {}

#[derive(Debug, Clone, Deserialize, Serialize)]
//#[serde(untagged)]
#[serde(tag = "type")]
pub enum ReqPayload {
    #[serde(rename = "init")]
    Init(InitPayload),
    #[serde(rename = "read")]
    Read,
    #[serde(rename = "add")]
    Add(AddPayload),
    #[serde(rename = "replicate")]
    Replicate(ReplicatePayload),
}

impl PayloadTrait for ReqPayload {}

pub trait SendTrait {}

#[derive(Debug, Clone, Deserialize, Serialize)]
//#[serde(untagged)]
#[serde(tag = "type")]
pub enum SendPayload {
    #[serde(rename = "init_ok")]
    InitOk(InitOkPayload),
    #[serde(rename = "read_ok")]
    ReadOk(ReadOkPayload),
    #[serde(rename = "replicate")]
    Replicate(ReplicatePayload),
    #[serde(rename = "add_ok")]
    AddOk(AddOkPayload),
}

impl SendTrait for SendPayload {}
impl PayloadTrait for SendPayload {}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Body<P: PayloadTrait> {
    #[serde(flatten)]
    pub payload: P,
    pub msg_id: usize,
}

impl<P> Body<P>
where
    P: PayloadTrait,
{
    pub fn new(payload: P, msg_id: usize) -> Self {
        Body { payload, msg_id }
    }
}
