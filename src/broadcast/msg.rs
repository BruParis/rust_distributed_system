use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::io::{self, Write};

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
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TopoPayload {
    pub topology: HashMap<String, HashSet<String>>,
}

impl TopoPayload {
    pub fn new(topology: HashMap<String, HashSet<String>>) -> Self {
        TopoPayload { topology }
    }
}

pub trait RpcTrait {}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct GossipPayload {
    pub message: usize,
}

impl GossipPayload {
    pub fn new(message: usize) -> Self {
        GossipPayload { message }
    }
}

impl RpcTrait for GossipPayload {}

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
pub struct TopologyOkPayload {
    in_reply_to: usize,
}

impl TopologyOkPayload {
    pub fn new(in_reply_to: usize) -> Self {
        TopologyOkPayload { in_reply_to }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ReadOkPayload {
    in_reply_to: usize,
    messages: HashSet<usize>,
}

impl ReadOkPayload {
    pub fn new(in_reply_to: usize, messages: HashSet<usize>) -> Self {
        ReadOkPayload {
            in_reply_to,
            messages,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct BroadcastOkPayload {
    pub in_reply_to: usize,
}

impl BroadcastOkPayload {
    pub fn new(in_reply_to: usize) -> Self {
        BroadcastOkPayload { in_reply_to }
    }
}

pub trait PayloadTrait {}

#[derive(Debug, Clone, Deserialize, Serialize)]
//#[serde(untagged)]
#[serde(tag = "type")]
pub enum ReqPayload {
    #[serde(rename = "init")]
    Init(InitPayload),
    #[serde(rename = "topology")]
    Topology(TopoPayload),
    #[serde(rename = "read")]
    Read,
    #[serde(rename = "broadcast")]
    Broadcast(GossipPayload),
    #[serde(rename = "broadcast_ok")]
    InterServerGossipOk(BroadcastOkPayload),
}

impl PayloadTrait for ReqPayload {}

#[derive(Debug, Clone, Deserialize, Serialize)]
//#[serde(untagged)]
#[serde(tag = "type")]
pub enum ReplyPayload {
    #[serde(rename = "init_ok")]
    InitOk(InitOkPayload),
    #[serde(rename = "topology_ok")]
    TopologyOk(TopologyOkPayload),
    #[serde(rename = "read_ok")]
    ReadOk(ReadOkPayload),
    #[serde(rename = "broadcast")]
    Gossip(GossipPayload),
    #[serde(rename = "broadcast_ok")]
    BroadcastOk(BroadcastOkPayload),
}

impl PayloadTrait for ReplyPayload {}

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
