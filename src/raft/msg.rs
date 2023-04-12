use serde::{self, ser::SerializeMap, Deserialize, Deserializer, Serialize, Serializer};
use std::collections::{HashMap, HashSet};
use std::io::{self, Write};

use crate::raft::node::Map;

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
    pub fn new(body: Body<P>, dest: String, src: String) -> Self {
        Message { src, dest, body }
    }

    pub fn send(&self) {
        let mut stderr = io::stderr().lock();
        serde_json::to_writer(&mut stderr, &self).unwrap();
        stderr.write_all(b"\n").unwrap();
        stderr.flush().unwrap();

        let mut stdout = io::stdout().lock();
        serde_json::to_writer(&mut stdout, &self).unwrap();
        stdout.write_all(b"\n").unwrap();
        stdout.flush().unwrap();
    }

    pub fn print(&self) {
        let mut stderr = io::stderr().lock();
        serde_json::to_writer(&mut stderr, &self).unwrap();
        stderr.write_all(b"\n").unwrap();
        stderr.flush().unwrap();
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct InitPayload {
    pub node_id: String,
    pub node_ids: HashSet<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ErrorPayload {
    pub code: usize,
    pub text: String,
}

impl ErrorPayload {
    pub fn new(code: usize, text: String) -> Self {
        ErrorPayload { code, text }
    }
}

pub trait OpPayloadTrait {
    fn apply(&self, map: &mut Map) -> Result<SendPayload, ErrorPayload>;
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ReadPayload {
    pub key: usize,
}

impl ReadPayload {
    pub fn new(key: usize) -> Self {
        ReadPayload { key }
    }
}

impl OpPayloadTrait for ReadPayload {
    fn apply(&self, map: &mut Map) -> Result<SendPayload, ErrorPayload> {
        map.apply_read(&self.key)
    }
}

/*#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct LinKvReadOkPayload {
    pub value: String,
}

impl LinKvReadOkPayload {
    pub fn new(value: String) -> Self {
        LinKvReadOkPayload { value }
    }
}*/

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WritePayload {
    pub key: usize,
    pub value: usize,
}

impl WritePayload {
    pub fn new(key: usize, value: usize) -> Self {
        WritePayload { key, value }
    }
}
impl OpPayloadTrait for WritePayload {
    fn apply(&self, map: &mut Map) -> Result<SendPayload, ErrorPayload> {
        map.apply_write(&self.key, &self.value)
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CasPayload {
    pub key: usize,
    pub from: usize,
    pub to: usize,
}

impl CasPayload {
    pub fn new(key: usize, from: usize, to: usize) -> Self {
        CasPayload { key, from, to }
    }
}

impl OpPayloadTrait for CasPayload {
    fn apply(&self, map: &mut Map) -> Result<SendPayload, ErrorPayload> {
        map.apply_cas(&self.key, &self.from, &self.to)
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ReadOkPayload {
    pub value: usize,
}

impl ReadOkPayload {
    pub fn new(value: usize) -> Self {
        ReadOkPayload { value }
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
    Read(ReadPayload),
    #[serde(rename = "write")]
    Write(WritePayload),
    #[serde(rename = "cas")]
    Cas(CasPayload),
    #[serde(rename = "error")]
    Error(ErrorPayload),
}

impl PayloadTrait for ReqPayload {}

pub trait SendTrait {}

#[derive(Debug, Clone, Deserialize, Serialize)]
//#[serde(untagged)]
#[serde(tag = "type")]
pub enum SendPayload {
    #[serde(rename = "init_ok")]
    InitOk,
    #[serde(rename = "read_ok")]
    ReadOk(ReadOkPayload),
    #[serde(rename = "write_ok")]
    WriteOk,
    #[serde(rename = "cas_ok")]
    CasOk,
    #[serde(rename = "error")]
    Error(ErrorPayload),
}

impl SendTrait for SendPayload {}
impl PayloadTrait for SendPayload {}

/*#[derive(Debug, Clone, Serialize)]
//#[serde(untagged)]
#[serde(tag = "type")]
pub enum LinKvPayload {
    #[serde(rename = "read")]
    Read(LinKvReadPayload),
    #[serde(rename = "write")]
    Write(LinKvWritePayload),
    #[serde(rename = "cas")]
    Cas(LinKvCasPayload),
}*/

// impl SendTrait for LinKvPayload {}
// impl PayloadTrait for LinKvPayload {}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Body<P: PayloadTrait> {
    #[serde(flatten)]
    pub payload: P,
    pub msg_id: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub in_reply_to: Option<usize>,
}

impl<P> Body<P>
where
    P: PayloadTrait,
{
    pub fn new(payload: P, msg_id: Option<usize>, in_reply_to: Option<usize>) -> Self {
        Body {
            payload,
            msg_id,
            in_reply_to,
        }
    }
}
