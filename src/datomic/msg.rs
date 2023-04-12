use serde::{self, ser::SerializeMap, Deserialize, Deserializer, Serialize, Serializer};
use std::collections::{HashMap, HashSet};
use std::io::{self, Write};

use crate::datomic::thunk::{Thunk, ThunkMap, ThunkValues, ThunkWriteEnum};
use crate::datomic::txn::TxnOp;

use super::thunk::ThunkTrait;

#[derive(Debug, Clone)]
struct LinKvBody {
    key: usize,
}

impl Serialize for LinKvBody {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut s = serializer.serialize_map(Some(2))?;
        s.serialize_entry("type", "init")?;
        s.serialize_entry("key", &self.key)?;
        s.end()
    }
}

#[derive(Debug, Clone)]
pub enum MessageDest {
    LinKv,
    VarDest(String),
}

const SVC: &str = "lin-kv";
//const SVC: &str = "lww-kv";

impl Serialize for MessageDest {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let dest = match &self {
            MessageDest::LinKv => SVC,
            MessageDest::VarDest(var_dest) => var_dest,
        };

        serializer.serialize_str(dest)
    }
}

impl<'de> Deserialize<'de> for MessageDest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let dest = String::deserialize(deserializer)?;
        match dest.as_str() {
            SVC => Ok(MessageDest::LinKv),
            _ => Ok(MessageDest::VarDest(dest)),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct Message<P: PayloadTrait> {
    pub src: String,
    pub dest: MessageDest,
    pub body: Body<P>,
}

impl<P> Message<P>
where
    P: PayloadTrait + Serialize,
{
    pub fn new(dest: MessageDest, body: Body<P>, src: String) -> Self {
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
pub struct TxnPayload {
    pub txn: Vec<TxnOp>,
}

impl TxnPayload {
    pub fn new(txn: Vec<TxnOp>) -> Self {
        TxnPayload { txn }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct LinKvRootOk {
    pub value: String,
}

impl LinKvRootOk {
    pub fn new(value: String) -> Self {
        LinKvRootOk { value }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct LinKvReadThunkOk {
    pub value: ThunkValues,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct LinKvReadMapOk {
    pub value: HashMap<String, String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct LinKvError {
    pub code: usize,
    pub text: String,
}

impl LinKvError {
    pub fn new(code: usize, text: String) -> Self {
        LinKvError { code, text }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(untagged)]
pub enum LinKvReplyValue {
    RootOk(LinKvRootOk),
    ReadThunkOk(LinKvReadThunkOk),
    ReadMapOk(LinKvReadMapOk),
    WriteOk(),
    CasOk(),
    Error(LinKvError),
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ReadOkPayload {
    pub value: LinKvReplyValue,
}

impl ReadOkPayload {
    pub fn new(value: LinKvReplyValue) -> Self {
        ReadOkPayload { value }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TxnOkPayload {
    pub txn: Vec<TxnOp>,
}

impl TxnOkPayload {
    pub fn new(txn: Vec<TxnOp>) -> Self {
        TxnOkPayload { txn }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct LinKvReadRootPayload {
    pub key: String,
}

impl LinKvReadRootPayload {
    pub fn new() -> Self {
        LinKvReadRootPayload {
            key: "root".to_string(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct LinKvReadPayload {
    pub key: String,
}

impl LinKvReadPayload {
    pub fn new(key: String) -> Self {
        LinKvReadPayload { key }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct LinKvWritePayload<'a> {
    pub key: String,
    pub value: &'a ThunkWriteEnum<'a>,
}

impl<'a> LinKvWritePayload<'a> {
    pub fn new(key: String, value: &'a ThunkWriteEnum) -> Self {
        LinKvWritePayload { key, value }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct LinKvCasPayload {
    pub key: usize,
    pub from: Vec<usize>,
    pub to: Vec<usize>,
    pub create_if_not_exists: bool,
}

impl LinKvCasPayload {
    pub fn new(key: usize, from: Vec<usize>, to: Vec<usize>) -> Self {
        LinKvCasPayload {
            key,
            from,
            to,
            create_if_not_exists: true,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct LinKvCasRootPayload {
    pub key: String,
    pub from: Thunk<ThunkMap>,
    pub to: Thunk<ThunkMap>,
    pub create_if_not_exists: bool,
}

impl LinKvCasRootPayload {
    pub fn new(from: Thunk<ThunkMap>, to: Thunk<ThunkMap>) -> Self {
        LinKvCasRootPayload {
            key: "root".to_owned(),
            from,
            to,
            create_if_not_exists: true,
        }
    }
}

pub trait PayloadTrait {}

#[derive(Debug, Clone, Deserialize, Serialize)]
//#[serde(untagged)]
#[serde(tag = "type")]
pub enum ReqPayload {
    #[serde(rename = "init")]
    Init(InitPayload),
    #[serde(rename = "txn")]
    Txn(TxnPayload),
    //#[serde(rename = "read_ok")]
    //RootOk(LinKvReplyValue),
    #[serde(rename = "read_ok")]
    ReadOk(LinKvReplyValue),
    #[serde(rename = "write_ok")]
    WriteOk,
    #[serde(rename = "cas_ok")]
    CasOk,
    #[serde(rename = "error")]
    LinKvError(LinKvError),
}

impl PayloadTrait for ReqPayload {}

pub trait SendTrait {}

#[derive(Debug, Clone, Deserialize, Serialize)]
//#[serde(untagged)]
#[serde(tag = "type")]
pub enum SendPayload {
    #[serde(rename = "init_ok")]
    InitOk,
    #[serde(rename = "txn_ok")]
    TxnOk(TxnOkPayload),
    #[serde(rename = "error")]
    Error(LinKvError),
}

impl SendTrait for SendPayload {}
impl PayloadTrait for SendPayload {}

#[derive(Debug, Clone, Serialize)]
//#[serde(untagged)]
#[serde(tag = "type")]
pub enum LinKvPayload<'a> {
    #[serde(rename = "read")]
    Root(LinKvReadRootPayload),
    #[serde(rename = "read")]
    Read(LinKvReadPayload),
    #[serde(rename = "write")]
    Write(LinKvWritePayload<'a>),
    #[serde(rename = "cas")]
    CasRoot(LinKvCasRootPayload),
    #[serde(rename = "cas")]
    Cas(LinKvCasPayload),
}

impl<'a> SendTrait for LinKvPayload<'a> {}
impl<'a> PayloadTrait for LinKvPayload<'a> {}

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
