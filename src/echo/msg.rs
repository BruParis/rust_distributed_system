use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct Message {
    pub src: String,
    pub dest: String,
    pub body: Body,
}

impl Message {
    pub fn new(dest: String, body: Body, src: String) -> Self {
        Message { dest, body, src }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct InitPayload {
    node_id: String,
}

impl InitPayload {
    pub fn get(&self) -> String {
        self.node_id.clone()
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct EchoPayload {
    echo: String,
}

impl EchoPayload {
    pub fn get(&self) -> String {
        self.echo.clone()
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TopoPayload {
    topology: HashMap<String, String>,
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
pub struct EchoOkPayload {
    echo: String,
    in_reply_to: usize,
}

impl EchoOkPayload {
    pub fn new(echo: String, in_reply_to: usize) -> Self {
        EchoOkPayload { echo, in_reply_to }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(untagged)]
pub enum Payload {
    Init(InitPayload),
    Echo(EchoPayload),
    InitOk(InitOkPayload),
    EchoOk(EchoOkPayload),
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Body {
    #[serde(rename = "type")]
    msg_type: String,
    #[serde(flatten)]
    pub payload: Payload,
    pub msg_id: usize,
}

impl Body {
    pub fn new(payload: Payload, msg_id: usize) -> Self {
        let msg_type = match payload {
            Payload::Init(_) => "init",
            Payload::Echo(_) => "echo",
            Payload::InitOk(_) => "init_ok",
            Payload::EchoOk(_) => "echo_ok",
        };
        Body {
            msg_type: msg_type.to_string(),
            payload,
            msg_id,
        }
    }
}
