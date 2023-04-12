use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::io::{self, Write};
use std::sync::{Arc, Condvar, Mutex, RwLock};

use crate::raft::msg::{
    Body, ErrorPayload, Message, OpPayloadTrait, PayloadTrait, ReadOkPayload, ReadPayload,
    ReqPayload, SendPayload, WritePayload,
};

const SVC: &str = "lin-kv";

pub fn log<M>(msg: &M)
where
    M: Serialize,
{
    let mut stderr = io::stderr();
    serde_json::to_writer(&stderr, &msg).unwrap();
    stderr.write_all(b"\n").unwrap();
    stderr.flush().unwrap();
}

#[derive(Debug)]
pub struct Map {
    map: HashMap<usize, usize>,
}

impl Map {
    pub fn new(map: HashMap<usize, usize>) -> Self {
        Map { map }
    }

    pub fn apply_read(&mut self, key: &usize) -> Result<SendPayload, ErrorPayload> {
        if let Some(value) = self.map.get(key) {
            Ok(SendPayload::ReadOk(ReadOkPayload::new(*value)))
        } else {
            Err(ErrorPayload::new(20, format!("Key {:?} not found", key)))
        }
    }

    pub fn apply_write(&mut self, key: &usize, value: &usize) -> Result<SendPayload, ErrorPayload> {
        self.map.insert(key.to_owned(), value.to_owned());
        Ok(SendPayload::WriteOk)
    }

    pub fn apply_cas(
        &mut self,
        key: &usize,
        from: &usize,
        to: &usize,
    ) -> Result<SendPayload, ErrorPayload> {
        if let Some(value) = self.map.get(key) {
            if value == from {
                self.map.insert(*key, to.to_owned());
                Ok(SendPayload::CasOk)
            } else {
                Err(ErrorPayload::new(
                    22,
                    format!("Value {:?} not equal to {:?}", value, from),
                ))
            }
        } else {
            Err(ErrorPayload::new(20, format!("Key {:?} not found", key)))
        }
    }

    pub fn apply<O>(&mut self, op: O) -> Result<SendPayload, ErrorPayload>
    where
        O: OpPayloadTrait,
    {
        op.apply(self)
    }
}

#[derive(Debug)]
pub struct Node {
    pub node_id: String,
    next_msg_id: RwLock<usize>,
    map: RwLock<Map>,
    lock: Mutex<()>,
}

impl Node {
    pub fn new(node_id: String) -> Self {
        Node {
            node_id,
            next_msg_id: RwLock::new(1),
            map: RwLock::new(Map::new(HashMap::new())),
            lock: Mutex::new(()),
        }
    }

    pub fn build_body<P>(&self, payload: P, in_reply_to: Option<usize>) -> Body<P>
    where
        P: PayloadTrait,
    {
        let body = Body::new(
            payload,
            Some(self.next_msg_id.read().unwrap().clone()),
            in_reply_to,
        );
        let mut nxt_msg_id_guard = self.next_msg_id.write().unwrap();
        *nxt_msg_id_guard += 1;
        body
    }
}

pub fn handle_op<O>(op_payload: O, node: &Arc<Node>) -> SendPayload
where
    O: OpPayloadTrait,
{
    let mut map_guard = node.map.write().unwrap();
    let reply_payload = match map_guard.apply(op_payload) {
        Ok(payload) => payload,
        Err(err) => SendPayload::Error(err),
    };
    reply_payload
}

pub fn handle_msg(request: Message<ReqPayload>, node: Arc<Node>) -> Result<(), ()> {
    let _lock = node.lock.lock().unwrap();

    eprintln!("Body : {:?}", request.body);
    let Body {
        payload: req_payload,
        msg_id: msg_id_opt,
        in_reply_to: _,
    } = request.body;

    // send response
    let reply_payload = match req_payload {
        ReqPayload::Init(_) => Ok(SendPayload::InitOk),
        ReqPayload::Error(err) => {
            eprintln!("Error received: {:?}", err);
            Err(())
        }
        ReqPayload::Read(op_p) => Ok(handle_op(op_p, &node)),
        ReqPayload::Write(op_p) => Ok(handle_op(op_p, &node)),
        ReqPayload::Cas(op_p) => Ok(handle_op(op_p, &node)),
        /*(ReqPayload::ReadOk(value), _, Some(reply_to)) => {
            eprintln!("read ok : {:?} to {:}", value, reply_to);
            (None, None)
        }
        (ReqPayload::WriteOk, _, Some(reply_to)) => {
            eprintln!("Promise write ok to: {:?}", reply_to);
            (None, None)
        }
        (ReqPayload::CasOk, _, Some(reply_to)) => {
            eprintln!("Promise cas ok to: {:?}", reply_to);
            (None, None)
        }
        (ReqPayload::ErrorPayload(err), _, Some(reply_to)) => {
            eprintln!("Linkv error: {:?} replying to: {:}", err, &reply_to);
            (None, None)
        }*/
    }?;

    /*let reply_dest = match reply_payload {
        SendPayload::ReadOk(_) => SVC.to_owned(),
        SendPayload::WriteOk => SVC.to_owned(),
        SendPayload::CasOk => SVC.to_owned(),
        SendPayload::Error(_) => SVC.to_owned(),
        SendPayload::InitOk => request.src,
    };*/

    // Send reply
    let body = node.build_body(reply_payload, msg_id_opt);
    log(&format!("Send {:?}", body));
    Message::new(body, request.src, node.node_id.clone()).send();

    Ok(())
}
