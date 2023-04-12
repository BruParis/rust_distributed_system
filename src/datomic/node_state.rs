use super::msg::{LinKvCasPayload, MessageDest, PayloadTrait, ReqPayload, SendPayload};
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::io::{self, Write};
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::time::Duration;

use crate::datomic::msg::{
    Body, InitOkPayload, LinKvPayload, LinKvReadPayload, LinKvReplyValue, Message, TxnOkPayload,
};
use crate::datomic::txn::{TxnOp, TxnReadOp};

static TIMEOUT: u64 = 5;

#[derive(Debug)]
pub struct Promise {
    value: Arc<Mutex<Option<LinKvReplyValue>>>,
    cvar: Condvar,
}

impl Promise {
    pub fn new() -> Self {
        Promise {
            value: Arc::new(Mutex::new(None)),
            cvar: Condvar::new(),
        }
    }

    fn deliver(&self, value_payload: LinKvReplyValue) {
        let mut value_guard = self.value.lock().unwrap();
        *value_guard = Some(value_payload);
        self.cvar.notify_one();
    }

    fn sync_rpc(&self, node_id: String, body: Body<LinKvPayload>) -> Option<LinKvReplyValue> {
        let lin_kv_msg = Message::new(MessageDest::LinKv, body, node_id);
        lin_kv_msg.send();

        // Block this thread until value is received
        let result = self
            .cvar
            .wait_timeout(self.value.lock().unwrap(), Duration::from_millis(TIMEOUT))
            .unwrap();

        //let mut value_guard = self.value.lock().unwrap();
        let mut value = result.0;
        if let None = *value {
            eprintln!("Promised timed out!");
        }

        value.take()
    }
}

#[derive(Debug)]
pub struct Node {
    pub node_id: String,
    next_msg_id: RwLock<usize>,
    pub neighbors: RwLock<HashSet<String>>,
    pub promise_map: RwLock<HashMap<usize, Arc<Promise>>>,
}

impl Node {
    pub fn new(node_id: String, neighbors: HashSet<String>) -> Self {
        Node {
            node_id,
            next_msg_id: RwLock::new(0),
            neighbors: RwLock::new(neighbors),
            promise_map: RwLock::new(HashMap::new()),
        }
    }

    fn new_promise(&self, promise_msg_id: usize) -> Arc<Promise> {
        let mut promise_map_guard = self.promise_map.write().unwrap();
        let promise = Arc::new(Promise::new());
        let stored_promise = promise.clone();
        promise_map_guard.insert(promise_msg_id, stored_promise);
        promise
    }

    fn transact(&self, txn: &Vec<TxnOp>) -> Vec<TxnOp> {
        let txn2: Vec<TxnOp> = txn
            .iter()
            .filter_map(|mop| {
                let key = mop.get_key();

                // read value from key with lin-kv
                let read_body = self.build_body(LinKvPayload::Read(LinKvReadPayload::new(key)));
                let promise_msg_id = read_body.msg_id.unwrap();

                let promise = self.new_promise(promise_msg_id);

                // We must have read a value from the key
                let mop_key = mop.get_key();
                match mop {
                    TxnOp::Read(_) => {
                        if let Some(LinKvReplyValue::ReadOk(r_p)) =
                            promise.sync_rpc(self.node_id.clone(), read_body)
                        {
                            // TxnRead could have null value (if coming from maelstrom client)
                            Some(TxnOp::Read(TxnReadOp::new(mop_key, r_p.value)))
                        } else {
                            self.log(&format!(
                                "LinKv did not returned read key {} from promise: {}",
                                key, promise_msg_id
                            ));
                            return None;
                        }
                    }
                    TxnOp::Append(append_op) => {
                        let read_opt = promise.sync_rpc(self.node_id.clone(), read_body);

                        let read_0 = match read_opt {
                            Some(LinKvReplyValue::ReadOk(r_p)) => r_p.value,
                            _ => {
                                self.log(&format!(
                                    "LinKv did not returned read key {} from promise: {}",
                                    key, promise_msg_id
                                ));
                                vec![]
                            }
                        };

                        // Create a copy with appended value ...
                        let mut read_1 = read_0.clone();
                        read_1.push(append_op.value);

                        // ... and write it back iff it hasn't changed
                        let cas_payload = LinKvCasPayload::new(mop_key, read_0, read_1);
                        let cas_body = self.build_body(LinKvPayload::Cas(cas_payload));
                        let cas_msg_id = cas_body.msg_id.unwrap();

                        let cas_promise = self.new_promise(cas_msg_id);

                        if let Some(LinKvReplyValue::CasOk(_)) =
                            cas_promise.sync_rpc(self.node_id.clone(), cas_body)
                        {
                            Some(TxnOp::Append(append_op.clone()))
                        } else {
                            // TODO: create specific error
                            eprintln!("CAS of {} failed!", key);
                            return None;
                        }
                    }
                }
            })
            .collect();
        return txn2;
    }

    pub fn log<M>(&self, msg: &M)
    where
        M: Serialize,
    {
        let mut stderr = io::stderr();
        serde_json::to_writer(&stderr, &msg).unwrap();
        stderr.write_all(b"\n").unwrap();
        stderr.flush().unwrap();
    }

    pub fn build_body<P>(&self, payload: P) -> Body<P>
    where
        P: PayloadTrait,
    {
        let body = Body::new(payload, Some(self.next_msg_id.read().unwrap().clone()));
        let mut nxt_msg_id_guard = self.next_msg_id.write().unwrap();
        *nxt_msg_id_guard += 1;
        body
    }

    pub fn handle_msg(&self, request: Message<ReqPayload>) -> Result<(), ()> {
        self.log(&format!("Received {:?}", request));

        let Body {
            payload: req_payload,
            msg_id: msg_id_opt,
        } = request.body;

        // send response
        let reply_payload_opt = match (req_payload, msg_id_opt) {
            (ReqPayload::Init(_), Some(msg_id)) => {
                Some(SendPayload::InitOk(InitOkPayload::new(msg_id)))
            }
            (ReqPayload::Txn(p), Some(msg_id)) => {
                let txn2 = self.transact(&p.txn);
                Some(SendPayload::TxnOk(TxnOkPayload::new(msg_id, txn2)))
            }
            (
                ReqPayload::ReadOk(value)
                | ReqPayload::CasOk(value)
                | ReqPayload::LinKvError(value),
                _,
            ) => {
                let mut promise_map_guard = self.promise_map.write().unwrap();
                if let Some(promise) = promise_map_guard.get_mut(&value.get_reply_to()) {
                    promise.deliver(value);
                } else {
                    eprintln!("Promise not found!");
                }
                None
            }
            _ => None,
        };

        if let Some(payload) = reply_payload_opt {
            let body = self.build_body(payload);
            self.log(&format!("Send {:?}", body));
            Message::new(
                MessageDest::VarDest(request.src.to_owned()),
                body,
                self.node_id.clone(),
            )
            .send();
        }

        Ok(())
    }
}
