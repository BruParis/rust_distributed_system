use super::msg::{MessageDest, PayloadTrait, ReqPayload, SendPayload};
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::io::{self, Write};
use std::sync::{Arc, Condvar, Mutex, RwLock};

use crate::datomic::msg::{
    Body, LinKvCasRootPayload, LinKvError, LinKvPayload, LinKvReadPayload, LinKvReadRootPayload,
    LinKvReplyValue, LinKvWritePayload, Message, TxnOkPayload,
};
use crate::datomic::promise::Promise;
use crate::datomic::thunk::{Thunk, ThunkMap, ThunkValues, ThunkWriteEnum};
use crate::datomic::txn::{TxnOp, TxnReadOp};

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
pub struct Node {
    pub node_id: String,
    next_msg_id: RwLock<usize>,
    next_thunk_id: RwLock<usize>,
    pub promise_map: RwLock<HashMap<usize, Arc<Promise>>>,
}

impl Node {
    pub fn new(node_id: String) -> Self {
        Node {
            node_id,
            next_msg_id: RwLock::new(1),
            next_thunk_id: RwLock::new(0),
            promise_map: RwLock::new(HashMap::new()),
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

    fn new_id(&self) -> usize {
        let mut next_thunk_id_guard = self.next_thunk_id.write().unwrap();
        let id = *next_thunk_id_guard;
        *next_thunk_id_guard += 1;
        id
    }

    pub fn new_promise(&self, promise_msg_id: usize) -> Arc<Promise> {
        let mut promise_map_guard = self.promise_map.write().unwrap();
        let promise = Arc::new(Promise::new());
        let stored_promise = promise.clone();
        promise_map_guard.insert(promise_msg_id, stored_promise);
        promise
    }

    pub fn map_transact(
        &self,
        map0: &mut Thunk<ThunkMap>,
        txn0: &Vec<TxnOp>,
    ) -> Result<(Thunk<ThunkMap>, Vec<TxnOp>), LinKvError> {
        let mut map_value = map0.get_value(&self)?;
        eprintln!("map_value: {:?}", map_value);

        let txn1_res: Result<Vec<TxnOp>, LinKvError> =
            txn0.into_iter().try_fold(vec![], |mut accu, mop| {
                let key = mop.get_key();
                match mop {
                    TxnOp::Read(_) => {
                        let new_txn = {
                            if let Some(thunk) = map_value.get_mut(&key) {
                                let thunk_value = thunk.get_value(&self)?;
                                TxnOp::Read(TxnReadOp::new(key, thunk_value))
                            } else {
                                TxnOp::Read(TxnReadOp::new(key, vec![]))
                            }
                        };
                        accu.push(new_txn);
                        Ok(accu)
                    }
                    TxnOp::Append(append_op) => {
                        let mut new_thunk_value = {
                            if let Some(thunk) = map_value.get_mut(&key) {
                                thunk.get_value(&self)?
                            } else {
                                vec![]
                            }
                        };

                        new_thunk_value.push(append_op.value);
                        let new_thunk =
                            Thunk::new(self.node_id.to_owned(), self.new_id(), new_thunk_value);

                        map_value.insert(key, new_thunk);

                        accu.push(TxnOp::Append(append_op.clone()));
                        Ok(accu)
                    }
                }
            });

        let map1 = {
            if map_value.is_empty() {
                // if empty, just copy map0, as it is already saved
                // -> avoid useless read error when getting values
                map0.clone()
            } else {
                Thunk::new(self.node_id.to_owned(), self.new_id(), map_value)
            }
        };

        eprintln!("map1: {:?}", map1);

        let txn1 = txn1_res?;

        Ok((map1, txn1))
    }

    fn init_map(&self, map_id: String, thunk_write_enum: ThunkWriteEnum) -> Result<(), LinKvError> {
        // init root value
        let write_payload = LinKvWritePayload::new(map_id.to_owned(), &thunk_write_enum);
        let init_body = self.build_body(LinKvPayload::Write(write_payload), None);
        let promise_msg_id = init_body.msg_id.unwrap();
        self.new_promise(promise_msg_id)
            .sync_rpc(self.node_id.clone(), init_body)?;
        Ok(())
    }

    fn cas_root(&self, map0: Thunk<ThunkMap>, map1: Thunk<ThunkMap>) -> Result<(), LinKvError> {
        let cas_payload = LinKvCasRootPayload::new(map0, map1);
        let cas_body = self.build_body(LinKvPayload::CasRoot(cas_payload), None);
        let cas_msg_id = cas_body.msg_id.unwrap();

        let cas_promise = self.new_promise(cas_msg_id);

        let cas_promise_res = cas_promise.sync_rpc(self.node_id.clone(), cas_body)?;
        if let Some(LinKvReplyValue::CasOk()) = cas_promise_res {
            eprintln!("CAS succeded!");
            return Ok(());
        } else {
            eprintln!("CAS root failed!");
            let cas_error = LinKvError::new(30, "CAS failed!".to_owned());
            return Err(cas_error);
        }
    }

    fn transact(&self, txn0: &Vec<TxnOp>) -> Result<Vec<TxnOp>, LinKvError> {
        // read value from key with lin-kv
        let read_body = self.build_body(LinKvPayload::Root(LinKvReadRootPayload::new()), None);
        let promise_msg_id = read_body.msg_id.unwrap();
        let root_res = self
            .new_promise(promise_msg_id)
            .sync_rpc(self.node_id.clone(), read_body);

        //let mut map0: Thunk<ThunkMap> = HashMap::new();
        let mut map0 = match root_res {
            Ok(Some(LinKvReplyValue::RootOk(r_p))) => Ok(Thunk::from_id(r_p.value)),
            Err(LinKvError { code: 20, text: _ }) => {
                log(&format!(
                    "LinKv returned empty root for: {}",
                    promise_msg_id
                ));

                // Dummy request to create {} at root
                let new_map = Thunk::new(self.node_id.to_owned(), self.new_id(), ThunkMap::new());

                // init root value
                self.init_map(
                    "root".to_owned(),
                    ThunkWriteEnum::Root(new_map.id.to_owned()),
                )?;
                let empty_map = HashMap::new();
                self.init_map(new_map.id.to_owned(), ThunkWriteEnum::Map(&empty_map))?;

                Ok(new_map)
            }
            _ => {
                let abort_error = LinKvError::new(
                    14,
                    format!("Could not read root from: {:?}", promise_msg_id),
                );
                Err(abort_error)
            }
        }?;

        let (mut map1, txn1) = self.map_transact(&mut map0, txn0)?;

        // Save all thunks values
        map1.save(&self)?;

        self.cas_root(map0, map1)?;

        Ok(txn1)
    }

    pub fn forward_to_promise(&self, reply_to: usize, reply: LinKvReplyValue) {
        let mut promise_map_guard = self.promise_map.write().unwrap();
        if let Some(promise) = promise_map_guard.remove(&reply_to) {
            promise.deliver(reply);
        } else {
            log(&format!("Promise not found for : {}", reply_to));
        }
    }
}

pub fn handle_msg(request: Message<ReqPayload>, node: Arc<Node>) -> Result<(), ()> {
    eprintln!("Body : {:?}", request.body);
    let Body {
        payload: req_payload,
        msg_id: msg_id_opt,
        in_reply_to: reply_to_opt,
    } = request.body;

    // send response
    let (reply_payload_opt, id_reply_opt) = match (req_payload, msg_id_opt, reply_to_opt) {
        (ReqPayload::Init(_), Some(msg_id_opt), _) => (Some(SendPayload::InitOk), Some(msg_id_opt)),
        (ReqPayload::Txn(p), Some(msg_id_opt), _) => {
            let payload = match node.transact(&p.txn) {
                Ok(txn2) => {
                    let txn_ok_payload = TxnOkPayload::new(txn2);
                    Some(SendPayload::TxnOk(txn_ok_payload))
                }
                Err(cas_err) => Some(SendPayload::Error(cas_err)),
            };
            (payload, Some(msg_id_opt))
        }
        (ReqPayload::ReadOk(value), _, Some(reply_to)) => {
            eprintln!("Promise read ok : {:?} to {:}", value, reply_to);
            node.forward_to_promise(reply_to, value);
            (None, None)
        }
        (ReqPayload::WriteOk, _, Some(reply_to)) => {
            eprintln!("Promise write ok to: {:?}", reply_to);
            node.forward_to_promise(reply_to, LinKvReplyValue::WriteOk());
            (None, None)
        }
        (ReqPayload::CasOk, _, Some(reply_to)) => {
            eprintln!("Promise cas ok to: {:?}", reply_to);
            node.forward_to_promise(reply_to, LinKvReplyValue::CasOk());
            (None, None)
        }
        (ReqPayload::LinKvError(err), _, Some(reply_to)) => {
            eprintln!("Linkv error: {:?} replying to: {:}", err, &reply_to);
            node.forward_to_promise(reply_to, LinKvReplyValue::Error(err));
            (None, None)
        }
        _ => (None, None),
    };

    // Send reply
    if let Some(payload) = reply_payload_opt {
        let body = node.build_body(payload, id_reply_opt);
        log(&format!("Send {:?}", body));
        Message::new(
            MessageDest::VarDest(request.src.to_owned()),
            body,
            node.node_id.clone(),
        )
        .send();
    }

    Ok(())
}
