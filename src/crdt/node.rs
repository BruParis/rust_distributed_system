use crate::crdt::crdt::{CrdtElem, CrdtTrait};
use crate::crdt::msg::{AddOkPayload, Body, InitOkPayload, Message};

use super::msg::{PayloadTrait, ReadOkPayload, ReqPayload, SendPayload};
use serde::Serialize;
use std::collections::HashSet;

use std::io::{self, Write};
use std::sync::RwLock;

#[derive(Debug)]
pub struct Node<C>
where
    C: CrdtTrait,
{
    pub node_id: String,
    next_msg_id: RwLock<usize>,
    pub crdt: C,
    pub neighbors: RwLock<HashSet<String>>,
}

impl<C: CrdtTrait> Node<C> {
    pub fn new(node_id: String, neighbors: HashSet<String>) -> Self {
        Node {
            node_id,
            next_msg_id: RwLock::new(0),
            crdt: C::new(&neighbors),
            neighbors: RwLock::new(neighbors),
        }
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
        let body = Body::new(payload, self.next_msg_id.read().unwrap().clone());
        let mut nxt_msg_id_guard = self.next_msg_id.write().unwrap();
        *nxt_msg_id_guard += 1;
        body
    }

    pub fn handle_msg(&self, request: Message<ReqPayload>) -> Result<(), ()> {
        self.log(&format!("Received {:?}", request));

        let req_msg_id = request.body.msg_id;

        // effect from request
        match &request.body.payload {
            ReqPayload::Add(add_p) => {
                add_p.element.map(|e| self.crdt.add(CrdtElem::GSetElem(e)));
                add_p.delta.map(|d| {
                    self.crdt
                        .add(CrdtElem::PNCounterDelta(self.node_id.clone(), d))
                });
            }
            ReqPayload::Replicate(replicate_p) => {
                self.crdt.merge(replicate_p.data.clone());
            }
            _ => (),
        };

        // send response
        let reply_payload_opt = match request.body.payload {
            ReqPayload::Init(_) => Some(SendPayload::InitOk(InitOkPayload::new(req_msg_id))),
            ReqPayload::Add(_) => Some(SendPayload::AddOk(AddOkPayload::new(req_msg_id))),
            ReqPayload::Read => {
                self.log(&format!("Request READ {:?}", request.src));
                let crdt_json = self.crdt.read_json();
                Some(SendPayload::ReadOk(ReadOkPayload::new(
                    req_msg_id, crdt_json,
                )))
            }
            _ => None,
        };

        if let Some(payload) = reply_payload_opt {
            let body = self.build_body(payload);
            self.log(&format!("Send {:?}", body));
            Message::new(request.src.to_owned(), body, self.node_id.clone()).send();
        }

        Ok(())
    }
}
