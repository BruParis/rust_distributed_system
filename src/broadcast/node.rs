use crate::broadcast::msg::{
    Body, BroadcastOkPayload, GossipPayload, InitOkPayload, Message, TopologyOkPayload,
};

use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::io::{self, Write};
use std::sync::{Arc, Mutex, RwLock};
use std::thread::{self, sleep, JoinHandle};
use std::time::Duration;

use super::msg::{ReadOkPayload, ReplyPayload, ReqPayload};

#[derive(Debug)]
struct Rpc {
    done: Arc<Mutex<bool>>,
    join_handle: Option<JoinHandle<()>>,
}

impl Rpc {
    pub fn new() -> Self {
        Rpc {
            done: Arc::new(Mutex::new(false)),
            join_handle: None,
        }
    }

    pub fn replied(&mut self) {
        *self.done.lock().unwrap() = true
    }

    pub fn detach(&mut self, msg: Message<ReplyPayload>) {
        let done_clone = Arc::clone(&self.done);

        self.join_handle = Some(thread::spawn(move || loop {
            if let Ok(done_guard) = done_clone.lock() {
                if !*done_guard {
                    eprintln!("Retrying: {:?}", msg);
                    msg.send();
                } else {
                    break;
                }
            }
            sleep(Duration::from_millis(10));
        }));
    }
}

#[derive(Debug)]
pub struct Node {
    node_id: String,
    next_msg_id: RwLock<usize>,
    msg_set: RwLock<HashSet<usize>>,
    neighbors: RwLock<HashSet<String>>,
    // rpc_wait_map: RwLock<HashMap<String, Vec<Body<ReplyPayload>>>>,
    rpc_wait_map: RwLock<HashMap<(String, usize), Rpc>>,
}

impl Node {
    pub fn new(node_id: String) -> Self {
        Node {
            node_id,
            next_msg_id: RwLock::new(0),
            msg_set: RwLock::new(HashSet::new()),
            neighbors: RwLock::new(HashSet::new()),
            // rpc_wait_map: RwLock::new(HashMap::new()),
            rpc_wait_map: RwLock::new(HashMap::new()),
        }
    }

    fn log<M>(&self, msg: &M)
    where
        M: Serialize,
    {
        let mut stderr = io::stderr();
        serde_json::to_writer(&stderr, &msg).unwrap();
        stderr.write_all(b"\n").unwrap();
        stderr.flush().unwrap();
    }

    fn build_body(&self, payload: ReplyPayload) -> Body<ReplyPayload> {
        let body = Body::new(payload, self.next_msg_id.read().unwrap().clone());
        let mut nxt_msg_id_guard = self.next_msg_id.write().unwrap();
        *nxt_msg_id_guard += 1;
        body
    }

    fn clone_set(&self) -> HashSet<usize> {
        self.msg_set.read().unwrap().clone()
    }

    fn insert_msg(&self, msg: usize) -> bool {
        self.msg_set.write().unwrap().insert(msg)
    }

    pub fn kill_retry(&self) {
        self.rpc_wait_map
            .write()
            .unwrap()
            .iter_mut()
            .for_each(|(_, v)| {
                if let Some(join_handle) = v.join_handle.take() {
                    join_handle.join().unwrap();
                }
            });
    }

    pub fn handle_msg(&self, request: &mut Message<ReqPayload>) -> Result<(), ()> {
        self.log(&format!("Received {:?}", request));

        let req_msg_id = request.body.msg_id;

        // effect from request
        match &mut request.body.payload {
            ReqPayload::Init(init_p) => {
                self.log(&format!("Initiated node {:}", init_p.node_id));
            }
            ReqPayload::Topology(topo_p) => {
                if let Some(neighbors) = topo_p.topology.remove(&self.node_id) {
                    self.log(&format!("My neighbours are {:?}", neighbors));
                    let mut write_guard = self.neighbors.write().unwrap();
                    *write_guard = neighbors;
                } else {
                    self.log(&format!("No neighbours found for node {:?}", self.node_id));
                }
            }
            ReqPayload::Broadcast(broadcast_p) => {
                // store message
                // + inter-server message (if needed)

                // if message not yet inserted in set...
                if self.insert_msg(broadcast_p.message.clone()) {
                    // ... gossip to all neighbors ...
                    let req_src = &request.src;
                    self.neighbors
                        .read()
                        .unwrap()
                        .iter()
                        // ... except from the message sender in case it is a neighbor
                        .filter(|&n| n != req_src)
                        .map(|n| (broadcast_p.message.clone(), n.clone()))
                        .for_each(|(msg, n)| {
                            //  prepare body for rpc
                            let payload = ReplyPayload::Gossip(GossipPayload::new(msg));
                            let body = self.build_body(payload);

                            // store rpc in rpc_wait_map_aux
                            let msg = Message::new(n.to_owned(), body, self.node_id.clone());
                            let rpc_msg_id = msg.body.msg_id;
                            let mut rpc = Rpc::new();
                            rpc.detach(msg);

                            let mut rpc_map_guard_aux = self.rpc_wait_map.write().unwrap();
                            rpc_map_guard_aux.insert((n.to_owned(), rpc_msg_id), rpc);
                        });
                }
            }
            ReqPayload::InterServerGossipOk(gossip_ok_p) => {
                // remove message from rpc_wait_map
                let mut rpc_map_guard_aux = self.rpc_wait_map.write().unwrap();
                if let Some(rpc) =
                    rpc_map_guard_aux.get_mut(&(request.src.to_string(), gossip_ok_p.in_reply_to))
                {
                    self.log(&format!("RPC ACK {:?}", request.src));
                    rpc.replied();
                }
            }
            _ => (),
        };

        // send response
        let reply_payload_opt = match request.body.payload {
            ReqPayload::Init(_) => Some(ReplyPayload::InitOk(InitOkPayload::new(req_msg_id))),
            ReqPayload::Topology(_) => {
                Some(ReplyPayload::TopologyOk(TopologyOkPayload::new(req_msg_id)))
            }
            ReqPayload::Broadcast(_) => Some(ReplyPayload::BroadcastOk(BroadcastOkPayload::new(
                req_msg_id,
            ))),
            ReqPayload::Read => {
                let set_clone = self.clone_set();
                Some(ReplyPayload::ReadOk(ReadOkPayload::new(
                    req_msg_id, set_clone,
                )))
            }
            _ => None,
        };

        if let Some(payload) = reply_payload_opt {
            let body = self.build_body(payload);
            Message::new(request.src.to_owned(), body, self.node_id.clone()).send();
        }

        Ok(())
    }
}
