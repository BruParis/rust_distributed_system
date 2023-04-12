use crate::crdt::crdt::CrdtTrait;
use crate::crdt::msg::{Message, ReplicatePayload, SendPayload};
use crate::crdt::node::Node;
use std::fmt;

pub struct Task<C: CrdtTrait> {
    pub callback: fn(&Node<C>),
    pub millisec: u64,
}

impl<C: CrdtTrait> fmt::Debug for Task<C> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_tuple("Task")
            .field(&format_args!("Callback"))
            .field(&format_args!("millisec: {:?}", self.millisec))
            .finish()
    }
}

pub fn replicate_set<C: CrdtTrait>(node: &Node<C>) {
    // ... replicate to all neighbors ...
    // TODO : handle LockResult<RwLockReadGuard<'_, T>> error
    if let Ok(neighbors_guard) = node.neighbors.read() {
        neighbors_guard.iter().for_each(|n| {
            let payload = SendPayload::Replicate(ReplicatePayload::new(node.crdt.data()));
            let body = node.build_body(payload);
            let msg = Message::new(n.to_owned(), body, node.node_id.clone());
            msg.send()
        });
    }
}
