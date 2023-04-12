use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use crate::datomic::msg::{Body, LinKvError, LinKvPayload, LinKvReplyValue, Message, MessageDest};

static TIMEOUT: u64 = 25;

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

    pub fn deliver(&self, value_payload: LinKvReplyValue) {
        let mut value_guard = self.value.lock().unwrap();
        *value_guard = Some(value_payload);
        self.cvar.notify_one();
    }

    pub fn sync_rpc(
        &self,
        node_id: String,
        body: Body<LinKvPayload>,
    ) -> Result<Option<LinKvReplyValue>, LinKvError> {
        let rpc_msg_id = body.msg_id.unwrap();
        let lin_kv_msg = Message::new(MessageDest::LinKv, body, node_id);
        lin_kv_msg.send();

        // Block this thread until value is received
        let result = self
            .cvar
            .wait_timeout(self.value.lock().unwrap(), Duration::from_millis(TIMEOUT))
            .unwrap();

        if result.1.timed_out() {
            eprintln!("Promise timed out!");
            let timeout_err = LinKvError::new(0, format!("No response from: {:?}", rpc_msg_id));
            return Err(timeout_err);
        }

        //let mut value_guard = self.value.lock().unwrap();
        let mut value = result.0;
        eprintln!("Promise returned: {:?}", value);
        match value.take() {
            Some(LinKvReplyValue::Error(err)) => Err(err),
            value_opt => Ok(value_opt),
        }
    }
}
