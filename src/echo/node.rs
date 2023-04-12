use crate::echo::msg::{Body, EchoOkPayload, InitOkPayload, Message, Payload};
use crate::output::to_stderr;

#[derive(Debug)]
pub struct Node {
    node_id: String,
    next_msg_id: usize,
}

impl Node {
    pub fn new(node_id: String) -> Self {
        Node {
            node_id,
            next_msg_id: 0,
        }
    }

    pub fn handle_msg(&mut self, request: &Message) -> Result<Message, ()> {
        let req_msg_id = request.body.msg_id;

        let payload = match &request.body.payload {
            Payload::Init(init_p) => {
                to_stderr(&format!("Initiated node {:}", init_p.get()).to_string());
                Ok(Payload::InitOk(InitOkPayload::new(req_msg_id)))
            }
            Payload::Echo(echo_p) => {
                to_stderr(&format!("Echoing {:}", echo_p.get()).to_string());
                Ok(Payload::EchoOk(EchoOkPayload::new(
                    echo_p.get(),
                    req_msg_id,
                )))
            }
            _ => Err(()),
        }?;

        let body_send = Body::new(payload, self.next_msg_id);

        self.next_msg_id += 1;

        Ok(Message::new(
            request.src.to_string(),
            body_send,
            self.node_id.clone(),
        ))
    }
}
