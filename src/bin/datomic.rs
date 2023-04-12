use std::collections::HashMap;
use std::io::{self, Write};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::Arc;
use std::thread::{self, JoinHandle};

use echo_server::datomic::msg::{
    Body, LinKvPayload, LinKvReadMapOk, LinKvReplyValue, LinKvWritePayload, Message, MessageDest,
    ReqPayload, SendPayload,
};
use echo_server::datomic::node::{handle_msg, Node};
use echo_server::datomic::thunk::{Thunk, ThunkMap, ThunkValues, ThunkWriteEnum};

fn read_msg(input: String) -> Result<Message<ReqPayload>, serde_json::Error> {
    eprintln!("Read msg: {}", input);

    // Parse the line as a JSON object
    let res_msg: Message<ReqPayload> = serde_json::from_str(&input)?;
    res_msg.print();

    Ok(res_msg)
}

fn check_init(req: Message<ReqPayload>) -> Option<Node> {
    let Body {
        payload: req_payload,
        msg_id: msg_id_opt,
        in_reply_to: _,
    } = req.body;

    if let (ReqPayload::Init(init_p), Some(msg_id)) = (req_payload, msg_id_opt) {
        let node = Node::new(init_p.node_id);

        let send_payload = SendPayload::InitOk;
        let body = Body::new(send_payload, Some(0), Some(msg_id));
        Message::new(
            MessageDest::VarDest(req.src.to_owned()),
            body,
            node.node_id.clone(),
        )
        .send();

        Some(node)
    } else {
        None
    }
}

fn init_loop() -> Node {
    let stdin = io::stdin();

    // Loop endlessly until init
    loop {
        // Read a line from stdin
        let mut input = String::new();

        if let Err(e) = stdin.read_line(&mut input) {
            eprintln!("Error reading from stdin: {}", e);
            continue;
        }

        match read_msg(input) {
            Ok(msg) => {
                if let Some(new_node) = check_init(msg) {
                    return new_node;
                }
            }
            Err(e) => eprintln!("Error parsing message: {}", e),
        }
    }
}

fn main() {
    let node = init_loop();

    let (tx, rx): (Sender<String>, Receiver<String>) = mpsc::channel();

    // Spawn a thread to read input from stdin
    let input_thread = thread::spawn(move || {
        loop {
            let mut input = String::new();
            io::stdin()
                .read_line(&mut input)
                .expect("Failed to read from stdin");
            let input = input.trim().to_string();

            // Send input to the receiving thread
            tx.send(input).expect("Failed to send message to receiver");
        }
    });

    let arc_node = Arc::new(node);

    // Spawn a thread to handle messages
    let mut handle_vec: Vec<JoinHandle<()>> = vec![];
    while let Ok(input) = rx.recv() {
        let arc_node_clone = Arc::clone(&arc_node);
        let handler_thread = thread::spawn(move || match read_msg(input) {
            Ok(msg) => {
                if let Err(e) = handle_msg(msg, arc_node_clone) {
                    eprintln!("Error handling message: {:?}", e);
                }
            }
            Err(e) => eprintln!("Error parsing message: {}", e),
        });
        handle_vec.push(handler_thread);
    }

    // Join the threads
    input_thread.join().expect("Input thread panicked");
    handle_vec.into_iter().for_each(|t_handle| {
        t_handle.join().expect("Handler thread panicked");
    });
}
