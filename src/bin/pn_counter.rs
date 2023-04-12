use std::io::{self};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::Arc;
use std::thread::{self, sleep, JoinHandle};
use std::time::Duration;

use echo_server::crdt::msg::{InitOkPayload, Message, ReqPayload, SendPayload};
use echo_server::crdt::node::Node;
use echo_server::crdt::pncounter::PNCounter;
use echo_server::crdt::tasks::{replicate_set, Task};

fn read_msg(input: String) -> Result<Message<ReqPayload>, serde_json::Error> {
    eprintln!("Read msg: {}", input);

    // Parse the line as a JSON object
    let res_msg = serde_json::from_str(&input)?;

    Ok(res_msg)
}

fn check_init(req: Message<ReqPayload>) -> Option<Node<PNCounter>> {
    if let ReqPayload::Init(mut init_p) = req.body.payload {
        init_p.node_ids.remove(&init_p.node_id);
        let node = Node::new(init_p.node_id, init_p.node_ids);

        let send_payload = SendPayload::InitOk(InitOkPayload::new(req.body.msg_id));
        let body = node.build_body(send_payload);
        Message::new(req.src.to_owned(), body, node.node_id.clone()).send();

        Some(node)
    } else {
        None
    }
}

fn init_loop() -> Node<PNCounter> {
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

    // Declare periodic task
    let task = Task {
        millisec: 10,
        callback: replicate_set,
    };
    let arc_node_task = Arc::clone(&arc_node);
    let c_thread = thread::spawn(move || loop {
        (task.callback)(&arc_node_task);
        sleep(Duration::from_millis(task.millisec));
    });

    // Spawn a thread to handle messages
    let mut handle_vec: Vec<JoinHandle<()>> = vec![];
    while let Ok(input) = rx.recv() {
        let arc_node_clone = Arc::clone(&arc_node);
        let handler_thread = thread::spawn(move || match read_msg(input) {
            Ok(msg) => {
                if let Err(e) = arc_node_clone.handle_msg(msg) {
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
    c_thread.join().expect("Callback thread panicked");
}
