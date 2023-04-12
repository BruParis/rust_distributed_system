use std::io::{self};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::Arc;
use std::thread::{self, JoinHandle};

use echo_server::broadcast::msg::{Message, ReqPayload};
use echo_server::broadcast::node::Node;

fn read_msg(input: String) -> Result<Message<ReqPayload>, serde_json::Error> {
    eprintln!("Read msg: {}", input);

    // Parse the line as a JSON object
    let res_msg = serde_json::from_str(&input)?;

    Ok(res_msg)
}

fn check_init(req: &Message<ReqPayload>) -> Option<String> {
    if let ReqPayload::Init(init_p) = &req.body.payload {
        Some(init_p.node_id.clone())
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
            Ok(mut msg) => {
                if let Some(node_id) = check_init(&msg) {
                    let node = Node::new(node_id);

                    if let Err(e) = node.handle_msg(&mut msg) {
                        eprintln!("Error handling message: {:?}", e);
                    }

                    return node;
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
            Ok(mut msg) => {
                if let Err(e) = arc_node_clone.handle_msg(&mut msg) {
                    eprintln!("Error handling message: {:?}", e);
                }
            }
            Err(e) => eprintln!("Error parsing message: {}", e),
        });
        handle_vec.push(handler_thread);
    }

    // Join the threads
    input_thread.join().expect("Input thread panicked");
    //retry_thread.join().expect("Retry thread panicked");
    handle_vec.into_iter().for_each(|t_handle| {
        t_handle.join().expect("Handler thread panicked");
    });

    arc_node.kill_retry();
}
