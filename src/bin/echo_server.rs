use std::io::{self, BufRead};

use echo_server::echo::msg::{Message, Payload};
use echo_server::echo::node::Node;
use echo_server::output::{to_stderr, to_stdout};

fn read_msg(input: String) -> Result<Message, serde_json::Error> {
    // Parse the line as a JSON object
    let res_msg = serde_json::from_str(&input)?;
    to_stderr(&res_msg);

    Ok(res_msg)
}

fn check_init(req: &Message) -> Option<String> {
    if let Payload::Init(init_p) = &req.body.payload {
        Some(init_p.get())
    } else {
        None
    }
}

fn init_loop(stdin_lock: &mut io::StdinLock) -> Node {
    // Loop endlessly until init
    loop {
        // Read a line from stdin
        let mut input = String::new();

        if let Err(e) = stdin_lock.read_line(&mut input) {
            eprintln!("Error reading from stdin: {}", e);
            continue;
        }

        match read_msg(input) {
            Ok(msg) => {
                if let Some(node_id) = check_init(&msg) {
                    let mut node = Node::new(node_id.clone());

                    if let Ok(res_msg) = node.handle_msg(&msg) {
                        to_stdout(&res_msg);
                    }

                    return node;
                }
            }
            Err(e) => eprintln!("Error parsing message: {}", e),
        }
    }
}

fn main_loop(node: &mut Node, stdin_lock: &mut io::StdinLock) {
    // Loop endlessly until init
    loop {
        // Read a line from stdin
        let mut input = String::new();

        if let Err(e) = stdin_lock.read_line(&mut input) {
            eprintln!("Error reading from stdin: {}", e);
            continue;
        }

        match read_msg(input) {
            Ok(msg) => {
                if let Ok(res_msg) = node.handle_msg(&msg) {
                    to_stdout(&res_msg);
                }
            }
            Err(e) => eprintln!("Error parsing message: {}", e),
        }
    }
}

fn main() {
    // Prepare stdin and stderr handles
    let stdin = io::stdin();
    let mut stdin_lock = stdin.lock();

    let mut node = init_loop(&mut stdin_lock);

    main_loop(&mut node, &mut stdin_lock);
}
