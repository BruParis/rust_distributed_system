use serde::Serialize;
use std::io::{self, Write};
use std::sync::Mutex;

static STDOUT_MUTEX: Mutex<()> = Mutex::new(());
static STDERR_MUTEX: Mutex<()> = Mutex::new(());

pub fn to_stderr<M>(msg: &M)
where
    M: Serialize,
{
    let _err_lock = STDERR_MUTEX.lock().unwrap();
    let mut stderr = io::stderr();
    serde_json::to_writer(&stderr, &msg).unwrap();
    stderr.write_all(b"\n").unwrap();
    stderr.flush().unwrap();
}

pub fn to_stdout<M>(msg: &M)
where
    M: Serialize,
{
    to_stderr(msg);
    let _out_lock = STDOUT_MUTEX.lock().unwrap();
    let mut stdout = io::stdout();
    if let Err(e) = serde_json::to_writer(&stdout, &msg) {
        eprintln!("Error serializing to stdout: {}", e);
    }

    if let Err(e) = stdout.write_all(b"\n") {
        eprintln!("Error writing buffer to stdout: {}", e);
    }
    if let Err(e) = stdout.flush() {
        eprintln!("Error flushing sdout: {}", e);
    }
}
