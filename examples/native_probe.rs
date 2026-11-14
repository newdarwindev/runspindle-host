//! Opt-in, metadata-only native observation. Run inside an outbound-denied sandbox.
use runspindle_host::native::StdioSession;
use std::path::Path;
fn main() {
    let args: Vec<String> = std::env::args().collect();
    assert_eq!(args.len(), 3, "expected binary and fresh private directory");
    let mut peer =
        StdioSession::spawn(Path::new(&args[1]), Path::new(&args[2])).expect("pinned native spawn");
    peer.initialize().expect("native handshake");
    let thread = peer.start_empty_thread().expect("empty thread creation");
    // Do not print the native ID or peer content: record only equality/length evidence.
    let result = peer.read_thread(&thread);
    println!("{{\"handshake\":true,\"thread_created\":true,\"thread_id_bytes\":{},\"same_session_readback\":{},\"turn_submission\":false}}", thread.len(), result.as_ref().is_ok_and(|v| v.thread_id == thread));
    result.expect("same-session readback");
}
