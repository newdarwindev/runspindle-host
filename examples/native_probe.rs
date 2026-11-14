//! Opt-in metadata-only native/SQLite bridge observation, under outbound denial.
use runspindle_host::{
    lifecycle::*,
    native::StdioSession,
    native_bridge::{LocalPin, NativeBridge},
    storage::Sqlite,
};
use rusqlite::Connection;
use std::{
    path::{Path, PathBuf},
    process::Command,
};
fn binding() -> Binding {
    Binding {
        task_id: "probe-task".into(),
        revision: 1,
        operation_id: "probe-operation".into(),
        policy_revision: "probe-policy".into(),
        host_id: "probe-host".into(),
        workspace_id: "probe-private-workspace".into(),
        lease_epoch: 1,
    }
}
struct Grant;
impl Verifier for Grant {
    fn verify(&self, b: &Binding, _: Action) -> bool {
        b == &binding()
    }
}
struct Cleanup(PathBuf);
impl Drop for Cleanup {
    fn drop(&mut self) {
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{}{suffix}", self.0.display()));
        }
    }
}
fn inspect(path: &str) {
    let db = Connection::open(path).expect("independent SQLite reader");
    let native: Vec<u8> = db
        .query_row(
            "SELECT state FROM host_native_state WHERE operation='probe-operation'",
            [],
            |r| r.get(0),
        )
        .expect("native journal");
    let host: Vec<u8> = db
        .query_row("SELECT state FROM host_state WHERE id=1", [], |r| r.get(0))
        .expect("host journal");
    let native: serde_json::Value = serde_json::from_slice(&native).unwrap();
    let host: serde_json::Value = serde_json::from_slice(&host).unwrap();
    let snapshot = &host["entries"]["probe-operation"];
    assert_eq!(native["binding"], serde_json::to_value(binding()).unwrap());
    assert_eq!(snapshot["binding"], native["binding"]);
    assert!(native["thread"].as_str().is_some_and(|s| !s.is_empty()));
    assert_eq!(snapshot["native_session_id"], native["thread"]);
    assert_eq!(snapshot["execution"], "Acknowledged");
    assert_eq!(native["terminal"], serde_json::Value::Null);
    println!("{{\"independent_process_sqlite_binding\":true,\"task_execution\":false}}");
}
fn main() {
    let args: Vec<String> = std::env::args().collect();
    assert_eq!(
        args.len(),
        3,
        "expected pinned binary and fresh private directory"
    );
    if args[1] == "--inspect" {
        inspect(&args[2]);
        return;
    }
    let directory = Path::new(&args[2]);
    let journal = directory.with_extension("sqlite");
    assert!(!journal.exists(), "fresh private journal required");
    let cleanup = Cleanup(journal.clone());
    let mut bridge = NativeBridge::open(
        &journal,
        LocalPin::new("probe-host", "probe-private-workspace").unwrap(),
        binding(),
        || {
            let mut io = StdioSession::spawn(Path::new(&args[1]), directory)?;
            io.initialize()?;
            Ok(io)
        },
        || 100,
    )
    .expect("pinned native bridge");
    let mut adapter =
        Adapter::with_storage(Grant, Sqlite::open(&journal).unwrap(), 8, 32, 100).unwrap();
    let presence = Presence {
        host_id: "probe-host".into(),
        workspace_id: "probe-private-workspace".into(),
        reachability: Reachability::Online,
        observed_at_ms: 100,
        valid_until_ms: 200,
    };
    let prepared = adapter
        .submit(
            Request {
                version: VERSION.into(),
                binding: binding(),
            },
            &presence,
            100,
            &mut bridge,
        )
        .expect("session preparation");
    assert_eq!(prepared.execution, Execution::Acknowledged);
    let thread = bridge.identities().unwrap().unwrap().thread_id;
    assert_eq!(prepared.native_session_id.as_deref(), Some(thread.as_str()));
    let readback = adapter
        .reconcile(&binding(), 100, &mut bridge)
        .expect("actual same-session readback");
    assert_eq!(readback.native_session_id.as_deref(), Some(thread.as_str()));
    let independent = Command::new(std::env::current_exe().unwrap())
        .arg("--inspect")
        .arg(&journal)
        .env_clear()
        .output()
        .expect("fresh journal reader process");
    assert!(
        independent.status.success(),
        "independent store readback failed"
    );
    println!("{{\"handshake\":true,\"thread_created\":true,\"thread_id_bytes\":{},\"same_session_readback\":true,\"independent_process_sqlite_binding\":true,\"turn_submission\":false,\"task_execution\":false}}",thread.len());
    drop(adapter);
    drop(bridge);
    drop(cleanup);
    assert!(!directory.exists());
    assert!(!journal.exists());
}
