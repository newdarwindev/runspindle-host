//! Independent durable peer ledger and fresh-process tests. No native app-server.
use runspindle_host::{lifecycle::*, storage::Sqlite};
use rusqlite::{params, Connection};
use std::{
    path::{Path, PathBuf},
    process::Command,
    sync::atomic::{AtomicU64, Ordering},
};
static ID: AtomicU64 = AtomicU64::new(0);
struct Temporary(PathBuf);
impl Temporary {
    fn new() -> Self {
        let p = std::env::temp_dir().join(format!(
            "host-durable-{}-{}",
            std::process::id(),
            ID.fetch_add(1, Ordering::SeqCst)
        ));
        std::fs::create_dir(&p).unwrap();
        Self(p)
    }
}
impl Drop for Temporary {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}
fn binding() -> Binding {
    Binding {
        task_id: "task".into(),
        revision: 1,
        operation_id: "operation".into(),
        policy_revision: "policy".into(),
        host_id: "host".into(),
        workspace_id: "workspace".into(),
        lease_epoch: 1,
    }
}
struct Grant;
impl Verifier for Grant {
    fn verify(&self, b: &Binding, _: Action) -> bool {
        b == &binding()
    }
}
fn presence() -> Presence {
    Presence {
        host_id: "host".into(),
        workspace_id: "workspace".into(),
        reachability: Reachability::Online,
        observed_at_ms: 100,
        valid_until_ms: 200,
    }
}
fn observation() -> Observation {
    Observation {
        binding: binding(),
        native_session_id: "peer-observed-session".into(),
        sequence: 1,
        observed_at_ms: 110,
        terminal: None,
    }
}
struct Ledger {
    db: Connection,
    crash: bool,
    cancel_crash: bool,
}
impl Ledger {
    fn open(p: &Path) -> Self {
        let db = Connection::open(p).unwrap();
        db.busy_timeout(std::time::Duration::from_secs(5)).unwrap();
        db.execute_batch("PRAGMA journal_mode=WAL;PRAGMA synchronous=FULL;CREATE TABLE IF NOT EXISTS calls(id INTEGER PRIMARY KEY,action TEXT);CREATE TABLE IF NOT EXISTS effects(operation TEXT PRIMARY KEY,session TEXT);").unwrap();
        Self {
            db,
            crash: false,
            cancel_crash: false,
        }
    }
    fn call(&self, a: &str) {
        self.db
            .execute("INSERT INTO calls(action) VALUES(?1)", [a])
            .unwrap();
    }
}
impl Peer for Ledger {
    fn launch(&mut self, b: &Binding) -> LaunchReply {
        self.call("launch");
        self.db
            .execute(
                "INSERT INTO effects VALUES(?1,'peer-observed-session')",
                [&b.operation_id],
            )
            .unwrap();
        if self.crash {
            std::process::exit(93)
        }
        LaunchReply::LostReply
    }
    fn discover(&mut self, _: &Binding) -> Vec<Observation> {
        self.call("discover");
        if self
            .db
            .query_row("SELECT count(*) FROM effects", [], |r| r.get::<_, u64>(0))
            .unwrap()
            == 1
        {
            vec![observation()]
        } else {
            vec![]
        }
    }
    fn cancel(&mut self, _: &Binding, s: &str) -> CancelReply {
        assert_eq!(s, "peer-observed-session");
        self.call("cancel");
        if self.cancel_crash {
            std::process::exit(94)
        }
        CancelReply::LostReply
    }
}
fn adapter(p: &Path) -> Adapter<Grant, Sqlite> {
    Adapter::with_storage(Grant, Sqlite::open(p).unwrap(), 8, 32, 50).unwrap()
}
fn run_child(dir: &Path, mode: &str) -> std::process::Child {
    Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "child", "--nocapture"])
        .env("HOST_TEST_DIR", dir)
        .env("HOST_TEST_MODE", mode)
        .spawn()
        .unwrap()
}
#[test]
fn child() {
    let Ok(dir) = std::env::var("HOST_TEST_DIR") else {
        return;
    };
    let dir = PathBuf::from(dir);
    let mode = std::env::var("HOST_TEST_MODE").unwrap();
    let mut a = adapter(&dir.join("host.sqlite"));
    let mut peer = Ledger::open(&dir.join("peer.sqlite"));
    if mode == "launch_crash" {
        peer.crash = true;
    }
    if mode == "cancel_crash" {
        peer.cancel_crash = true;
        a.cancel(&binding(), &mut peer).unwrap();
        return;
    }
    if mode == "reconcile" {
        a.reconcile(&binding(), 120, &mut peer).unwrap();
        return;
    }
    let result = a.submit(
        Request {
            version: VERSION.into(),
            binding: binding(),
        },
        &presence(),
        120,
        &mut peer,
    );
    assert!(result.is_ok() || result == Err(Error::Conflict));
}
#[test]
fn dispatch_crash_recovers_in_fresh_process_without_second_launch() {
    let t = Temporary::new();
    assert_eq!(
        run_child(&t.0, "launch_crash").wait().unwrap().code(),
        Some(93)
    );
    assert!(run_child(&t.0, "submit").wait().unwrap().success());
    assert!(run_child(&t.0, "reconcile").wait().unwrap().success());
    let a = adapter(&t.0.join("host.sqlite"));
    assert_eq!(
        a.snapshot(&binding()).unwrap().native_session_id.as_deref(),
        Some("peer-observed-session")
    );
    let p = Ledger::open(&t.0.join("peer.sqlite"));
    assert_eq!(
        p.db.query_row("SELECT group_concat(action,',') FROM calls", [], |r| r
            .get::<_, String>(
            0
        ))
        .unwrap(),
        "launch,discover"
    );
}
#[test]
fn concurrent_process_owners_launch_once() {
    let t = Temporary::new();
    drop(adapter(&t.0.join("host.sqlite")));
    drop(Ledger::open(&t.0.join("peer.sqlite")));
    let mut children = (0..8)
        .map(|_| run_child(&t.0, "submit"))
        .collect::<Vec<_>>();
    for c in &mut children {
        assert!(c.wait().unwrap().success());
    }
    let p = Ledger::open(&t.0.join("peer.sqlite"));
    assert_eq!(
        p.db.query_row(
            "SELECT count(*) FROM calls WHERE action='launch'",
            [],
            |r| r.get::<_, u64>(0)
        )
        .unwrap(),
        1
    );
}
#[test]
fn cancel_crash_never_resends_and_preserves_pending_receipts() {
    let t = Temporary::new();
    assert!(run_child(&t.0, "submit").wait().unwrap().success());
    assert!(run_child(&t.0, "reconcile").wait().unwrap().success());
    assert_eq!(
        run_child(&t.0, "cancel_crash").wait().unwrap().code(),
        Some(94)
    );
    let mut a = adapter(&t.0.join("host.sqlite"));
    let mut p = Ledger::open(&t.0.join("peer.sqlite"));
    let s = a.cancel(&binding(), &mut p).unwrap();
    assert_eq!(s.cancellation, Cancellation::Requested);
    assert!(s.cancel_dispatched);
    assert!(!a.try_drain().unwrap().is_empty());
    drop(a);
    assert!(adapter(&t.0.join("host.sqlite"))
        .try_drain()
        .unwrap()
        .is_empty());
    assert_eq!(
        p.db.query_row(
            "SELECT count(*) FROM calls WHERE action='cancel'",
            [],
            |r| r.get::<_, u64>(0)
        )
        .unwrap(),
        1
    );
}
#[test]
fn stale_owner_cannot_overwrite_reconciliation() {
    use runspindle_host::storage::{Storage, Stored};
    let t = Temporary::new();
    let mut a = Sqlite::open(t.0.join("host.sqlite")).unwrap();
    let mut b = Sqlite::open(t.0.join("host.sqlite")).unwrap();
    let (g, s) = a.load().unwrap();
    b.compare_and_swap(g, &s).unwrap();
    assert_eq!(
        a.compare_and_swap(g, &Stored::default()),
        Err(Error::Conflict)
    );
}
#[test]
fn changed_binding_and_configuration_conflict_after_reopen() {
    let t = Temporary::new();
    let mut a = adapter(&t.0.join("host.sqlite"));
    let mut p = Ledger::open(&t.0.join("peer.sqlite"));
    a.submit(
        Request {
            version: VERSION.into(),
            binding: binding(),
        },
        &presence(),
        120,
        &mut p,
    )
    .unwrap();
    drop(a);
    let a = adapter(&t.0.join("host.sqlite"));
    let mut b = binding();
    b.lease_epoch += 1;
    assert_eq!(a.snapshot(&b), Err(Error::Conflict));
    assert!(matches!(
        Adapter::with_storage(
            Grant,
            Sqlite::open(t.0.join("host.sqlite")).unwrap(),
            9,
            32,
            50
        ),
        Err(Error::Conflict)
    ));
}

#[test]
fn sqlite_reopen_preserves_session_sequence_cancellation_and_terminal() {
    let t = Temporary::new();
    let path = t.0.join("host.sqlite");
    let mut peer = Ledger::open(&t.0.join("peer.sqlite"));
    let mut a = adapter(&path);
    a.submit(
        Request {
            version: VERSION.into(),
            binding: binding(),
        },
        &presence(),
        120,
        &mut peer,
    )
    .unwrap();
    a.reconcile(&binding(), 120, &mut peer).unwrap();
    a.cancel(&binding(), &mut peer).unwrap();
    drop(a);
    let mut a = adapter(&path);
    for kind in 0..4 {
        let mut o = observation();
        o.sequence = 2;
        o.observed_at_ms = 120;
        match kind {
            0 => o.native_session_id = "rebound".into(),
            1 => o.sequence = 1,
            2 => o.observed_at_ms = 121,
            _ => o.binding.lease_epoch = 2,
        }
        assert!(a
            .event(
                &binding(),
                Event {
                    version: VERSION.into(),
                    observation: o,
                    kind: EventKind::Progress,
                    encoded_size: 256
                },
                120
            )
            .is_err());
        assert_eq!(
            a.snapshot(&binding()).unwrap().native_session_id.as_deref(),
            Some("peer-observed-session")
        );
    }
    let mut o = observation();
    o.sequence = 2;
    o.observed_at_ms = 120;
    o.terminal = Some(Terminal::Completed);
    let event = Event {
        version: VERSION.into(),
        observation: o,
        kind: EventKind::Terminal(Terminal::Completed),
        encoded_size: 256,
    };
    a.event(&binding(), event.clone(), 120).unwrap();
    drop(a);
    let mut a = adapter(&path);
    assert_eq!(
        a.event(&binding(), event, 120).unwrap().execution,
        Execution::Terminal(Terminal::Completed)
    );
    assert_eq!(
        a.snapshot(&binding()).unwrap().cancellation,
        Cancellation::ObservedTerminal
    );
    assert!(a
        .event(
            &binding(),
            Event {
                version: VERSION.into(),
                observation: observation(),
                kind: EventKind::Progress,
                encoded_size: 256
            },
            120
        )
        .is_err());
}
#[test]
fn durable_predispatch_cancel_has_no_native_terminal_or_peer_effect() {
    let t = Temporary::new();
    let path = t.0.join("host.sqlite");
    let mut a = adapter(&path);
    let mut p = Ledger::open(&t.0.join("peer.sqlite"));
    let mut offline = presence();
    offline.reachability = Reachability::Offline;
    a.submit(
        Request {
            version: VERSION.into(),
            binding: binding(),
        },
        &offline,
        120,
        &mut p,
    )
    .unwrap();
    a.cancel(&binding(), &mut p).unwrap();
    drop(a);
    let mut a = adapter(&path);
    let s = a.dispatch(&binding(), &presence(), 120, &mut p).unwrap();
    assert_eq!(s.execution, Execution::CancelledBeforeDispatch);
    assert_eq!(s.cancellation, Cancellation::Requested);
    assert!(s.native_session_id.is_none());
    assert_eq!(
        p.db.query_row("SELECT count(*) FROM calls", params![], |r| r
            .get::<_, u64>(0))
            .unwrap(),
        0
    );
}
