//! Scripted native boundary; real SQLite stores and independent child/effect ledger.
use runspindle_host::{
    lifecycle::*,
    native::{NativeIds, NativeNotice, WireError},
    native_bridge::{LocalPin, NativeBridge, NativeIo},
    storage::Sqlite,
};
use rusqlite::{params, Connection};
use std::{
    collections::VecDeque,
    path::{Path, PathBuf},
    process::Command,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
};
static SERIAL: AtomicU64 = AtomicU64::new(0);
const THREAD: &str = "native-thread";
const TURN: &str = "native-turn";
const ITEM: &str = "native-item";
struct Temp(PathBuf);
impl Temp {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "host-native-bridge-{}-{}",
            std::process::id(),
            SERIAL.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for Temp {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}
fn pin() -> LocalPin {
    LocalPin::new("host", "workspace").unwrap()
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
// Explicit synthetic ownership authority for negative binding tests only.
struct OwnershipGrant;
impl Verifier for OwnershipGrant {
    fn verify(&self, _: &Binding, action: Action) -> bool {
        action == Action::TakeOwnership
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
fn request() -> Request {
    Request {
        version: VERSION.into(),
        binding: binding(),
    }
}
fn adapter(path: &Path, receipts: usize) -> Adapter<Grant, Sqlite> {
    Adapter::with_storage(Grant, Sqlite::open(path).unwrap(), 8, receipts, 100).unwrap()
}
fn turn() -> NativeNotice {
    NativeNotice::TurnStarted {
        thread_id: THREAD.into(),
        turn_id: TURN.into(),
    }
}
fn item() -> NativeNotice {
    NativeNotice::ItemStarted {
        thread_id: THREAD.into(),
        turn_id: TURN.into(),
        item_id: ITEM.into(),
    }
}
fn item_done() -> NativeNotice {
    NativeNotice::ItemCompleted {
        thread_id: THREAD.into(),
        turn_id: TURN.into(),
        item_id: ITEM.into(),
    }
}
fn done(terminal: Terminal) -> NativeNotice {
    NativeNotice::TurnCompleted {
        thread_id: THREAD.into(),
        turn_id: TURN.into(),
        terminal,
    }
}
type Events = Arc<Mutex<VecDeque<NativeNotice>>>;
struct Scripted {
    db: Connection,
    events: Events,
    crash_start: bool,
    crash_notice: bool,
    reject_read: bool,
}
impl Scripted {
    fn open(path: &Path, events: Events) -> Self {
        let db = Connection::open(path).unwrap();
        db.busy_timeout(std::time::Duration::from_secs(5)).unwrap();
        db.execute_batch("PRAGMA journal_mode=WAL;PRAGMA synchronous=FULL;CREATE TABLE IF NOT EXISTS calls(id INTEGER PRIMARY KEY,action TEXT,thread TEXT,turn TEXT);CREATE TABLE IF NOT EXISTS effects(operation TEXT PRIMARY KEY,thread TEXT);").unwrap();
        Self {
            db,
            events,
            crash_start: false,
            crash_notice: false,
            reject_read: false,
        }
    }
    fn call(&self, action: &str, thread: &str, turn: &str) {
        self.db
            .execute(
                "INSERT INTO calls(action,thread,turn) VALUES(?1,?2,?3)",
                params![action, thread, turn],
            )
            .unwrap();
    }
}
impl NativeIo for Scripted {
    fn start_empty_thread(&mut self) -> Result<String, WireError> {
        self.call("start", THREAD, "");
        self.db
            .execute("INSERT INTO effects VALUES('operation',?1)", [THREAD])
            .unwrap();
        if self.crash_start {
            std::process::exit(93)
        }
        Ok(THREAD.into())
    }
    fn read_thread(&mut self, thread: &str) -> Result<NativeIds, WireError> {
        self.call("read", thread, "");
        if self.reject_read {
            return Err(WireError::Rejected);
        }
        assert_eq!(thread, THREAD);
        Ok(NativeIds {
            thread_id: thread.into(),
            turn_id: None,
            item_id: None,
        })
    }
    fn next_notice(&mut self, thread: &str) -> Option<Result<NativeNotice, WireError>> {
        assert_eq!(thread, THREAD);
        let notice = self.events.lock().unwrap().pop_front()?;
        self.call("notice", thread, "");
        if self.crash_notice {
            std::process::exit(96)
        }
        Some(Ok(notice))
    }
}
fn count(path: &Path, action: &str) -> u64 {
    Connection::open(path)
        .unwrap()
        .query_row(
            "SELECT count(*) FROM calls WHERE action=?1",
            [action],
            |r| r.get(0),
        )
        .unwrap()
}
fn peer(dir: &Path, events: Events) -> NativeBridge<Scripted, impl Fn() -> u64> {
    NativeBridge::open(
        dir.join("host.sqlite"),
        pin(),
        binding(),
        &Grant,
        || Ok(Scripted::open(&dir.join("peer.sqlite"), events)),
        || 120,
    )
    .unwrap()
}
fn emit(
    bridge: &mut NativeBridge<Scripted, impl Fn() -> u64>,
    adapter: &mut Adapter<Grant, Sqlite>,
    events: &Events,
    notice: NativeNotice,
) {
    events.lock().unwrap().push_back(notice);
    assert!(bridge.poll().unwrap());
    assert!(bridge.deliver(adapter, 120).unwrap());
}
struct CrashAfterBound<'a, P: Peer>(&'a mut P);
impl<P: Peer> Peer for CrashAfterBound<'_, P> {
    fn launch(&mut self, b: &Binding) -> LaunchReply {
        let result = self.0.launch(b);
        assert!(matches!(result, LaunchReply::Accepted(_)));
        std::process::exit(94)
    }
    fn discover(&mut self, b: &Binding) -> Vec<Observation> {
        self.0.discover(b)
    }
    fn cancel(&mut self, b: &Binding, s: &str) -> CancelReply {
        self.0.cancel(b, s)
    }
}
fn child(dir: &Path, mode: &str) -> std::process::Child {
    Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "native_bridge_child", "--nocapture"])
        .env("NATIVE_BRIDGE_TEST_DIR", dir)
        .env("NATIVE_BRIDGE_TEST_MODE", mode)
        .spawn()
        .unwrap()
}
#[test]
fn native_bridge_child() {
    let Ok(dir) = std::env::var("NATIVE_BRIDGE_TEST_DIR") else {
        return;
    };
    let dir = PathBuf::from(dir);
    let mode = std::env::var("NATIVE_BRIDGE_TEST_MODE").unwrap();
    let events = Events::default();
    let mut io = Scripted::open(&dir.join("peer.sqlite"), events.clone());
    io.crash_start = mode == "start_crash";
    io.crash_notice = mode == "notice_crash";
    let mut bridge = NativeBridge::open(
        dir.join("host.sqlite"),
        pin(),
        binding(),
        &Grant,
        || {
            if mode == "owner_factory" {
                std::fs::write(dir.join("factory-held"), "ready").unwrap();
                let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
                while !dir.join("factory-release").exists() {
                    assert!(std::time::Instant::now() < deadline);
                    std::thread::sleep(std::time::Duration::from_millis(5));
                }
            }
            Ok(io)
        },
        || 120,
    )
    .unwrap();
    if mode == "owner_factory" {
        return;
    }
    let mut a = adapter(&dir.join("host.sqlite"), 32);
    match mode.as_str() {
        "ack_crash" => {
            a.submit(
                request(),
                &presence(),
                120,
                &mut CrashAfterBound(&mut bridge),
            )
            .unwrap();
        }
        "unknown" => {
            a.submit(request(), &presence(), 120, &mut bridge).unwrap();
            assert_eq!(
                a.reconcile(&binding(), 120, &mut bridge),
                Err(Error::Unknown)
            );
            assert_eq!(
                a.snapshot(&binding()).unwrap().execution,
                Execution::Unknown
            );
        }
        "recover" => {
            a.submit(request(), &presence(), 120, &mut bridge).unwrap();
            let value = a.reconcile(&binding(), 120, &mut bridge).unwrap();
            assert_eq!(value.native_session_id.as_deref(), Some(THREAD));
            assert_eq!(value.execution, Execution::Acknowledged);
        }
        "turn_item" => {
            emit(&mut bridge, &mut a, &events, turn());
            emit(&mut bridge, &mut a, &events, item());
        }
        "finish" => {
            assert_eq!(
                bridge.identities().unwrap().unwrap().turn_id.as_deref(),
                Some(TURN)
            );
            assert_eq!(bridge.item_completed(ITEM).unwrap(), Some(false));
            emit(&mut bridge, &mut a, &events, item_done());
            emit(&mut bridge, &mut a, &events, done(Terminal::Completed));
            assert_eq!(bridge.turn_outcome().unwrap(), Some(Terminal::Completed));
            assert_eq!(
                a.snapshot(&binding()).unwrap().execution,
                Execution::Acknowledged
            );
        }
        "cancel" => {
            a.cancel(&binding(), &mut bridge).unwrap();
        }
        "notice_crash" => {
            events.lock().unwrap().push_back(done(Terminal::Completed));
            bridge.poll().unwrap();
        }
        _ => {
            a.submit(request(), &presence(), 120, &mut bridge).unwrap();
        }
    }
}
#[test]
fn native_bridge_fresh_processes_preserve_prepared_session_turn_and_items_without_task_completion()
{
    let t = Temp::new();
    for mode in ["start", "recover", "turn_item", "finish"] {
        assert!(child(&t.0, mode).wait().unwrap().success());
    }
    let a = adapter(&t.0.join("host.sqlite"), 32);
    assert_eq!(
        a.snapshot(&binding()).unwrap().execution,
        Execution::Acknowledged
    );
    assert_eq!(count(&t.0.join("peer.sqlite"), "start"), 1);
    assert_eq!(count(&t.0.join("peer.sqlite"), "read"), 1);
    let bridge = peer(&t.0, Events::default());
    assert_eq!(bridge.items().unwrap().get(ITEM), Some(&true));
    let db = Connection::open(t.0.join("host.sqlite")).unwrap();
    let bytes: Vec<u8> = db
        .query_row(
            "SELECT state FROM host_native_state WHERE operation='operation'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    let state: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(state["binding"], serde_json::to_value(binding()).unwrap());
    assert_eq!(state["thread"], THREAD);
    assert_eq!(state["turn"], TURN);
}
#[test]
fn native_bridge_uncertain_start_never_relaunches_in_fresh_process_twice() {
    for _ in 0..2 {
        let t = Temp::new();
        assert_eq!(child(&t.0, "start_crash").wait().unwrap().code(), Some(93));
        for _ in 0..2 {
            assert!(child(&t.0, "unknown").wait().unwrap().success());
        }
        assert_eq!(count(&t.0.join("peer.sqlite"), "start"), 1);
        assert_eq!(count(&t.0.join("peer.sqlite"), "read"), 0);
        assert_eq!(
            Connection::open(t.0.join("peer.sqlite"))
                .unwrap()
                .query_row("SELECT count(*) FROM effects", [], |r| r.get::<_, u64>(0))
                .unwrap(),
            1
        );
    }
}
#[test]
fn native_bridge_saved_native_reply_recovers_after_adapter_ack_loss_twice() {
    for _ in 0..2 {
        let t = Temp::new();
        assert_eq!(child(&t.0, "ack_crash").wait().unwrap().code(), Some(94));
        assert!(child(&t.0, "recover").wait().unwrap().success());
        assert_eq!(count(&t.0.join("peer.sqlite"), "start"), 1);
        assert_eq!(count(&t.0.join("peer.sqlite"), "read"), 1);
    }
}
#[test]
fn native_bridge_wrong_binding_and_rejected_ephemeral_read_never_relaunch() {
    let t = Temp::new();
    let events = Events::default();
    let mut bridge = peer(&t.0, events);
    let mut a = adapter(&t.0.join("host.sqlite"), 32);
    a.submit(request(), &presence(), 120, &mut bridge).unwrap();
    drop(bridge);
    let mut changed = binding();
    changed.lease_epoch += 1;
    assert!(matches!(
        NativeBridge::open(
            t.0.join("host.sqlite"),
            pin(),
            changed,
            &OwnershipGrant,
            || Ok(Scripted::open(&t.0.join("peer.sqlite"), Events::default())),
            || 120
        ),
        Err(Error::Conflict)
    ));
    let mut io = Scripted::open(&t.0.join("peer.sqlite"), Events::default());
    io.reject_read = true;
    let mut bridge = NativeBridge::open(
        t.0.join("host.sqlite"),
        pin(),
        binding(),
        &Grant,
        || Ok(io),
        || 120,
    )
    .unwrap();
    assert_eq!(
        a.reconcile(&binding(), 120, &mut bridge),
        Err(Error::Unknown)
    );
    assert_eq!(
        a.snapshot(&binding()).unwrap().native_session_id.as_deref(),
        Some(THREAD)
    );
    a.submit(request(), &presence(), 120, &mut bridge).unwrap();
    assert_eq!(count(&t.0.join("peer.sqlite"), "start"), 1);
}
#[test]
fn native_bridge_foreign_reordered_duplicate_and_terminal_metadata_are_fenced() {
    let t = Temp::new();
    let events = Events::default();
    let mut bridge = peer(&t.0, events.clone());
    let mut a = adapter(&t.0.join("host.sqlite"), 32);
    a.submit(request(), &presence(), 120, &mut bridge).unwrap();
    for (notice, error) in [
        (item(), Error::Conflict),
        (done(Terminal::Completed), Error::Conflict),
        (
            NativeNotice::TurnStarted {
                thread_id: "foreign".into(),
                turn_id: TURN.into(),
            },
            Error::WrongBinding,
        ),
    ] {
        events.lock().unwrap().push_back(notice);
        assert_eq!(bridge.poll(), Err(error));
    }
    emit(&mut bridge, &mut a, &events, turn());
    events.lock().unwrap().push_back(turn());
    assert!(!bridge.poll().unwrap());
    emit(&mut bridge, &mut a, &events, item());
    events.lock().unwrap().push_back(item());
    assert!(!bridge.poll().unwrap());
    emit(&mut bridge, &mut a, &events, item_done());
    events.lock().unwrap().push_back(item());
    assert_eq!(bridge.poll(), Err(Error::Stale));
    let cancelled = a.cancel(&binding(), &mut bridge).unwrap();
    assert_eq!(cancelled.cancellation, Cancellation::Requested);
    assert_eq!(cancelled.execution, Execution::Acknowledged);
    emit(&mut bridge, &mut a, &events, done(Terminal::Completed));
    assert_eq!(bridge.turn_outcome().unwrap(), Some(Terminal::Completed));
    assert_eq!(
        a.snapshot(&binding()).unwrap().execution,
        Execution::Acknowledged
    );
    assert_eq!(
        a.snapshot(&binding()).unwrap().cancellation,
        Cancellation::Requested
    );
    events.lock().unwrap().push_back(done(Terminal::Failed));
    assert_eq!(bridge.poll(), Err(Error::Conflict));
    assert_eq!(count(&t.0.join("peer.sqlite"), "interrupt"), 0);
}
#[test]
fn native_bridge_backpressure_retains_pending_observation_across_restart() {
    let t = Temp::new();
    let events = Events::default();
    let mut bridge = peer(&t.0, events.clone());
    let mut a = adapter(&t.0.join("host.sqlite"), 1);
    a.submit(request(), &presence(), 120, &mut bridge).unwrap();
    events.lock().unwrap().push_back(turn());
    assert!(bridge.poll().unwrap());
    assert_eq!(bridge.deliver(&mut a, 120), Err(Error::Backpressure));
    events.lock().unwrap().push_back(item());
    assert_eq!(bridge.poll(), Err(Error::Backpressure));
    assert_eq!(count(&t.0.join("peer.sqlite"), "notice"), 1);
    drop(bridge);
    a.try_drain().unwrap();
    let mut bridge = peer(&t.0, events);
    assert!(bridge.deliver(&mut a, 120).unwrap());
    a.try_drain().unwrap();
    assert!(bridge.poll().unwrap());
    assert!(bridge.deliver(&mut a, 120).unwrap());
    assert_eq!(bridge.item_completed(ITEM).unwrap(), Some(false));
}
#[test]
fn native_bridge_observed_turn_never_authorizes_interrupt_across_restart_twice() {
    for _ in 0..2 {
        let t = Temp::new();
        for mode in ["start", "turn_item"] {
            assert!(child(&t.0, mode).wait().unwrap().success());
        }
        assert!(child(&t.0, "cancel").wait().unwrap().success());
        let mut bridge = peer(&t.0, Events::default());
        let mut a = adapter(&t.0.join("host.sqlite"), 32);
        assert_eq!(
            a.cancel(&binding(), &mut bridge).unwrap().cancellation,
            Cancellation::Requested
        );
        assert_eq!(count(&t.0.join("peer.sqlite"), "interrupt"), 0);
    }
}
#[test]
fn native_bridge_receive_crash_fences_unknown_instead_of_claiming_replayed_terminal_twice() {
    for _ in 0..2 {
        let t = Temp::new();
        for mode in ["start", "turn_item"] {
            assert!(child(&t.0, mode).wait().unwrap().success());
        }
        assert_eq!(child(&t.0, "notice_crash").wait().unwrap().code(), Some(96));
        let mut bridge = peer(&t.0, Events::default());
        let mut a = adapter(&t.0.join("host.sqlite"), 32);
        assert_eq!(bridge.poll(), Err(Error::Unknown));
        assert_eq!(
            a.reconcile(&binding(), 120, &mut bridge),
            Err(Error::Unknown)
        );
        assert_eq!(bridge.turn_outcome().unwrap(), None);
        assert_eq!(
            a.snapshot(&binding()).unwrap().execution,
            Execution::Unknown
        );
        assert_eq!(count(&t.0.join("peer.sqlite"), "start"), 1);
    }
}
#[test]
fn native_bridge_failed_sqlite_save_retains_notice_without_second_native_read() {
    let t = Temp::new();
    let events = Events::default();
    let mut bridge = peer(&t.0, events.clone());
    let mut a = adapter(&t.0.join("host.sqlite"), 32);
    a.submit(request(), &presence(), 120, &mut bridge).unwrap();
    let db = Connection::open(t.0.join("host.sqlite")).unwrap();
    db.execute_batch("CREATE TRIGGER fail_native_save BEFORE UPDATE ON host_native_state WHEN CAST(new.state AS TEXT) LIKE '%\"receiving\":false%' AND CAST(old.state AS TEXT) LIKE '%\"receiving\":true%' BEGIN SELECT RAISE(ABORT,'scripted persistence failure');END;").unwrap();
    events.lock().unwrap().push_back(turn());
    assert_eq!(bridge.poll(), Err(Error::Storage));
    assert_eq!(count(&t.0.join("peer.sqlite"), "notice"), 1);
    db.execute_batch("DROP TRIGGER fail_native_save").unwrap();
    assert!(bridge.poll().unwrap());
    bridge.deliver(&mut a, 120).unwrap();
    assert_eq!(
        bridge.identities().unwrap().unwrap().turn_id.as_deref(),
        Some(TURN)
    );
    assert_eq!(count(&t.0.join("peer.sqlite"), "notice"), 1);
}

#[test]
fn native_bridge_configured_local_pins_precede_database_and_transport_factory() {
    for local in [
        LocalPin::new("other-host", "workspace").unwrap(),
        LocalPin::new("host", "other-workspace").unwrap(),
    ] {
        let t = Temp::new();
        let result = NativeBridge::open(
            t.0.join("host.sqlite"),
            local,
            binding(),
            &Grant,
            || Ok(Scripted::open(&t.0.join("peer.sqlite"), Events::default())),
            || 120,
        );
        assert!(matches!(result, Err(Error::WrongBinding)));
        assert!(!t.0.join("host.sqlite").exists());
        assert!(!t.0.join("peer.sqlite").exists());
    }
}
#[test]
fn native_bridge_every_reopened_binding_field_and_requested_operation_is_fenced() {
    let t = Temp::new();
    let mut bridge = peer(&t.0, Events::default());
    let mut a = adapter(&t.0.join("host.sqlite"), 32);
    a.submit(request(), &presence(), 120, &mut bridge).unwrap();
    assert_eq!(bridge.launch(&binding()), LaunchReply::LostReply);
    let mut changes = Vec::new();
    let mut b = binding();
    b.task_id = "other-task".into();
    changes.push(b);
    let mut b = binding();
    b.revision += 1;
    changes.push(b);
    let mut b = binding();
    b.policy_revision = "other-policy".into();
    changes.push(b);
    let mut b = binding();
    b.lease_epoch += 1;
    changes.push(b);
    for b in changes {
        let called = Arc::new(AtomicU64::new(0));
        let counter = called.clone();
        let result = NativeBridge::open(
            t.0.join("host.sqlite"),
            pin(),
            b.clone(),
            &OwnershipGrant,
            || {
                counter.fetch_add(1, Ordering::Relaxed);
                Ok(Scripted::open(&t.0.join("peer.sqlite"), Events::default()))
            },
            || 120,
        );
        assert!(matches!(result, Err(Error::Conflict)));
        assert_eq!(called.load(Ordering::Relaxed), 0);
        assert_eq!(bridge.launch(&b), LaunchReply::Rejected);
        assert!(bridge.discover(&b).is_empty());
        assert_eq!(bridge.cancel(&b, THREAD), CancelReply::LostReply);
    }
    let mut wrong_operation = binding();
    wrong_operation.operation_id = "another-operation".into();
    assert_eq!(bridge.launch(&wrong_operation), LaunchReply::Rejected);
    assert!(bridge.discover(&wrong_operation).is_empty());
    assert_eq!(count(&t.0.join("peer.sqlite"), "start"), 1);
    assert_eq!(count(&t.0.join("peer.sqlite"), "read"), 0);
    assert_eq!(count(&t.0.join("peer.sqlite"), "interrupt"), 0);
}

#[test]
fn native_bridge_takeover_requires_authority_before_database_or_factory() {
    let t = Temp::new();
    let calls = AtomicU64::new(0);
    let result = NativeBridge::open(
        t.0.join("host.sqlite"),
        pin(),
        binding(),
        &Deny,
        || {
            calls.fetch_add(1, Ordering::Relaxed);
            Ok(Scripted::open(&t.0.join("peer.sqlite"), Events::default()))
        },
        || 120,
    );
    assert!(matches!(result, Err(Error::Denied)));
    assert_eq!(calls.load(Ordering::Relaxed), 0);
    assert!(!t.0.join("host.sqlite").exists());
}

#[test]
fn native_bridge_stale_owner_cannot_launch_read_or_consume_after_takeover() {
    let t = Temp::new();
    let events = Events::default();
    let mut stale = peer(&t.0, events.clone());
    let mut current = peer(&t.0, events.clone());
    assert_eq!(stale.launch(&binding()), LaunchReply::LostReply);
    assert_eq!(count(&t.0.join("peer.sqlite"), "start"), 0);
    assert!(matches!(
        current.launch(&binding()),
        LaunchReply::Accepted(_)
    ));
    events.lock().unwrap().push_back(turn());
    assert!(stale.discover(&binding()).is_empty());
    assert_eq!(stale.poll(), Err(Error::Conflict));
    assert_eq!(count(&t.0.join("peer.sqlite"), "read"), 0);
    assert_eq!(count(&t.0.join("peer.sqlite"), "notice"), 0);
    assert_eq!(current.discover(&binding()).len(), 1);
    assert_eq!(current.poll(), Ok(true));
    assert_eq!(count(&t.0.join("peer.sqlite"), "start"), 1);
    assert_eq!(count(&t.0.join("peer.sqlite"), "read"), 1);
    assert_eq!(count(&t.0.join("peer.sqlite"), "notice"), 1);
}

#[test]
fn native_bridge_failed_factory_does_not_restore_old_owner() {
    let t = Temp::new();
    let mut stale = peer(&t.0, Events::default());
    assert!(matches!(stale.launch(&binding()), LaunchReply::Accepted(_)));
    let result = NativeBridge::<Scripted, _>::open(
        t.0.join("host.sqlite"),
        pin(),
        binding(),
        &Grant,
        || Err(WireError::Io),
        || 120,
    );
    assert!(matches!(result, Err(Error::Unknown)));
    assert!(stale.discover(&binding()).is_empty());
    assert_eq!(stale.poll(), Err(Error::Conflict));
    assert_eq!(stale.launch(&binding()), LaunchReply::LostReply);
    let mut current = peer(&t.0, Events::default());
    assert_eq!(current.launch(&binding()), LaunchReply::LostReply);
    assert_eq!(current.discover(&binding()).len(), 1);
    assert_eq!(count(&t.0.join("peer.sqlite"), "start"), 1);
    assert_eq!(count(&t.0.join("peer.sqlite"), "read"), 1);
}

// Independent SQLite writer probes prove there is no owner-check/native-call gap.
// A pre-call generation comparison alone would let this competing writer through.
fn assert_takeover_locked(path: &Path) {
    let db = Connection::open(path).unwrap();
    db.busy_timeout(std::time::Duration::ZERO).unwrap();
    let error = db.execute_batch("BEGIN IMMEDIATE").unwrap_err();
    assert_eq!(
        error.sqlite_error_code(),
        Some(rusqlite::ErrorCode::DatabaseBusy)
    );
}
struct LockedPeer {
    host: PathBuf,
    peer: Scripted,
}
impl NativeIo for LockedPeer {
    fn start_empty_thread(&mut self) -> Result<String, WireError> {
        assert_takeover_locked(&self.host);
        self.peer.start_empty_thread()
    }
    fn read_thread(&mut self, thread: &str) -> Result<NativeIds, WireError> {
        assert_takeover_locked(&self.host);
        self.peer.read_thread(thread)
    }
    fn next_notice(&mut self, thread: &str) -> Option<Result<NativeNotice, WireError>> {
        assert_takeover_locked(&self.host);
        self.peer.next_notice(thread)
    }
}
#[test]
fn native_bridge_factory_and_actual_io_hold_atomic_owner_fence() {
    let t = Temp::new();
    let host = t.0.join("host.sqlite");
    let effects = t.0.join("peer.sqlite");
    let events = Events::default();
    events.lock().unwrap().push_back(turn());
    let mut bridge = NativeBridge::open(
        &host,
        pin(),
        binding(),
        &Grant,
        || {
            assert_takeover_locked(&host);
            Ok(LockedPeer {
                host: host.clone(),
                peer: Scripted::open(&effects, events),
            })
        },
        || 120,
    )
    .unwrap();
    assert!(matches!(
        bridge.launch(&binding()),
        LaunchReply::Accepted(_)
    ));
    assert_eq!(bridge.discover(&binding()).len(), 1);
    assert_eq!(bridge.poll(), Ok(true));
    assert_eq!(count(&effects, "start"), 1);
    assert_eq!(count(&effects, "read"), 1);
    assert_eq!(count(&effects, "notice"), 1);
}

#[test]
fn native_bridge_competing_process_takeover_blocks_old_owner_until_factory_returns() {
    let t = Temp::new();
    let mut stale = peer(&t.0, Events::default());
    let mut owner = child(&t.0, "owner_factory");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
    while !t.0.join("factory-held").exists() {
        assert!(std::time::Instant::now() < deadline);
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    let (ready, entered) = std::sync::mpsc::channel();
    let old = std::thread::spawn(move || {
        ready.send(()).unwrap();
        (
            stale.launch(&binding()),
            stale.discover(&binding()),
            stale.poll(),
        )
    });
    entered
        .recv_timeout(std::time::Duration::from_secs(2))
        .unwrap();
    std::thread::sleep(std::time::Duration::from_millis(100));
    assert!(!old.is_finished());
    std::fs::write(t.0.join("factory-release"), "release").unwrap();
    let (launch, read, poll) = old.join().unwrap();
    assert_eq!(launch, LaunchReply::LostReply);
    assert!(read.is_empty());
    assert_eq!(poll, Err(Error::Conflict));
    assert!(owner.wait().unwrap().success());
    assert_eq!(count(&t.0.join("peer.sqlite"), "start"), 0);
    assert_eq!(count(&t.0.join("peer.sqlite"), "read"), 0);
    assert_eq!(count(&t.0.join("peer.sqlite"), "notice"), 0);
}

#[test]
fn native_bridge_revoked_factory_authority_retains_committed_takeover() {
    struct RevokesBeforeFactory(AtomicU64);
    impl Verifier for RevokesBeforeFactory {
        fn verify(&self, _: &Binding, action: Action) -> bool {
            assert_eq!(action, Action::TakeOwnership);
            self.0.fetch_add(1, Ordering::SeqCst) < 2
        }
    }
    let t = Temp::new();
    let mut stale = peer(&t.0, Events::default());
    let grant = RevokesBeforeFactory(AtomicU64::new(0));
    let calls = AtomicU64::new(0);
    let result = NativeBridge::open(
        t.0.join("host.sqlite"),
        pin(),
        binding(),
        &grant,
        || {
            calls.fetch_add(1, Ordering::SeqCst);
            Ok(Scripted::open(&t.0.join("peer.sqlite"), Events::default()))
        },
        || 120,
    );
    assert!(matches!(result, Err(Error::Denied)));
    assert_eq!(grant.0.load(Ordering::SeqCst), 3);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(stale.launch(&binding()), LaunchReply::LostReply);
    assert_eq!(stale.poll(), Err(Error::Conflict));
    let generation: u64 = Connection::open(t.0.join("host.sqlite"))
        .unwrap()
        .query_row("SELECT generation FROM host_native_owner", [], |r| r.get(0))
        .unwrap();
    assert_eq!(generation, 2);
    assert_eq!(count(&t.0.join("peer.sqlite"), "start"), 0);
}
