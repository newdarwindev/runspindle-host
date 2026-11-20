//! Synthetic task authority/peer; real durable SQLite and fresh-process effects.
use runspindle_host::{
    lifecycle::{Action, Binding, Error, LaunchReply, Peer, Verifier},
    native::{NativeIds, NativeNotice, WireError},
    native_bridge::{LocalPin, NativeBridge, NativeIo},
    native_turn::{self, InputRef, InputResolver, Outcome, Profile, Request},
};
use rusqlite::Connection;
use sha2::{Digest, Sha256};
use std::{
    io::{Cursor, Read},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};
static SERIAL: AtomicU64 = AtomicU64::new(0);
const TEXT: &str = "synthetic task input canary";
struct Temp(PathBuf);
impl Temp {
    fn new() -> Self {
        let p = std::env::temp_dir().join(format!(
            "native-cancel-{}-{}",
            std::process::id(),
            SERIAL.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&p).unwrap();
        Self(p)
    }
}
impl Drop for Temp {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
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
fn request() -> Request {
    Request {
        version: native_turn::VERSION.into(),
        binding: binding(),
        thread_id: "thread-native".into(),
        input: InputRef {
            sha256: format!("{:x}", Sha256::digest(TEXT.as_bytes())),
            byte_length: TEXT.len() as u64,
        },
        profile: Profile {
            kind: "read_only_text_v1".into(),
            model: "synthetic-model".into(),
        },
    }
}
struct Ownership;
impl Verifier for Ownership {
    fn verify(&self, _: &Binding, a: Action) -> bool {
        a == Action::TakeOwnership
    }
}
struct Exact;
impl native_turn::Verifier for Exact {
    fn verify_turn(&self, r: &Request) -> bool {
        r == &request()
    }
}
struct Input {
    bytes: Vec<u8>,
    opens: AtomicU64,
}
impl Input {
    fn good() -> Self {
        Self {
            bytes: TEXT.as_bytes().to_vec(),
            opens: AtomicU64::new(0),
        }
    }
}
impl InputResolver for Input {
    fn open(&self, _: &InputRef) -> std::io::Result<Box<dyn Read>> {
        self.opens.fetch_add(1, Ordering::SeqCst);
        Ok(Box::new(Cursor::new(self.bytes.clone())))
    }
}
struct Native {
    effects: Connection,
    crash: bool,
    lost: bool,
    start_lost: bool,
}
impl NativeIo for Native {
    fn start_empty_thread(&mut self) -> Result<String, WireError> {
        Ok("thread-native".into())
    }
    fn read_thread(&mut self, t: &str) -> Result<NativeIds, WireError> {
        Ok(NativeIds {
            thread_id: t.into(),
            turn_id: None,
            item_id: None,
        })
    }
    fn next_notice(&mut self, _: &str) -> Option<Result<NativeNotice, WireError>> {
        let value: Option<String> = self
            .effects
            .query_row("SELECT kind FROM notices LIMIT 1", [], |r| r.get(0))
            .ok();
        let kind = value?;
        self.effects.execute("DELETE FROM notices", []).unwrap();
        Some(Ok(match kind.as_str() {
            "start" => NativeNotice::TurnStarted {
                thread_id: "thread-native".into(),
                turn_id: "turn-native".into(),
            },
            "completed" => NativeNotice::TurnCompleted {
                thread_id: "thread-native".into(),
                turn_id: "turn-native".into(),
                terminal: runspindle_host::lifecycle::Terminal::Completed,
            },
            _ => NativeNotice::TurnCompleted {
                thread_id: "thread-native".into(),
                turn_id: "turn-native".into(),
                terminal: runspindle_host::lifecycle::Terminal::Failed,
            },
        }))
    }
    fn start_text_turn(
        &mut self,
        permit: native_turn::TurnPermit<'_>,
    ) -> Result<String, WireError> {
        assert_eq!(
            (permit.thread_id(), permit.text(), permit.model()),
            ("thread-native", TEXT, "synthetic-model")
        );
        self.effects
            .execute("INSERT INTO effects(action) VALUES('start')", [])
            .unwrap();
        if self.start_lost {
            Err(WireError::Timeout)
        } else {
            Ok("turn-native".into())
        }
    }
    fn interrupt_turn(&mut self, permit: native_turn::CancelPermit<'_>) -> Result<(), WireError> {
        assert_eq!(
            (permit.thread_id(), permit.turn_id()),
            ("thread-native", "turn-native")
        );
        self.effects
            .execute("INSERT INTO effects(action) VALUES('cancel')", [])
            .unwrap();
        if self.crash {
            std::process::exit(95);
        }
        if self.lost {
            Err(WireError::Timeout)
        } else {
            Ok(())
        }
    }
}
fn bridge(p: &Path, crash: bool, lost: bool) -> NativeBridge<Native, impl Fn() -> u64> {
    let effects = Connection::open(p.join("effects.sqlite")).unwrap();
    effects
        .execute_batch("CREATE TABLE IF NOT EXISTS effects(id INTEGER PRIMARY KEY,action TEXT);CREATE TABLE IF NOT EXISTS notices(kind TEXT);")
        .unwrap();
    NativeBridge::open(
        p.join("host.sqlite"),
        LocalPin::new("host", "workspace").unwrap(),
        binding(),
        &Ownership,
        || {
            Ok(Native {
                effects,
                crash,
                lost,
                start_lost: p.join("lose-start").exists(),
            })
        },
        || 120,
    )
    .unwrap()
}
fn count(p: &Path) -> u64 {
    Connection::open(p.join("effects.sqlite"))
        .unwrap()
        .query_row(
            "SELECT count(*) FROM effects WHERE action='cancel'",
            [],
            |r| r.get(0),
        )
        .unwrap()
}
fn prepare(p: &Path) {
    let mut b = bridge(p, false, false);
    assert!(matches!(b.launch(&binding()), LaunchReply::Accepted(_)));
}

fn cancel() -> native_turn::CancelRequest {
    native_turn::CancelRequest {
        version: native_turn::CANCEL_VERSION.into(),
        task: request(),
        turn_id: "turn-native".into(),
    }
}
struct CancelGrant;
impl native_turn::CancelVerifier for CancelGrant {
    fn verify_cancel(&self, r: &native_turn::CancelRequest) -> bool {
        r == &cancel()
    }
}
fn prepared(p: &Path) {
    prepare(p);
    let mut b = bridge(p, false, false);
    assert!(matches!(
        b.submit_turn(&request(), &Exact, &Input::good()).unwrap(),
        Outcome::Acknowledged { .. }
    ));
}
fn notice(p: &Path, kind: &str) {
    Connection::open(p.join("effects.sqlite"))
        .unwrap()
        .execute("INSERT INTO notices VALUES(?1)", [kind])
        .unwrap();
}
#[test]
fn native_cancel_exact_target_and_separate_authority_have_one_effect() {
    use native_turn::CancelOutcome;
    let t = Temp::new();
    prepared(&t.0);
    let mut b = bridge(&t.0, false, false);
    assert_eq!(
        b.cancel_turn(&cancel(), &native_turn::Deny),
        Err(Error::Denied)
    );
    let mut changed = cancel();
    changed.task.profile.model = "foreign".into();
    assert_eq!(b.cancel_turn(&changed, &CancelGrant), Err(Error::Denied));
    struct Allow;
    impl native_turn::CancelVerifier for Allow {
        fn verify_cancel(&self, _: &native_turn::CancelRequest) -> bool {
            true
        }
    }
    changed = cancel();
    changed.turn_id = "foreign".into();
    assert_eq!(b.cancel_turn(&changed, &Allow), Err(Error::WrongBinding));
    assert_eq!(count(&t.0), 0);
    assert_eq!(
        b.cancel_turn(&cancel(), &CancelGrant),
        Ok(CancelOutcome::Acknowledged)
    );
    assert_eq!(
        b.cancel_turn(&cancel(), &CancelGrant),
        Ok(CancelOutcome::Acknowledged)
    );
    assert_eq!(b.turn_outcome().unwrap(), None);
    assert_eq!(count(&t.0), 1);
}
#[test]
fn native_cancel_unsolicited_and_unresolved_turns_never_authorize_interrupt() {
    let t = Temp::new();
    prepare(&t.0);
    let mut b = bridge(&t.0, false, false);
    notice(&t.0, "start");
    assert!(b.poll().unwrap());
    assert_eq!(b.cancel_turn(&cancel(), &CancelGrant), Err(Error::Unknown));
    assert_eq!(count(&t.0), 0);
    let t = Temp::new();
    prepare(&t.0);
    std::fs::write(t.0.join("lose-start"), b"synthetic").unwrap();
    let mut b = bridge(&t.0, false, false);
    assert_eq!(
        b.submit_turn(&request(), &Exact, &Input::good()),
        Ok(Outcome::Unknown)
    );
    assert_eq!(b.cancel_turn(&cancel(), &CancelGrant), Err(Error::Unknown));
    assert_eq!(count(&t.0), 0);
}
#[test]
fn native_cancel_revoked_during_acknowledgement_retains_receipt_without_disclosing_it() {
    use native_turn::CancelOutcome;

    struct GrantUntilInterrupt<'a>(&'a Path);
    impl native_turn::CancelVerifier for GrantUntilInterrupt<'_> {
        fn verify_cancel(&self, r: &native_turn::CancelRequest) -> bool {
            r == &cancel() && count(self.0) == 0
        }
    }

    let t = Temp::new();
    prepared(&t.0);
    let mut b = bridge(&t.0, false, false);
    let result = b.cancel_turn(&cancel(), &GrantUntilInterrupt(&t.0));
    assert_eq!(count(&t.0), 1);
    assert_eq!(result, Err(Error::Denied));
    drop(b);

    let mut reopened = bridge(&t.0, false, false);
    assert_eq!(
        reopened.cancel_turn(&cancel(), &CancelGrant),
        Ok(CancelOutcome::Acknowledged)
    );
    assert_eq!(reopened.turn_outcome().unwrap(), None);
    assert_eq!(count(&t.0), 1);
}
#[test]
fn native_cancel_revocation_and_lost_ack_remain_unknown_without_retry() {
    use native_turn::CancelOutcome;
    struct Revoke(AtomicU64);
    impl native_turn::CancelVerifier for Revoke {
        fn verify_cancel(&self, _: &native_turn::CancelRequest) -> bool {
            self.0.fetch_add(1, Ordering::SeqCst) == 0
        }
    }
    let t = Temp::new();
    prepared(&t.0);
    let mut b = bridge(&t.0, false, false);
    assert_eq!(
        b.cancel_turn(&cancel(), &Revoke(AtomicU64::new(0))),
        Err(Error::Denied)
    );
    assert_eq!(
        b.cancel_turn(&cancel(), &CancelGrant),
        Ok(CancelOutcome::Unknown)
    );
    assert_eq!(count(&t.0), 0);
    let t = Temp::new();
    prepared(&t.0);
    let mut old = bridge(&t.0, false, true);
    assert_eq!(
        old.cancel_turn(&cancel(), &CancelGrant),
        Ok(CancelOutcome::Unknown)
    );
    let mut new = bridge(&t.0, false, false);
    assert_eq!(
        old.cancel_turn(&cancel(), &CancelGrant),
        Err(Error::Conflict)
    );
    assert_eq!(
        new.cancel_turn(&cancel(), &CancelGrant),
        Ok(CancelOutcome::Unknown)
    );
    assert_eq!(count(&t.0), 1);
}
#[test]
fn native_cancel_terminal_metadata_before_and_after_ack_is_not_overwritten() {
    use native_turn::CancelOutcome;
    use runspindle_host::lifecycle::Terminal;
    for (kind, terminal) in [
        ("completed", Terminal::Completed),
        ("failed", Terminal::Failed),
    ] {
        let t = Temp::new();
        prepared(&t.0);
        let mut b = bridge(&t.0, false, false);
        notice(&t.0, kind);
        assert!(b.poll().unwrap());
        assert_eq!(
            b.cancel_turn(&cancel(), &CancelGrant),
            Ok(CancelOutcome::ObservedTerminal { terminal })
        );
        assert_eq!(count(&t.0), 0);
        let t = Temp::new();
        prepared(&t.0);
        let mut b = bridge(&t.0, false, false);
        assert_eq!(
            b.cancel_turn(&cancel(), &CancelGrant),
            Ok(CancelOutcome::Acknowledged)
        );
        notice(&t.0, kind);
        assert!(b.poll().unwrap());
        assert_eq!(
            b.cancel_turn(&cancel(), &CancelGrant),
            Ok(CancelOutcome::ObservedTerminal { terminal })
        );
        assert_eq!(b.turn_outcome().unwrap(), Some(terminal));
        assert_eq!(count(&t.0), 1);
    }
}
#[test]
fn native_cancel_crash_child() {
    let Ok(path) = std::env::var("RUNSPINDLE_CANCEL_CRASH_FIXTURE") else {
        return;
    };
    let mut b = bridge(Path::new(&path), true, false);
    let _ = b.cancel_turn(&cancel(), &CancelGrant);
    panic!("fixture did not crash");
}
#[test]
fn native_cancel_fresh_process_crash_never_repeats_interrupt() {
    let t = Temp::new();
    prepared(&t.0);
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "native_cancel_crash_child", "--nocapture"])
        .env("RUNSPINDLE_CANCEL_CRASH_FIXTURE", &t.0)
        .status()
        .unwrap();
    assert_eq!(status.code(), Some(95));
    let mut b = bridge(&t.0, false, false);
    assert_eq!(
        b.cancel_turn(&cancel(), &CancelGrant),
        Ok(native_turn::CancelOutcome::Unknown)
    );
    assert_eq!(count(&t.0), 1);
}
#[test]
fn native_cancel_canonical_request_has_no_hidden_target_overrides() {
    let r = cancel();
    let bytes = r.encode().unwrap();
    assert_eq!(native_turn::CancelRequest::decode(&bytes).unwrap(), r);
    let mut value = serde_json::to_value(r).unwrap();
    value["task"]["binding"]["extra"] = serde_json::json!(true);
    assert!(native_turn::CancelRequest::decode(&serde_json::to_vec(&value).unwrap()).is_err());
    assert!(native_turn::CancelRequest::decode(&[bytes.as_slice(), b" "].concat()).is_err());
}
