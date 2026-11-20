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
            "native-turn-{}-{}",
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
        None
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
            .execute("INSERT INTO effects DEFAULT VALUES", [])
            .unwrap();
        if self.crash {
            std::process::exit(94);
        }
        if self.lost {
            Err(WireError::Timeout)
        } else {
            Ok("turn-native".into())
        }
    }
}
fn bridge(p: &Path, crash: bool, lost: bool) -> NativeBridge<Native, impl Fn() -> u64> {
    let effects = Connection::open(p.join("effects.sqlite")).unwrap();
    effects
        .execute_batch("CREATE TABLE IF NOT EXISTS effects(id INTEGER PRIMARY KEY);")
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
            })
        },
        || 120,
    )
    .unwrap()
}
fn count(p: &Path) -> u64 {
    Connection::open(p.join("effects.sqlite"))
        .unwrap()
        .query_row("SELECT count(*) FROM effects", [], |r| r.get(0))
        .unwrap()
}
fn prepare(p: &Path) {
    let mut b = bridge(p, false, false);
    assert!(matches!(b.launch(&binding()), LaunchReply::Accepted(_)));
}
#[test]
fn native_turn_revoked_during_acknowledgement_retains_identity_without_disclosing_it() {
    struct GrantUntilSend<'a>(&'a Path);
    impl native_turn::Verifier for GrantUntilSend<'_> {
        fn verify_turn(&self, r: &Request) -> bool {
            r == &request() && count(self.0) == 0
        }
    }

    let t = Temp::new();
    prepare(&t.0);
    let mut b = bridge(&t.0, false, false);
    let input = Input::good();
    let result = b.submit_turn(&request(), &GrantUntilSend(&t.0), &input);
    assert_eq!(count(&t.0), 1);
    assert_eq!(result, Err(Error::Denied));
    drop(b);

    let mut reopened = bridge(&t.0, false, false);
    assert_eq!(
        reopened.submit_turn(&request(), &Exact, &input),
        Ok(Outcome::Acknowledged {
            turn_id: "turn-native".into()
        })
    );
    assert_eq!(count(&t.0), 1);
    assert_eq!(input.opens.load(Ordering::SeqCst), 1);
}
#[test]
fn native_turn_exact_authority_digest_and_profile_precede_effects() {
    let t = Temp::new();
    prepare(&t.0);
    let mut b = bridge(&t.0, false, false);
    let input = Input::good();
    assert_eq!(
        b.submit_turn(&request(), &native_turn::Deny, &input),
        Err(Error::Denied)
    );
    assert_eq!(input.opens.load(Ordering::SeqCst), 0);
    let bad = Input {
        bytes: b"changed".to_vec(),
        opens: AtomicU64::new(0),
    };
    assert_eq!(
        b.submit_turn(&request(), &Exact, &bad),
        Err(Error::WrongBinding)
    );
    let mut changed = request();
    changed.profile.model = "other".into();
    assert_eq!(b.submit_turn(&changed, &Exact, &input), Err(Error::Denied));
    changed.profile.model = "synthetic-model --override".into();
    assert_eq!(
        b.submit_turn(&changed, &Exact, &input),
        Err(Error::Malformed)
    );
    assert_eq!(count(&t.0), 0);
    assert_eq!(
        b.submit_turn(&request(), &Exact, &input),
        Ok(Outcome::Acknowledged {
            turn_id: "turn-native".into()
        })
    );
    assert_eq!(
        b.submit_turn(&request(), &Exact, &input),
        Ok(Outcome::Acknowledged {
            turn_id: "turn-native".into()
        })
    );
    assert_eq!(input.opens.load(Ordering::SeqCst), 1);
    assert_eq!(count(&t.0), 1);
    let db = Connection::open(t.0.join("host.sqlite")).unwrap();
    let bytes: Vec<u8> = db
        .query_row("SELECT state FROM host_native_state", [], |r| r.get(0))
        .unwrap();
    assert!(!String::from_utf8(bytes).unwrap().contains(TEXT));
}
#[test]
fn native_turn_revocation_after_resolution_retains_no_retry_reservation() {
    struct Revoke(AtomicU64);
    impl native_turn::Verifier for Revoke {
        fn verify_turn(&self, r: &Request) -> bool {
            r == &request() && self.0.fetch_add(1, Ordering::SeqCst) < 2
        }
    }
    let t = Temp::new();
    prepare(&t.0);
    let mut b = bridge(&t.0, false, false);
    assert_eq!(
        b.submit_turn(&request(), &Revoke(AtomicU64::new(0)), &Input::good()),
        Err(Error::Denied)
    );
    assert_eq!(count(&t.0), 0);
    assert_eq!(
        b.submit_turn(&request(), &Exact, &Input::good()),
        Ok(Outcome::Unknown)
    );
    assert_eq!(count(&t.0), 0);
}
#[test]
fn native_turn_lost_reply_and_stale_owner_never_repeat() {
    let t = Temp::new();
    prepare(&t.0);
    let mut old = bridge(&t.0, false, true);
    assert_eq!(
        old.submit_turn(&request(), &Exact, &Input::good()),
        Ok(Outcome::Unknown)
    );
    let mut new = bridge(&t.0, false, false);
    assert_eq!(
        old.submit_turn(&request(), &Exact, &Input::good()),
        Err(Error::Conflict)
    );
    assert_eq!(
        new.submit_turn(&request(), &Exact, &Input::good()),
        Ok(Outcome::Unknown)
    );
    assert_eq!(count(&t.0), 1);
}
#[test]
fn native_turn_crash_child() {
    let Ok(path) = std::env::var("RUNSPINDLE_TURN_CRASH_FIXTURE") else {
        return;
    };
    let mut b = bridge(Path::new(&path), true, false);
    let _ = b.submit_turn(&request(), &Exact, &Input::good());
    panic!("fixture did not crash after recording effect");
}
#[test]
fn native_turn_fresh_process_crash_keeps_unknown_without_second_effect() {
    let t = Temp::new();
    prepare(&t.0);
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "native_turn_crash_child", "--nocapture"])
        .env("RUNSPINDLE_TURN_CRASH_FIXTURE", &t.0)
        .status()
        .unwrap();
    assert_eq!(status.code(), Some(94));
    let mut b = bridge(&t.0, false, false);
    assert_eq!(
        b.submit_turn(&request(), &Exact, &Input::good()),
        Ok(Outcome::Unknown)
    );
    assert_eq!(count(&t.0), 1);
}
#[test]
fn native_turn_closed_envelope_rejects_unknown_duplicate_and_bad_types() {
    let valid = request().encode().unwrap();
    assert_eq!(Request::decode(&valid).unwrap(), request());
    assert!(Request::decode(&[valid.as_slice(), b" "].concat()).is_err());
    for field in ["override", "cwd", "sandboxPolicy"] {
        let mut value = serde_json::to_value(request()).unwrap();
        value["profile"][field] = serde_json::json!("unsafe");
        assert!(Request::decode(&serde_json::to_vec(&value).unwrap()).is_err());
    }
    let mut value = serde_json::to_value(request()).unwrap();
    value["binding"]["extra"] = serde_json::json!(true);
    assert!(Request::decode(&serde_json::to_vec(&value).unwrap()).is_err());
    let text = String::from_utf8(valid)
        .unwrap()
        .replace("\"byte_length\":", "\"byte_length\":1,\"byte_length\":");
    assert!(Request::decode(text.as_bytes()).is_err());
    let mut value = serde_json::to_value(request()).unwrap();
    value["input"]["byte_length"] = serde_json::json!(true);
    assert!(Request::decode(&serde_json::to_vec(&value).unwrap()).is_err());
}
