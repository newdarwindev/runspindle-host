//! Ordinary-path contract, independently observed SQLite and factory effects.
use runspindle_host::{
    lifecycle::{Action, Binding, Error, LaunchReply, Peer, Verifier},
    native::{NativeIds, NativeNotice, WireError},
    native_bridge::{LocalPin, NativeBridge, NativeIo},
    storage::{Memory, Sqlite, Storage, Stored},
};
use rusqlite::Connection;
use std::{
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

static SERIAL: AtomicU64 = AtomicU64::new(0);
struct Temp(PathBuf);
impl Temp {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "host-durable-paths-{}-{}",
            std::process::id(),
            SERIAL.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
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
struct Grant;
impl Verifier for Grant {
    fn verify(&self, _: &Binding, action: Action) -> bool {
        action == Action::TakeOwnership
    }
}
struct Native {
    effects: Connection,
}
impl NativeIo for Native {
    fn start_empty_thread(&mut self) -> Result<String, WireError> {
        self.effects
            .execute("INSERT INTO effects DEFAULT VALUES", [])
            .unwrap();
        Ok("native-thread".into())
    }
    fn read_thread(&mut self, thread: &str) -> Result<NativeIds, WireError> {
        Ok(NativeIds {
            thread_id: thread.into(),
            turn_id: None,
            item_id: None,
        })
    }
    fn next_notice(&mut self, _: &str) -> Option<Result<NativeNotice, WireError>> {
        None
    }
}
fn bridge(
    path: &Path,
    effects: &Path,
    calls: &AtomicU64,
) -> Result<NativeBridge<Native, impl Fn() -> u64>, Error> {
    NativeBridge::open(
        path,
        LocalPin::new("host", "workspace").unwrap(),
        binding(),
        &Grant,
        || {
            calls.fetch_add(1, Ordering::SeqCst);
            let effects = Connection::open(effects).unwrap();
            effects
                .execute_batch("CREATE TABLE IF NOT EXISTS effects(id INTEGER PRIMARY KEY);")
                .unwrap();
            Ok(Native { effects })
        },
        || 120,
    )
}
#[test]
fn durable_paths_reject_ephemeral_and_uri_aliases_before_factory_or_database() {
    let t = Temp::new();
    let calls = AtomicU64::new(0);
    let effects = t.0.join("effects.sqlite");
    let file_uri = format!(
        "file:{}?mode=rwc",
        t.0.join("must-not-exist.sqlite").display()
    );
    for path in [
        "",
        ":memory:",
        "file::memory:",
        "file::memory:?cache=shared",
        "file:alias?mode=memory&cache=shared",
        "file:",
        "file:///",
        &file_uri,
    ] {
        assert!(
            matches!(Sqlite::open(path), Err(Error::Unsupported)),
            "{path}"
        );
        assert!(
            matches!(
                bridge(Path::new(path), &effects, &calls),
                Err(Error::Unsupported)
            ),
            "{path}"
        );
    }
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(!effects.exists());
    assert!(!t.0.join("must-not-exist.sqlite").exists());
}
#[test]
fn durable_paths_pinned_sqlite_special_names_are_not_persistent() {
    // This records actual selected rusqlite behavior, not a simulated parser.
    for path in [
        "",
        ":memory:",
        "file::memory:",
        "file:alias?mode=memory&cache=shared",
    ] {
        {
            let db = Connection::open(path).unwrap();
            db.execute_batch("CREATE TABLE marker(value INTEGER);INSERT INTO marker VALUES(1);")
                .unwrap();
            let file: String = db
                .query_row(
                    "SELECT file FROM pragma_database_list WHERE name='main'",
                    [],
                    |r| r.get(0),
                )
                .unwrap();
            assert!(file.is_empty());
        }
        let db = Connection::open(path).unwrap();
        assert_eq!(
            db.query_row(
                "SELECT count(*) FROM sqlite_master WHERE name='marker'",
                [],
                |r| r.get::<_, u64>(0)
            )
            .unwrap(),
            0
        );
    }
    assert!(Memory::default().load().is_ok()); // Explicit nondurable API remains available.
}
#[test]
fn durable_paths_ordinary_files_reopen_without_reset() {
    let t = Temp::new();
    let paths = [t.0.join("normal.sqlite")];
    #[cfg(unix)]
    let paths = [
        paths[0].clone(),
        t.0.join(":memory:"),
        t.0.join("file:literal?mode=memory"),
        t.0.join("File:ordinary"),
    ];
    for path in paths {
        let mut db = Sqlite::open(&path).unwrap();
        let state = Stored {
            config: Some((8, 16, 100)),
            ..Stored::default()
        };
        assert_eq!(db.compare_and_swap(0, &state).unwrap(), 1);
        drop(db);
        assert!(path.is_file());
        let (generation, state) = Sqlite::open(&path).unwrap().load().unwrap();
        assert_eq!(generation, 1);
        assert_eq!(state.config, Some((8, 16, 100)));
    }
}
#[test]
fn durable_paths_restart_child() {
    let Ok(directory) = std::env::var("HOST_DURABLE_PATH_FIXTURE") else {
        return;
    };
    let dir = Path::new(&directory);
    let path = Path::new("./:memory:");
    let calls = AtomicU64::new(0);
    let mut b = bridge(path, &dir.join("effects.sqlite"), &calls).unwrap();
    match std::env::var("HOST_DURABLE_PATH_MODE").unwrap().as_str() {
        "first" => assert!(matches!(b.launch(&binding()), LaunchReply::Accepted(_))),
        _ => {
            assert!(matches!(b.launch(&binding()), LaunchReply::LostReply));
            assert_eq!(b.identities().unwrap().unwrap().thread_id, "native-thread");
        }
    }
}
#[cfg(unix)]
#[test]
fn durable_paths_prefixed_memory_filename_keeps_reservation_in_fresh_processes() {
    let t = Temp::new();
    for mode in ["first", "reopen"] {
        let result = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "durable_paths_restart_child", "--nocapture"])
            .current_dir(&t.0)
            .env("HOST_DURABLE_PATH_FIXTURE", &t.0)
            .env("HOST_DURABLE_PATH_MODE", mode)
            .status()
            .unwrap();
        assert!(result.success());
    }
    assert!(t.0.join(":memory:").is_file());
    let db = Connection::open(t.0.join("effects.sqlite")).unwrap();
    assert_eq!(
        db.query_row("SELECT count(*) FROM effects", [], |r| r.get::<_, u64>(0))
            .unwrap(),
        1
    );
}
#[cfg(unix)]
#[test]
fn durable_paths_non_utf8_ordinary_path_reports_platform_storage_outcome() {
    use std::os::unix::ffi::OsStringExt;
    let t = Temp::new();
    let path = t.0.join(std::ffi::OsString::from_vec(
        b"ordinary-\xff.sqlite".to_vec(),
    ));
    #[cfg(target_os = "macos")]
    {
        // This macOS filesystem rejects the invalid UTF-8 name. The path policy
        // still admits its ordinary spelling; SQLite reports the OS failure.
        let calls = AtomicU64::new(0);
        let effects = t.0.join("effects.sqlite");
        assert!(matches!(Sqlite::open(&path), Err(Error::Storage)));
        assert!(matches!(
            bridge(&path, &effects, &calls),
            Err(Error::Storage)
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert!(!path.exists());
        assert!(!effects.exists());
    }
    #[cfg(not(target_os = "macos"))]
    {
        Sqlite::open(&path).unwrap();
        assert!(path.is_file());
    }
}
