//! Pinned metadata-only app-server stdio seam. No turn submission or account access.
//! Native wire loss never authorizes a repeated thread/start.
use serde::{
    de::{self, MapAccess, SeqAccess, Visitor},
    Deserialize, Deserializer,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::VecDeque;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::{Duration, Instant};

pub const SOURCE: &str = "d32063f96c2dff1dc96af839b3952abb6acfeda4";
pub const MAX_FRAME: usize = 65_536;
pub const MAX_REQUEST: usize = 4096;
pub const MAX_PENDING: usize = 32;
pub const BINARY_SHA256: &str = "739cda86e15d30a6e2b40cf8dcbbfb988aaf48b305b27065573bb8e9fd41d002";
const DEADLINE: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WireError {
    Pin,
    Io,
    Closed,
    Malformed,
    Oversized,
    Backpressure,
    Timeout,
    Unexpected,
    Unsupported,
    Rejected,
}

// serde_json::Value alone silently keeps the last duplicate object member.
// Native identity and reply correlation must reject that ambiguity at every depth.
struct UniqueValue(Value);
impl<'de> Deserialize<'de> for UniqueValue {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct UniqueVisitor;
        impl<'de> Visitor<'de> for UniqueVisitor {
            type Value = UniqueValue;
            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("JSON with unique object members")
            }
            fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
                Ok(UniqueValue(Value::Null))
            }
            fn visit_bool<E: de::Error>(self, value: bool) -> Result<Self::Value, E> {
                Ok(UniqueValue(Value::Bool(value)))
            }
            fn visit_i64<E: de::Error>(self, value: i64) -> Result<Self::Value, E> {
                Ok(UniqueValue(value.into()))
            }
            fn visit_u64<E: de::Error>(self, value: u64) -> Result<Self::Value, E> {
                Ok(UniqueValue(value.into()))
            }
            fn visit_f64<E: de::Error>(self, value: f64) -> Result<Self::Value, E> {
                serde_json::Number::from_f64(value)
                    .map(|n| UniqueValue(Value::Number(n)))
                    .ok_or_else(|| E::custom("invalid number"))
            }
            fn visit_str<E: de::Error>(self, value: &str) -> Result<Self::Value, E> {
                Ok(UniqueValue(Value::String(value.to_owned())))
            }
            fn visit_string<E: de::Error>(self, value: String) -> Result<Self::Value, E> {
                Ok(UniqueValue(Value::String(value)))
            }
            fn visit_seq<A: SeqAccess<'de>>(
                self,
                mut sequence: A,
            ) -> Result<Self::Value, A::Error> {
                let mut values = Vec::new();
                while let Some(value) = sequence.next_element::<UniqueValue>()? {
                    values.push(value.0);
                }
                Ok(UniqueValue(Value::Array(values)))
            }
            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
                let mut values = serde_json::Map::new();
                while let Some(key) = map.next_key::<String>()? {
                    if values.contains_key(&key) {
                        return Err(de::Error::custom("duplicate JSON member"));
                    }
                    values.insert(key, map.next_value::<UniqueValue>()?.0);
                }
                Ok(UniqueValue(Value::Object(values)))
            }
        }
        deserializer.deserialize_any(UniqueVisitor)
    }
}

/// Reads one newline-delimited frame with a hard allocation bound, including newline.
/// No JSON-RPC 2.0 marker is required by the pinned app-server protocol.
pub fn read_frame(reader: &mut impl BufRead) -> Result<Value, WireError> {
    let mut bytes = Vec::new();
    reader
        .take((MAX_FRAME + 1) as u64)
        .read_until(b'\n', &mut bytes)
        .map_err(|_| WireError::Io)?;
    if bytes.is_empty() {
        return Err(WireError::Closed);
    }
    if bytes.len() > MAX_FRAME {
        return Err(WireError::Oversized);
    }
    if bytes.last() != Some(&b'\n') {
        return Err(WireError::Malformed);
    }
    let value = serde_json::from_slice::<UniqueValue>(&bytes)
        .map_err(|_| WireError::Malformed)?
        .0;
    if !value.is_object() {
        return Err(WireError::Malformed);
    }
    Ok(value)
}

/// Sanitized identity metadata; no native text, reasoning, paths or content is retained.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeIds {
    pub thread_id: String,
    pub turn_id: Option<String>,
    pub item_id: Option<String>,
}
fn id(value: &Value) -> Result<String, WireError> {
    let text = value.as_str().ok_or(WireError::Malformed)?;
    if text.is_empty()
        || text.len() > 128
        || !text
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-_.:".contains(&b))
    {
        return Err(WireError::Malformed);
    }
    Ok(text.to_owned())
}
/// Parse only supported identity notifications, preserving peer IDs byte-for-byte.
pub fn notification_ids(value: &Value, expected_thread: &str) -> Result<NativeIds, WireError> {
    if value.get("id").is_some() {
        return Err(WireError::Unexpected);
    }
    let p = &value["params"];
    let (thread, turn, item) = match value["method"].as_str() {
        Some("thread/started") => (id(&p["thread"]["id"])?, None, None),
        Some("turn/started" | "turn/completed") => {
            (id(&p["threadId"])?, Some(id(&p["turn"]["id"])?), None)
        }
        Some("item/started" | "item/completed") => (
            id(&p["threadId"])?,
            Some(id(&p["turnId"])?),
            Some(id(&p["item"]["id"])?),
        ),
        _ => return Err(WireError::Unsupported),
    };
    if thread != expected_thread {
        return Err(WireError::Unexpected);
    }
    Ok(NativeIds {
        thread_id: thread,
        turn_id: turn,
        item_id: item,
    })
}

/// One request in flight. Overflow, foreign/duplicate reply, timeout and server requests
/// poison the connection. Retry/relaunch is a lifecycle decision, never a transport action.
type WriteRequest = (Vec<u8>, mpsc::Sender<Result<(), WireError>>);

pub struct StdioSession {
    child: Child,
    input: Option<SyncSender<WriteRequest>>,
    incoming: Receiver<Result<Value, WireError>>,
    reader_failed: Arc<AtomicBool>,
    notifications: VecDeque<Value>,
    next_id: u64,
    initialized: bool,
    closed: bool,
    directory: PathBuf,
}
impl StdioSession {
    /// The caller provides a fresh private directory, never a real Codex home/workspace.
    /// Copy and hash the executable before executing the exact copied bytes. The only
    /// accepted binary digest is the reviewed local-platform pin in the repository.
    #[cfg(unix)]
    pub fn spawn(binary: &Path, directory: &Path) -> Result<Self, WireError> {
        use std::os::unix::fs::DirBuilderExt;
        fs::DirBuilder::new()
            .mode(0o700)
            .create(directory)
            .map_err(|_| WireError::Io)?;
        let result = Self::spawn_inner(binary, directory);
        if result.is_err() {
            let _ = fs::remove_dir_all(directory);
        }
        result
    }
    #[cfg(unix)]
    fn spawn_inner(binary: &Path, directory: &Path) -> Result<Self, WireError> {
        use std::os::unix::fs::PermissionsExt;
        if std::env::consts::OS != "macos" || std::env::consts::ARCH != "aarch64" {
            return Err(WireError::Pin);
        }
        let copy = directory.join("codex");
        fs::copy(binary, &copy).map_err(|_| WireError::Io)?;
        let mut file = File::open(&copy).map_err(|_| WireError::Io)?;
        let mut hash = Sha256::new();
        let mut buffer = [0u8; 65536];
        loop {
            let n = file.read(&mut buffer).map_err(|_| WireError::Io)?;
            if n == 0 {
                break;
            }
            hash.update(&buffer[..n]);
        }
        if format!("{:x}", hash.finalize()) != BINARY_SHA256 {
            return Err(WireError::Pin);
        }
        fs::set_permissions(&copy, fs::Permissions::from_mode(0o500)).map_err(|_| WireError::Io)?;
        let home = directory.join("home");
        fs::create_dir(&home).map_err(|_| WireError::Io)?;
        fs::write(
            home.join("config.toml"),
            "[analytics]\nenabled = false\n[feedback]\nenabled = false\n",
        )
        .map_err(|_| WireError::Io)?;
        let child = Command::new(copy)
            .args(["app-server", "--stdio"])
            .env_clear()
            .env("HOME", &home)
            .env("CODEX_HOME", &home)
            .env("PATH", "/usr/bin:/bin")
            .current_dir(&home)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|_| WireError::Io)?;
        Self::from_child(child, directory.to_owned())
    }
    fn from_child(mut child: Child, directory: PathBuf) -> Result<Self, WireError> {
        let mut stdin = child.stdin.take().ok_or(WireError::Io)?;
        let (writer, requests) =
            mpsc::sync_channel::<(Vec<u8>, mpsc::Sender<Result<(), WireError>>)>(1);
        std::thread::spawn(move || {
            while let Ok((bytes, done)) = requests.recv() {
                let result = stdin.write_all(&bytes).map_err(|_| WireError::Io);
                let failed = result.is_err();
                let _ = done.send(result);
                if failed {
                    break;
                }
            }
        });
        let input = Some(writer);
        let output = child.stdout.take().ok_or(WireError::Io)?;
        let (sender, incoming) = mpsc::sync_channel(MAX_PENDING);
        let reader_failed = Arc::new(AtomicBool::new(false));
        let failed_reader = Arc::clone(&reader_failed);
        std::thread::spawn(move || {
            let mut reader = BufReader::new(output);
            loop {
                let frame = read_frame(&mut reader);
                let failed = frame.is_err();
                if sender.try_send(frame).is_err() || failed {
                    failed_reader.store(true, Ordering::Release);
                    break;
                }
            }
        });
        Ok(Self {
            child,
            input,
            incoming,
            reader_failed,
            notifications: VecDeque::new(),
            next_id: 1,
            initialized: false,
            closed: false,
            directory,
        })
    }
    fn send(&mut self, value: Value) -> Result<(), WireError> {
        if self.closed || self.reader_failed.load(Ordering::Acquire) {
            return Err(WireError::Closed);
        }
        let mut bytes = serde_json::to_vec(&value).map_err(|_| WireError::Malformed)?;
        bytes.push(b'\n');
        if bytes.len() > MAX_REQUEST {
            return Err(WireError::Oversized);
        }
        let (done, completion) = mpsc::channel();
        self.input
            .as_ref()
            .ok_or(WireError::Closed)?
            .try_send((bytes, done))
            .map_err(|_| WireError::Backpressure)?;
        completion
            .recv_timeout(DEADLINE)
            .map_err(|_| WireError::Timeout)?
    }
    fn rpc(&mut self, method: &str, params: Value) -> Result<Value, WireError> {
        let result = self.rpc_inner(method, params);
        if result.is_err() {
            self.close();
        }
        result
    }
    fn rpc_inner(&mut self, method: &str, params: Value) -> Result<Value, WireError> {
        let request_id = self.next_id;
        self.next_id = self.next_id.checked_add(1).ok_or(WireError::Closed)?;
        self.send(json!({"id":request_id,"method":method,"params":params}))?;
        let deadline = Instant::now() + DEADLINE;
        loop {
            let frame = self
                .incoming
                .recv_timeout(deadline.saturating_duration_since(Instant::now()))
                .map_err(|e| match e {
                    mpsc::RecvTimeoutError::Timeout => WireError::Timeout,
                    _ => WireError::Closed,
                })??;
            if frame.get("id").is_some() {
                if frame["id"] != request_id || frame.get("method").is_some() {
                    return Err(WireError::Unexpected);
                }
                if frame.get("error").is_some() {
                    return Err(WireError::Rejected);
                }
                return frame.get("result").cloned().ok_or(WireError::Malformed);
            }
            if !frame["method"].is_string() || frame.get("params").is_none() {
                return Err(WireError::Malformed);
            }
            if self.notifications.len() >= MAX_PENDING {
                return Err(WireError::Backpressure);
            }
            self.notifications.push_back(frame);
        }
    }
    /// Opt out of experimental methods. A version response is not support for execution.
    pub fn initialize(&mut self) -> Result<(), WireError> {
        if self.initialized {
            return Err(WireError::Unexpected);
        }
        let reply = self.rpc("initialize", json!({"clientInfo":{"name":"runspindle-host","version":"0.0.0"},"capabilities":{"experimentalApi":false}}))?;
        if !reply["userAgent"]
            .as_str()
            .is_some_and(|s| s.contains("0.169.0"))
        {
            self.close();
            return Err(WireError::Pin);
        }
        if let Err(error) = self.send(json!({"method":"initialized"})) {
            self.close();
            return Err(error);
        }
        self.initialized = true;
        Ok(())
    }
    /// Creates an empty native thread only. It never sends a prompt or starts a turn.
    pub fn start_empty_thread(&mut self) -> Result<String, WireError> {
        if !self.initialized {
            return Err(WireError::Unsupported);
        }
        let reply = self.rpc(
            "thread/start",
            json!({"ephemeral":true,"approvalPolicy":"never","sandbox":"read-only"}),
        )?;
        match id(&reply["thread"]["id"]) {
            Ok(value) => Ok(value),
            Err(error) => {
                self.close();
                Err(error)
            }
        }
    }
    pub fn read_thread(&mut self, thread: &str) -> Result<NativeIds, WireError> {
        if !self.initialized {
            return Err(WireError::Unsupported);
        }
        id(&json!(thread))?;
        let reply = self.rpc(
            "thread/read",
            json!({"threadId":thread,"includeTurns":false}),
        )?;
        let observed = match id(&reply["thread"]["id"]) {
            Ok(value) => value,
            Err(error) => {
                self.close();
                return Err(error);
            }
        };
        if observed != thread {
            self.close();
            return Err(WireError::Unexpected);
        }
        Ok(NativeIds {
            thread_id: observed,
            turn_id: None,
            item_id: None,
        })
    }
    /// A successful interrupt response is acknowledgement only, never completion.
    pub fn interrupt(&mut self, thread: &str, turn: &str) -> Result<(), WireError> {
        if !self.initialized {
            return Err(WireError::Unsupported);
        }
        id(&json!(thread))?;
        id(&json!(turn))?;
        self.rpc("turn/interrupt", json!({"threadId":thread,"turnId":turn}))?;
        Ok(())
    }
    pub fn take_notification(&mut self, thread: &str) -> Option<Result<NativeIds, WireError>> {
        self.notifications
            .pop_front()
            .map(|value| notification_ids(&value, thread))
    }
    pub fn close(&mut self) {
        self.closed = true;
        self.input.take();
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
impl Drop for StdioSession {
    fn drop(&mut self) {
        self.close();
        let _ = fs::remove_dir_all(&self.directory);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use std::sync::atomic::{AtomicU64, Ordering};
    static SERIAL: AtomicU64 = AtomicU64::new(0);
    fn scripted(behavior: &str) -> (StdioSession, PathBuf) {
        let directory = std::env::temp_dir().join(format!(
            "runspindle-wire-{}-{}",
            std::process::id(),
            SERIAL.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&directory).unwrap();
        let ledger = directory.join("calls");
        let script = format!(
            r#"
import sys,json
for line in sys.stdin:
 r=json.loads(line)
 with open(sys.argv[1],'a') as f: f.write(r['method']+'\n')
 if r['method']=='initialize':
  print(json.dumps({{'id':r['id'],'result':{{'userAgent':'codex/0.169.0'}}}}),flush=True)
 elif r['method']=='initialized': pass
 else:
{behavior}
"#
        );
        let child = Command::new("python3")
            .args(["-u", "-c", &script])
            .arg(&ledger)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        (StdioSession::from_child(child, directory).unwrap(), ledger)
    }
    #[test]
    fn native_frames_are_bounded_and_fail_closed() {
        for bytes in [
            b"{}".to_vec(),
            b"[]\n".to_vec(),
            b"garbage\n".to_vec(),
            b"\xff\n".to_vec(),
        ] {
            assert_eq!(
                read_frame(&mut Cursor::new(bytes)),
                Err(WireError::Malformed)
            );
        }
        assert_eq!(
            read_frame(&mut Cursor::new(vec![b'x'; MAX_FRAME + 1])),
            Err(WireError::Oversized)
        );
        assert_eq!(
            read_frame(&mut Cursor::new(Vec::<u8>::new())),
            Err(WireError::Closed)
        );
    }
    #[test]
    fn native_identity_fences_keep_actual_turn_and_item_ids() {
        let event = json!({"method":"item/started","params":{"threadId":"thread-native","turnId":"turn-native","item":{"id":"item-native","text":"private text must not appear"}}});
        assert_eq!(
            notification_ids(&event, "thread-native").unwrap(),
            NativeIds {
                thread_id: "thread-native".into(),
                turn_id: Some("turn-native".into()),
                item_id: Some("item-native".into())
            }
        );
        assert_eq!(
            notification_ids(&event, "another-thread"),
            Err(WireError::Unexpected)
        );
        assert_eq!(
            notification_ids(
                &json!({"method":"item/reasoning/textDelta","params":{}}),
                "thread-native"
            ),
            Err(WireError::Unsupported)
        );
    }
    #[test]
    fn native_handshake_precedes_effects_and_readback_preserves_identity() {
        let (mut peer, ledger) = scripted("  print(json.dumps({'id':r['id'],'result':{'thread':{'id':'native-123'}}}),flush=True)");
        assert_eq!(peer.start_empty_thread(), Err(WireError::Unsupported));
        assert!(!ledger.exists());
        peer.initialize().unwrap();
        assert_eq!(peer.start_empty_thread().unwrap(), "native-123");
        assert_eq!(
            peer.read_thread("native-123").unwrap().thread_id,
            "native-123"
        );
        assert_eq!(
            fs::read_to_string(ledger).unwrap(),
            "initialize\ninitialized\nthread/start\nthread/read\n"
        );
    }
    #[test]
    fn native_foreign_duplicate_and_server_requests_poison_without_retry() {
        for response in [
            "{'id':999,'result':{}}",
            "{'id':1,'result':{}}",
            "{'id':r['id'],'method':'requestApproval','params':{}}",
        ] {
            let (mut peer, ledger) =
                scripted(&format!("  print(json.dumps({response}),flush=True)"));
            peer.initialize().unwrap();
            assert_eq!(peer.start_empty_thread(), Err(WireError::Unexpected));
            assert_eq!(peer.start_empty_thread(), Err(WireError::Closed));
            assert_eq!(
                fs::read_to_string(ledger)
                    .unwrap()
                    .matches("thread/start")
                    .count(),
                1
            );
        }
    }
    #[test]
    fn native_notification_backpressure_keeps_connection_unusable() {
        let (mut peer, ledger) = scripted("  for i in range(100): print(json.dumps({'method':'thread/started','params':{'thread':{'id':'native-123'}}}),flush=True)");
        peer.initialize().unwrap();
        assert!(matches!(
            peer.start_empty_thread(),
            Err(WireError::Backpressure | WireError::Closed)
        ));
        assert!(peer.notifications.len() <= MAX_PENDING);
        assert_eq!(peer.start_empty_thread(), Err(WireError::Closed));
        assert_eq!(
            fs::read_to_string(ledger)
                .unwrap()
                .matches("thread/start")
                .count(),
            1
        );
    }
    #[test]
    fn native_shutdown_and_interrupt_ack_are_distinct_from_completion() {
        let (mut peer, ledger) =
            scripted("  print(json.dumps({'id':r['id'],'result':{}}),flush=True)");
        peer.initialize().unwrap();
        peer.interrupt("thread-native", "turn-native").unwrap();
        assert!(peer.take_notification("thread-native").is_none());
        peer.close();
        assert_eq!(
            peer.interrupt("thread-native", "turn-native"),
            Err(WireError::Closed)
        );
        assert_eq!(
            fs::read_to_string(ledger)
                .unwrap()
                .matches("turn/interrupt")
                .count(),
            1
        );
    }
    #[test]
    fn native_lost_reply_never_retries_effect() {
        for _ in 0..2 {
            let (mut peer, ledger) = scripted("  sys.exit(0)");
            peer.initialize().unwrap();
            assert_eq!(peer.start_empty_thread(), Err(WireError::Closed));
            assert_eq!(peer.start_empty_thread(), Err(WireError::Closed));
            assert_eq!(
                fs::read_to_string(ledger)
                    .unwrap()
                    .matches("thread/start")
                    .count(),
                1
            );
        }
    }
    #[cfg(unix)]
    #[test]
    fn native_wrong_binary_pin_has_zero_process_effects_and_cleans_up() {
        let directory =
            std::env::temp_dir().join(format!("runspindle-bad-pin-{}", std::process::id()));
        assert!(matches!(
            StdioSession::spawn(Path::new("/bin/echo"), &directory),
            Err(WireError::Pin)
        ));
        assert!(!directory.exists());
    }
    #[test]
    fn native_malformed_launch_id_cannot_trigger_a_second_start() {
        let (mut peer, ledger) = scripted(
            "  print(json.dumps({'id':r['id'],'result':{'thread':{'id':''}}}),flush=True)",
        );
        peer.initialize().unwrap();
        assert_eq!(peer.start_empty_thread(), Err(WireError::Malformed));
        assert_eq!(peer.start_empty_thread(), Err(WireError::Closed));
        assert_eq!(
            fs::read_to_string(ledger)
                .unwrap()
                .matches("thread/start")
                .count(),
            1
        );
    }
    #[test]
    fn native_duplicate_reply_and_nested_identity_members_are_rejected() {
        for raw in [
            r#"{"id":999,"id":1,"result":{}}"#,
            r#"{"id":1,"result":{"thread":{"id":"foreign","id":"native-123"}}}"#,
            r#"{"method":"item/started","params":{"threadId":"foreign","threadId":"native-123"}}"#,
            r#"{"result":[{"id":"first","id":"second"}]}"#,
        ] {
            assert_eq!(
                read_frame(&mut Cursor::new(format!("{raw}\n"))),
                Err(WireError::Malformed)
            );
        }
        let (mut peer, ledger) = scripted("  print('{\"id\":999,\"id\":'+str(r['id'])+',\"result\":{\"thread\":{\"id\":\"native-123\"}}}',flush=True)");
        peer.initialize().unwrap();
        assert_eq!(peer.start_empty_thread(), Err(WireError::Malformed));
        assert_eq!(peer.start_empty_thread(), Err(WireError::Closed));
        assert_eq!(
            fs::read_to_string(ledger)
                .unwrap()
                .matches("thread/start")
                .count(),
            1
        );
    }
}
