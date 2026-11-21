//! Pinned metadata-only app-server stdio seam. No turn submission or account access.
//! Native wire loss never authorizes a repeated thread/start.
use serde::{
    de::{self, MapAccess, SeqAccess, Visitor},
    Deserialize, Deserializer,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::VecDeque;
use std::fs;
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

// Resource bound, not a new executable pin or platform qualification.
#[cfg(unix)]
const MAX_BINARY_BYTES: u64 = 512 * 1024 * 1024;

/// Copy only a bounded regular file into a fresh caller-owned directory. The
/// descriptor, not a path precheck, rejects FIFO/device and final-link inputs.
#[cfg(unix)]
fn acquire_binary(source: &Path, destination: &Path, expected: &str) -> Result<(), WireError> {
    use std::fs::OpenOptions;
    use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
    let mut input = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(source)
        .map_err(|_| WireError::Io)?;
    let metadata = input.metadata().map_err(|_| WireError::Io)?;
    if !metadata.is_file() {
        return Err(WireError::Pin);
    }
    let size = metadata.len();
    if size == 0 || size > MAX_BINARY_BYTES {
        return Err(WireError::Oversized);
    }
    let mut output = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(destination)
        .map_err(|_| WireError::Io)?;
    let result = (|| {
        copy_exact_binary(&mut input, &mut output, size)?;
        use std::io::{Seek, SeekFrom};
        output.seek(SeekFrom::Start(0)).map_err(|_| WireError::Io)?;
        let mut hash = Sha256::new();
        let mut buffer = [0u8; 65536];
        let mut remaining = size;
        while remaining > 0 {
            let count = remaining.min(buffer.len() as u64) as usize;
            output
                .read_exact(&mut buffer[..count])
                .map_err(|_| WireError::Io)?;
            hash.update(&buffer[..count]);
            remaining -= count as u64;
        }
        if format!("{:x}", hash.finalize()) != expected {
            return Err(WireError::Pin);
        }
        output
            .set_permissions(fs::Permissions::from_mode(0o500))
            .map_err(|_| WireError::Io)?;
        Ok(())
    })();
    drop(output);
    if result.is_err() {
        let _ = fs::remove_file(destination);
    }
    result
}

#[cfg(unix)]
fn copy_exact_binary(
    input: &mut impl Read,
    output: &mut impl Write,
    size: u64,
) -> Result<(), WireError> {
    let mut remaining = size;
    let mut buffer = [0u8; 65536];
    while remaining > 0 {
        let count = remaining.min(buffer.len() as u64) as usize;
        input
            .read_exact(&mut buffer[..count])
            .map_err(|_| WireError::Io)?;
        output
            .write_all(&buffer[..count])
            .map_err(|_| WireError::Io)?;
        remaining -= count as u64;
    }
    let mut extra = [0];
    if input.read(&mut extra).map_err(|_| WireError::Io)? != 0 {
        return Err(WireError::Oversized);
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WireError {
    Pin,
    Io,
    /// Startup failed and owned-child or I/O shutdown could not be confirmed.
    ShutdownUnknown,
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

pub(crate) fn unique_json(bytes: &[u8]) -> Result<Value, WireError> {
    serde_json::from_slice::<UniqueValue>(bytes)
        .map(|v| v.0)
        .map_err(|_| WireError::Malformed)
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
    if value.get("result").is_some() || value.get("error").is_some() {
        return Err(WireError::Malformed);
    }
    let p = &value["params"];
    if !p.is_object() {
        return Err(WireError::Malformed);
    }
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
/// Sanitized native notification, preserving only protocol identity and terminal status.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NativeNotice {
    ThreadStarted {
        thread_id: String,
    },
    TurnStarted {
        thread_id: String,
        turn_id: String,
    },
    TurnCompleted {
        thread_id: String,
        turn_id: String,
        terminal: crate::lifecycle::Terminal,
    },
    ItemStarted {
        thread_id: String,
        turn_id: String,
        item_id: String,
    },
    ItemCompleted {
        thread_id: String,
        turn_id: String,
        item_id: String,
    },
}
impl NativeNotice {
    fn into_ids(self) -> NativeIds {
        let (thread_id, turn_id, item_id) = match self {
            Self::ThreadStarted { thread_id } => (thread_id, None, None),
            Self::TurnStarted { thread_id, turn_id }
            | Self::TurnCompleted {
                thread_id, turn_id, ..
            } => (thread_id, Some(turn_id), None),
            Self::ItemStarted {
                thread_id,
                turn_id,
                item_id,
            }
            | Self::ItemCompleted {
                thread_id,
                turn_id,
                item_id,
            } => (thread_id, Some(turn_id), Some(item_id)),
        };
        NativeIds {
            thread_id,
            turn_id,
            item_id,
        }
    }
    pub fn thread_id(&self) -> &str {
        match self {
            Self::ThreadStarted { thread_id }
            | Self::TurnStarted { thread_id, .. }
            | Self::TurnCompleted { thread_id, .. }
            | Self::ItemStarted { thread_id, .. }
            | Self::ItemCompleted { thread_id, .. } => thread_id,
        }
    }
}
fn notice(value: &Value, thread: &str) -> Result<NativeNotice, WireError> {
    let ids = notification_ids(value, thread)?;
    Ok(match value["method"].as_str() {
        Some("thread/started") => NativeNotice::ThreadStarted {
            thread_id: ids.thread_id,
        },
        Some("turn/started") if value["params"]["turn"]["status"] == "inProgress" => {
            NativeNotice::TurnStarted {
                thread_id: ids.thread_id,
                turn_id: ids.turn_id.ok_or(WireError::Malformed)?,
            }
        }
        Some("turn/completed") => {
            let terminal = match value["params"]["turn"]["status"].as_str() {
                Some("completed") => crate::lifecycle::Terminal::Completed,
                Some("failed") => crate::lifecycle::Terminal::Failed,
                Some("interrupted") => crate::lifecycle::Terminal::Cancelled,
                _ => return Err(WireError::Malformed),
            };
            NativeNotice::TurnCompleted {
                thread_id: ids.thread_id,
                turn_id: ids.turn_id.ok_or(WireError::Malformed)?,
                terminal,
            }
        }
        Some("item/started") => NativeNotice::ItemStarted {
            thread_id: ids.thread_id,
            turn_id: ids.turn_id.ok_or(WireError::Malformed)?,
            item_id: ids.item_id.ok_or(WireError::Malformed)?,
        },
        Some("item/completed") => NativeNotice::ItemCompleted {
            thread_id: ids.thread_id,
            turn_id: ids.turn_id.ok_or(WireError::Malformed)?,
            item_id: ids.item_id.ok_or(WireError::Malformed)?,
        },
        _ => return Err(WireError::Unsupported),
    })
}

/// Owned OS child status only; never native task completion or cancellation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChildExit {
    Exited { success: bool, code: Option<i32> },
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShutdownReceipt {
    pub process_id: u32,
    pub termination_requested: bool,
    pub kill_signal_sent: bool,
    pub child_exit: ChildExit,
    pub io_threads_stopped: bool,
}

fn observe_exit(child: &mut Child, deadline: Instant) -> ChildExit {
    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                return ChildExit::Exited {
                    success: status.success(),
                    code: status.code(),
                };
            }
            Err(_) => return ChildExit::Unknown,
            Ok(None) => {}
        }
        if Instant::now() >= deadline {
            return ChildExit::Unknown;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
}

fn cleanup_permitted(receipt: ShutdownReceipt) -> bool {
    matches!(receipt.child_exit, ChildExit::Exited { .. }) && receipt.io_threads_stopped
}

type WriteRequest = (Vec<u8>, mpsc::Sender<Result<(), WireError>>);

fn receive_before<T>(
    receiver: &Receiver<T>,
    deadline: Instant,
    now: impl Fn() -> Instant,
) -> Result<T, mpsc::RecvTimeoutError> {
    // A ready channel can still return a message after its receiver's timeout
    // has elapsed. Check admission to the wait and acceptance of its result.
    let remaining = deadline
        .checked_duration_since(now())
        .filter(|duration| !duration.is_zero())
        .ok_or(mpsc::RecvTimeoutError::Timeout)?;
    let value = receiver.recv_timeout(remaining)?;
    if now() >= deadline {
        return Err(mpsc::RecvTimeoutError::Timeout);
    }
    Ok(value)
}

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
    io_threads: Vec<std::thread::JoinHandle<()>>,
    shutdown_receipt: Option<ShutdownReceipt>,
    telemetry: Option<Arc<crate::telemetry::Buffer>>,
}
impl StdioSession {
    /// Opt-in local metadata buffer; never grants authority or installs an exporter.
    pub fn set_telemetry(&mut self, buffer: Option<Arc<crate::telemetry::Buffer>>) {
        self.telemetry = buffer;
    }
    fn observe(
        &self,
        operation: crate::telemetry::Operation,
        outcome: crate::telemetry::Outcome,
        started: std::time::SystemTime,
    ) {
        if let Some(buffer) = &self.telemetry {
            buffer.record(crate::telemetry::Observation {
                operation,
                outcome,
                started,
                ended: std::time::SystemTime::now(),
            });
        }
    }
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
        // Preparation failures have no child. Once a child exists, its owning
        // session alone decides whether observed shutdown permits cleanup.
        match Self::spawn_child(binary, directory) {
            Ok(child) => Self::from_child(child, directory.to_owned()),
            Err(error) => {
                let _ = fs::remove_dir_all(directory);
                Err(error)
            }
        }
    }
    #[cfg(unix)]
    fn spawn_child(binary: &Path, directory: &Path) -> Result<Child, WireError> {
        if std::env::consts::OS != "macos" || std::env::consts::ARCH != "aarch64" {
            return Err(WireError::Pin);
        }
        let copy = directory.join("codex");
        acquire_binary(binary, &copy, BINARY_SHA256)?;
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
        Ok(child)
    }
    fn from_child(child: Child, directory: PathBuf) -> Result<Self, WireError> {
        let (session, result) = Self::configure_child(child, directory, |worker| {
            std::thread::Builder::new().spawn(worker)
        });
        result.map(|()| session)
    }
    // Establish ownership before taking pipes or starting workers. Returning the
    // owned session even on error keeps cleanup in the same Drop/receipt path.
    fn configure_child(
        child: Child,
        directory: PathBuf,
        mut spawn_worker: impl FnMut(
            Box<dyn FnOnce() + Send>,
        ) -> std::io::Result<std::thread::JoinHandle<()>>,
    ) -> (Self, Result<(), WireError>) {
        let (writer, requests) = mpsc::sync_channel::<WriteRequest>(1);
        let (sender, incoming) = mpsc::sync_channel(MAX_PENDING);
        let reader_failed = Arc::new(AtomicBool::new(false));
        let mut session = Self {
            child,
            input: Some(writer),
            incoming,
            reader_failed,
            notifications: VecDeque::new(),
            next_id: 1,
            initialized: false,
            closed: false,
            directory,
            io_threads: Vec::new(),
            shutdown_receipt: None,
            telemetry: None,
        };
        let result = (|| {
            // Validate both pipe handles before starting either worker.
            let mut stdin = session.child.stdin.take().ok_or(WireError::Io)?;
            let output = session.child.stdout.take().ok_or(WireError::Io)?;
            let writer_thread = spawn_worker(Box::new(move || {
                while let Ok((bytes, done)) = requests.recv() {
                    let result = stdin.write_all(&bytes).map_err(|_| WireError::Io);
                    let failed = result.is_err();
                    let _ = done.send(result);
                    if failed {
                        break;
                    }
                }
            }))
            .map_err(|_| WireError::Io)?;
            session.io_threads.push(writer_thread);
            let failed_reader = Arc::clone(&session.reader_failed);
            let reader_thread = spawn_worker(Box::new(move || {
                let mut reader = BufReader::new(output);
                loop {
                    let frame = read_frame(&mut reader);
                    let failed = frame.is_err();
                    if sender.try_send(frame).is_err() || failed {
                        failed_reader.store(true, Ordering::Release);
                        break;
                    }
                }
            }))
            .map_err(|_| WireError::Io)?;
            session.io_threads.push(reader_thread);
            Ok(())
        })();
        let result = if result.is_err() {
            let receipt = session.close_observed();
            if !cleanup_permitted(receipt) {
                Err(WireError::ShutdownUnknown)
            } else {
                result
            }
        } else {
            result
        };
        (session, result)
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
        let deadline = Instant::now() + DEADLINE;
        self.input
            .as_ref()
            .ok_or(WireError::Closed)?
            .try_send((bytes, done))
            .map_err(|_| WireError::Backpressure)?;
        receive_before(&completion, deadline, Instant::now).map_err(|_| WireError::Timeout)?
    }
    fn rpc(&mut self, method: &str, params: Value) -> Result<Value, WireError> {
        let started = std::time::SystemTime::now();
        let result = self.rpc_inner(method, params);
        self.observe(
            crate::telemetry::Operation::rpc(method),
            if result.is_ok() {
                crate::telemetry::Outcome::Acknowledged
            } else {
                crate::telemetry::Outcome::Error
            },
            started,
        );
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
            let frame =
                receive_before(&self.incoming, deadline, Instant::now).map_err(|e| match e {
                    mpsc::RecvTimeoutError::Timeout => WireError::Timeout,
                    _ => WireError::Closed,
                })??;
            // A reply queued before the reader poisoned the stream is not a
            // fresh success. This observes the flag here, not future failures.
            if self.reader_failed.load(Ordering::Acquire) {
                return Err(WireError::Closed);
            }
            if frame.get("id").is_some() {
                if frame["id"] != request_id || frame.get("method").is_some() {
                    return Err(WireError::Unexpected);
                }
                // An ambiguous or malformed error must not become an authoritative
                // rejection. No peer error content escapes this metadata boundary.
                let result = frame.get("result");
                let error = frame.get("error");
                if result.is_some() == error.is_some() || frame.get("params").is_some() {
                    return Err(WireError::Malformed);
                }
                if let Some(error) = error {
                    if error["code"].as_i64().is_none() || !error["message"].is_string() {
                        return Err(WireError::Malformed);
                    }
                    return Err(WireError::Rejected);
                }
                return result.cloned().ok_or(WireError::Malformed);
            }
            if !frame["method"].is_string()
                || !frame["params"].is_object()
                || frame.get("result").is_some()
                || frame.get("error").is_some()
            {
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
            .and_then(|agent| agent.split(' ').next())
            .and_then(|product| product.split_once('/'))
            .is_some_and(|(name, version)| {
                !name.is_empty()
                    && name.bytes().all(|byte| byte.is_ascii_graphic())
                    && version == "0.169.0"
            })
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
        let buffered = self.notifications.len();
        let reply = self.rpc(
            "thread/start",
            json!({"ephemeral":true,"approvalPolicy":"never","sandbox":"read-only"}),
        )?;
        let identity = id(&reply["thread"]["id"]).and_then(|thread| {
            // Before binding the reply, check thread identities observed while
            // awaiting it. Earlier queued notices belong to earlier exchanges.
            for frame in self.notifications.iter().skip(buffered) {
                if frame["method"] == "thread/started" {
                    notification_ids(frame, &thread)?;
                }
            }
            Ok(thread)
        });
        match identity {
            Ok(value) => Ok(value),
            Err(error) => {
                self.close();
                Err(error)
            }
        }
    }
    /// Crate-internal task effect, reached through the separately authorized bridge.
    pub(crate) fn start_text_turn(
        &mut self,
        thread: &str,
        text: &str,
        model: &str,
    ) -> Result<String, WireError> {
        if !self.initialized {
            return Err(WireError::Unsupported);
        }
        id(&json!(thread))?;
        id(&json!(model))?;
        let reply = self.rpc(
            "turn/start",
            json!({
                "threadId":thread,
                "input":[{"type":"text","text":text,"text_elements":[]}],
                "model":model,
                "approvalPolicy":"never",
                "sandboxPolicy":{"type":"readOnly","networkAccess":false}
            }),
        )?;
        let result = id(&reply["turn"]["id"]);
        if result.is_err() {
            self.close();
        }
        result
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
    pub(crate) fn interrupt(&mut self, thread: &str, turn: &str) -> Result<(), WireError> {
        if !self.initialized {
            return Err(WireError::Unsupported);
        }
        id(&json!(thread))?;
        id(&json!(turn))?;
        let reply = self.rpc("turn/interrupt", json!({"threadId":thread,"turnId":turn}))?;
        // The pinned TurnInterruptResponse schema is an object. Other JSON
        // values are not acknowledgement, and never establish task completion.
        if !reply.is_object() {
            self.close();
            return Err(WireError::Malformed);
        }
        Ok(())
    }
    pub fn take_notification(&mut self, thread: &str) -> Option<Result<NativeIds, WireError>> {
        // Both public consumers use the same strict parser and poisoned-stream
        // behavior; asking for IDs alone cannot bypass notification validation.
        self.next_notice(thread)
            .map(|result| result.map(NativeNotice::into_ids))
    }
    /// Drain only sanitized notifications; unsolicited replies poison the stream.
    pub fn next_notice(&mut self, thread: &str) -> Option<Result<NativeNotice, WireError>> {
        if self.closed || self.reader_failed.load(Ordering::Acquire) {
            self.close();
            return Some(Err(WireError::Closed));
        }
        let frame = if let Some(value) = self.notifications.pop_front() {
            Ok(value)
        } else {
            match self.incoming.try_recv() {
                Ok(value) => value,
                Err(mpsc::TryRecvError::Empty) => return None,
                Err(_) => Err(WireError::Closed),
            }
        };
        let result = frame.and_then(|value| notice(&value, thread));
        if result.is_err() {
            self.close();
        }
        Some(result)
    }
    /// Stop only the child this session spawned. Unknown outcomes stay unknown;
    /// repeated calls return the same receipt rather than retrying termination.
    pub fn close_observed(&mut self) -> ShutdownReceipt {
        self.closed = true;
        self.input.take();
        if let Some(receipt) = self.shutdown_receipt {
            return receipt;
        }
        let started = std::time::SystemTime::now();
        let deadline = Instant::now() + DEADLINE;
        let mut receipt = ShutdownReceipt {
            process_id: self.child.id(),
            termination_requested: false,
            kill_signal_sent: false,
            child_exit: observe_exit(&mut self.child, Instant::now()),
            io_threads_stopped: false,
        };
        if receipt.child_exit == ChildExit::Unknown {
            receipt.termination_requested = true;
            receipt.kill_signal_sent = self.child.kill().is_ok();
            receipt.child_exit = observe_exit(&mut self.child, deadline);
        }
        while !self.io_threads.iter().all(|thread| thread.is_finished())
            && Instant::now() < deadline
        {
            std::thread::sleep(Duration::from_millis(5));
        }
        receipt.io_threads_stopped = self.io_threads.iter().all(|thread| thread.is_finished());
        if receipt.io_threads_stopped {
            for thread in self.io_threads.drain(..) {
                if thread.join().is_err() {
                    receipt.io_threads_stopped = false;
                }
            }
        }
        self.shutdown_receipt = Some(receipt);
        self.observe(
            crate::telemetry::Operation::Shutdown,
            if cleanup_permitted(receipt) {
                crate::telemetry::Outcome::Observed
            } else {
                crate::telemetry::Outcome::Unknown
            },
            started,
        );
        receipt
    }

    pub fn close(&mut self) {
        let _ = self.close_observed();
    }
}
impl Drop for StdioSession {
    fn drop(&mut self) {
        let receipt = self.close_observed();
        // Never remove evidence while child exit or local I/O shutdown is unknown.
        if cleanup_permitted(receipt) {
            let _ = fs::remove_dir_all(&self.directory);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use std::sync::atomic::{AtomicU64, Ordering};
    static SERIAL: AtomicU64 = AtomicU64::new(0);
    #[test]
    fn telemetry_observes_scripted_rpc_without_retaining_payloads() {
        use crate::telemetry::{Buffer, Operation, Outcome};
        let (mut peer, _) = scripted("  print(json.dumps({'id':r['id'],'result':{'thread':{'id':'PRIVATE_ID_CANARY'}}}),flush=True)");
        let buffer = Arc::new(Buffer::default());
        peer.set_telemetry(Some(buffer.clone()));
        peer.initialize().unwrap();
        assert_eq!(peer.start_empty_thread().unwrap(), "PRIVATE_ID_CANARY");
        let receipt = peer.close_observed();
        assert!(cleanup_permitted(receipt));
        peer.close_observed(); // Retained shutdown does not invent another stop event.
        let records = buffer.drain();
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].operation, Operation::Handshake);
        assert_eq!(records[1].outcome, Outcome::Acknowledged);
        assert_eq!(records[2].operation, Operation::Shutdown);
        assert_eq!(records[2].outcome, Outcome::Observed);
        assert!(!format!("{records:?}").contains("PRIVATE_ID_CANARY"));
    }

    #[test]
    fn telemetry_full_buffer_does_not_change_native_outcome() {
        use crate::telemetry::{Buffer, Observation, Operation, Outcome, CAPACITY};
        let (mut peer, _) = scripted("  pass");
        let buffer = Arc::new(Buffer::default());
        for _ in 0..CAPACITY {
            buffer.record(Observation {
                operation: Operation::OtherRpc,
                outcome: Outcome::Error,
                started: std::time::SystemTime::now(),
                ended: std::time::SystemTime::now(),
            });
        }
        peer.set_telemetry(Some(buffer.clone()));
        peer.initialize().unwrap();
        assert!(cleanup_permitted(peer.close_observed()));
        assert_eq!(buffer.dropped(), 2);
    }

    #[cfg(unix)]
    #[test]
    fn native_binary_acquisition_rejects_special_files_and_preserves_existing_destination() {
        use std::os::unix::fs::{symlink, PermissionsExt};
        let dir = std::env::temp_dir().join(format!(
            "host-acquire-{}-{}",
            std::process::id(),
            SERIAL.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&dir).unwrap();
        let source = dir.join("source");
        let target = dir.join("target");
        fs::write(&source, b"fixture executable, never launched").unwrap();
        let digest = format!("{:x}", Sha256::digest(fs::read(&source).unwrap()));
        acquire_binary(&source, &target, &digest).unwrap();
        assert_eq!(fs::read(&target).unwrap(), fs::read(&source).unwrap());
        assert_eq!(
            fs::metadata(&target).unwrap().permissions().mode() & 0o777,
            0o500
        );
        assert_eq!(
            acquire_binary(&source, &target, &digest),
            Err(WireError::Io)
        );
        assert_eq!(fs::read(&target).unwrap(), fs::read(&source).unwrap());
        fs::remove_file(&target).unwrap();
        assert_eq!(
            acquire_binary(&source, &target, "wrong pin"),
            Err(WireError::Pin)
        );
        assert!(!target.exists());
        let link = dir.join("link");
        symlink(&source, &link).unwrap();
        assert_eq!(acquire_binary(&link, &target, &digest), Err(WireError::Io));
        assert_eq!(acquire_binary(&dir, &target, &digest), Err(WireError::Pin));
        let fifo = dir.join("fifo");
        assert!(Command::new("mkfifo")
            .arg(&fifo)
            .status()
            .unwrap()
            .success());
        let start = Instant::now();
        assert_eq!(acquire_binary(&fifo, &target, &digest), Err(WireError::Pin));
        assert!(start.elapsed() < Duration::from_secs(1));
        assert!(!target.exists());
        let large = fs::File::create(dir.join("large")).unwrap();
        large.set_len(MAX_BINARY_BYTES + 1).unwrap();
        assert_eq!(
            acquire_binary(&dir.join("large"), &target, &digest),
            Err(WireError::Oversized)
        );
        fs::write(&source, b"").unwrap();
        assert_eq!(
            acquire_binary(&source, &target, &digest),
            Err(WireError::Oversized)
        );
        assert!(!target.exists());
        fs::remove_dir_all(dir).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn native_binary_copy_is_exact_and_growth_is_bounded() {
        let mut output = Vec::new();
        assert_eq!(
            copy_exact_binary(&mut Cursor::new(b"ab"), &mut output, 3),
            Err(WireError::Io)
        );
        output.clear();
        assert_eq!(
            copy_exact_binary(&mut std::io::repeat(42), &mut output, 17),
            Err(WireError::Oversized)
        );
        assert_eq!(output, vec![42; 17]);
        output.clear();
        copy_exact_binary(&mut Cursor::new(b"abc"), &mut output, 3).unwrap();
        assert_eq!(output, b"abc");
        assert_eq!(
            copy_exact_binary(&mut Cursor::new(b"abc"), &mut std::io::sink(), 2),
            Err(WireError::Oversized)
        );
    }

    fn scripted(behavior: &str) -> (StdioSession, PathBuf) {
        scripted_user_agent(behavior, "codex/0.169.0")
    }

    fn scripted_user_agent(behavior: &str, user_agent: &str) -> (StdioSession, PathBuf) {
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
  print(json.dumps({{'id':r['id'],'result':{{'userAgent':sys.argv[2]}}}}),flush=True)
 elif r['method']=='initialized': pass
 else:
{behavior}
"#
        );
        let child = Command::new("python3")
            .args(["-u", "-c", &script])
            .arg(&ledger)
            .arg(user_agent)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        (StdioSession::from_child(child, directory).unwrap(), ledger)
    }

    #[test]
    fn native_handshake_requires_exact_build_version_in_leading_product() {
        for agent in [
            "codex/10.169.0",
            "codex/0.169.01",
            "codex/0.169.0-alpha.1",
            "codex/0.169.1 (synthetic target 0.169.0)",
            "codex/0.169.1 other/0.169.0",
            "0.169.0",
            "/0.169.0",
        ] {
            let (mut peer, ledger) = scripted_user_agent("  pass", agent);
            assert_eq!(peer.initialize(), Err(WireError::Pin), "{agent}");
            assert_eq!(peer.start_empty_thread(), Err(WireError::Unsupported));
            assert_eq!(peer.initialize(), Err(WireError::Closed));
            assert_eq!(fs::read_to_string(ledger).unwrap(), "initialize\n");
        }
        let (mut peer, ledger) = scripted_user_agent(
            "  print(json.dumps({'id':r['id'],'result':{'thread':{'id':'native-123'}}}),flush=True)",
            "runspindle-host/0.169.0 (Synthetic OS 99.0; arm64) unknown",
        );
        assert_eq!(peer.initialize(), Ok(()));
        assert_eq!(peer.start_empty_thread(), Ok("native-123".into()));
        assert_eq!(
            fs::read_to_string(ledger).unwrap(),
            "initialize\ninitialized\nthread/start\n"
        );
    }
    #[test]
    fn native_owned_child_exit_is_observed_and_repeated_close_is_idempotent() {
        let (mut peer, ledger) =
            scripted("  print(json.dumps({'id':r['id'],'result':{}}),flush=True)");
        peer.initialize().unwrap();
        // The response fences the preceding initialized notification and its ledger write.
        peer.rpc("fixture/barrier", json!({})).unwrap();
        let pid = peer.child.id();
        let before = fs::read_to_string(&ledger).unwrap();
        let started = Instant::now();
        let receipt = peer.close_observed();
        assert!(started.elapsed() < Duration::from_secs(7));
        assert_eq!(receipt.process_id, pid);
        assert!(matches!(receipt.child_exit, ChildExit::Exited { .. }));
        assert!(receipt.io_threads_stopped);
        // Read the owned OS handle independently of the returned receipt.
        assert!(peer.child.try_wait().unwrap().is_some());
        assert_eq!(peer.close_observed(), receipt);
        assert_eq!(peer.start_empty_thread(), Err(WireError::Closed));
        assert_eq!(fs::read_to_string(&ledger).unwrap(), before);
        let directory = peer.directory.clone();
        drop(peer);
        assert!(!directory.exists());
    }

    #[test]
    fn native_unresponsive_child_is_terminated_not_reported_as_task_completion() {
        let (mut peer, ledger) = scripted(
            "  import time\n  with open(sys.argv[1]+'.blocked','w') as f: f.write('entered')\n  time.sleep(60)",
        );
        peer.initialize().unwrap();
        // Request a method that blocks in the child, without waiting for its reply.
        peer.send(json!({"id": 90, "method": "fixture/block", "params": {}}))
            .unwrap();
        let marker = ledger.with_file_name("calls.blocked");
        let entry_deadline = Instant::now() + DEADLINE;
        while fs::read_to_string(&marker).ok().as_deref() != Some("entered") {
            assert!(
                Instant::now() < entry_deadline,
                "child did not enter blocking path"
            );
            assert!(peer.child.try_wait().unwrap().is_none());
            std::thread::sleep(Duration::from_millis(5));
        }
        assert!(peer.child.try_wait().unwrap().is_none());
        let receipt = peer.close_observed();
        assert!(receipt.termination_requested);
        assert!(receipt.kill_signal_sent);
        assert!(matches!(receipt.child_exit, ChildExit::Exited { .. }));
        assert!(peer.child.try_wait().unwrap().is_some());
        assert_eq!(
            peer.interrupt("thread-native", "turn-native"),
            Err(WireError::Closed)
        );
    }

    #[test]
    fn native_unknown_exit_observation_never_authorizes_directory_cleanup() {
        let (mut peer, ledger) = scripted("  pass");
        peer.initialize().unwrap();
        assert_eq!(
            observe_exit(&mut peer.child, Instant::now()),
            ChildExit::Unknown
        );
        let directory = peer.directory.clone();
        // Fault injection affects only the retained receipt, never leaves a child alive.
        peer.child.kill().unwrap();
        assert!(matches!(
            observe_exit(&mut peer.child, Instant::now() + DEADLINE),
            ChildExit::Exited { .. }
        ));
        let mut receipt = peer.close_observed();
        receipt.child_exit = ChildExit::Unknown;
        peer.shutdown_receipt = Some(receipt);
        drop(peer);
        assert!(directory.is_dir());
        assert!(ledger.is_file());
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn native_unfinished_io_prevents_cleanup_even_after_child_exit() {
        let (mut peer, ledger) = scripted("  pass");
        peer.initialize().unwrap();
        let mut receipt = peer.close_observed();
        assert!(matches!(receipt.child_exit, ChildExit::Exited { .. }));
        receipt.io_threads_stopped = false;
        peer.shutdown_receipt = Some(receipt);
        let directory = peer.directory.clone();
        drop(peer);
        assert!(ledger.is_file());
        fs::remove_dir_all(directory).unwrap();
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
        assert_eq!(peer.next_notice("native-123"), Some(Err(WireError::Closed)));
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

    #[test]
    fn native_wait_does_not_consume_a_reply_after_the_deadline() {
        let (send, receive) = mpsc::channel();
        let reply = json!({"id": 2, "result": {"thread": {"id": "native-thread"}}});
        send.send(reply.clone()).unwrap();
        let deadline = Instant::now();

        assert_eq!(
            receive_before(&receive, deadline, || deadline),
            Err(mpsc::RecvTimeoutError::Timeout)
        );
        assert_eq!(receive.try_recv().unwrap(), reply);
    }

    #[test]
    fn native_wait_checks_the_deadline_again_after_dequeuing() {
        let (send, receive) = mpsc::channel();
        send.send(Ok::<(), WireError>(())).unwrap();
        let before = Instant::now();
        let deadline = before + DEADLINE;
        let clock = std::cell::Cell::new(before);

        assert_eq!(
            receive_before(&receive, deadline, || clock.replace(deadline)),
            Err(mpsc::RecvTimeoutError::Timeout)
        );
        assert_eq!(receive.try_recv(), Err(mpsc::TryRecvError::Empty));

        send.send(Ok(())).unwrap();
        assert_eq!(receive_before(&receive, deadline, || before), Ok(Ok(())));
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
    #[test]
    fn native_poisoned_stream_never_exposes_queued_identity_after_close() {
        let (mut peer, _) = scripted("  print(json.dumps({'method':'turn/started','params':{'threadId':'native-123','turn':{'id':'native-turn','status':'inProgress'}}}),flush=True)\n  print(json.dumps({'id':r['id'],'result':{'thread':{'id':'native-123'}}}),flush=True)");
        peer.initialize().unwrap();
        peer.start_empty_thread().unwrap();
        assert!(!peer.notifications.is_empty());
        peer.close();
        assert_eq!(peer.next_notice("native-123"), Some(Err(WireError::Closed)));
        assert_eq!(
            peer.take_notification("native-123"),
            Some(Err(WireError::Closed))
        );
    }
    #[test]
    fn native_ambiguous_and_malformed_replies_never_acknowledge_or_retry() {
        for response in [
            "{'id':r['id'],'result':{},'error':{'code':-1,'message':'synthetic'}}",
            "{'id':r['id']}",
            "{'id':r['id'],'error':None}",
            "{'id':r['id'],'error':{'code':'-1','message':'synthetic'}}",
            "{'id':r['id'],'error':{'code':-1,'message':7}}",
            "{'id':r['id'],'result':{},'params':{}}",
        ] {
            let (mut peer, ledger) =
                scripted(&format!("  print(json.dumps({response}),flush=True)"));
            peer.initialize().unwrap();
            assert_eq!(
                peer.interrupt("thread-native", "turn-native"),
                Err(WireError::Malformed)
            );
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
    }
    #[test]
    fn native_interrupt_acknowledgement_requires_pinned_object_response() {
        for response in ["None", "False", "[]", "7", "'synthetic'"] {
            let (mut peer, ledger) = scripted(&format!(
                "  print(json.dumps({{'id':r['id'],'result':{response}}}),flush=True)"
            ));
            peer.initialize().unwrap();
            assert_eq!(
                peer.interrupt("thread-native", "turn-native"),
                Err(WireError::Malformed)
            );
            assert_eq!(peer.start_empty_thread(), Err(WireError::Closed));
            let calls = fs::read_to_string(ledger).unwrap();
            assert_eq!(calls.matches("turn/interrupt").count(), 1);
            assert!(!calls.contains("thread/start"));
        }
    }
    #[test]
    fn native_valid_rejection_is_sanitized_and_poisoned() {
        let (mut peer, ledger) = scripted(
            "  print(json.dumps({'id':r['id'],'error':{'code':-32000,'message':'synthetic-secret-canary','data':{'private':'synthetic-only'}}}),flush=True)"
        );
        peer.initialize().unwrap();
        let error = peer.interrupt("thread-native", "turn-native").unwrap_err();
        assert_eq!(error, WireError::Rejected);
        assert!(!format!("{error:?}").contains("synthetic-secret-canary"));
        assert_eq!(peer.start_empty_thread(), Err(WireError::Closed));
        assert_eq!(
            fs::read_to_string(ledger)
                .unwrap()
                .matches("turn/interrupt")
                .count(),
            1
        );
    }
    #[test]
    fn native_start_rejects_conflicting_or_malformed_thread_notices() {
        for (thread, error) in [
            ("{'id':'wrong-thread'}", WireError::Unexpected),
            ("{'id':None}", WireError::Malformed),
        ] {
            let (mut peer, ledger) = scripted(&format!(
                "  print(json.dumps({{'method':'thread/started','params':{{'thread':{thread}}}}}),flush=True)\n  print(json.dumps({{'id':r['id'],'result':{{'thread':{{'id':'native-123'}}}}}}),flush=True)"
            ));
            peer.initialize().unwrap();
            assert_eq!(peer.start_empty_thread(), Err(error));
            assert_eq!(peer.read_thread("native-123"), Err(WireError::Closed));
            assert_eq!(peer.start_empty_thread(), Err(WireError::Closed));
            assert_eq!(peer.next_notice("native-123"), Some(Err(WireError::Closed)));
            assert_eq!(
                fs::read_to_string(ledger).unwrap(),
                "initialize\ninitialized\nthread/start\n"
            );
        }
    }
    #[test]
    fn native_start_preserves_thread_notices_from_earlier_exchanges() {
        let (mut peer, _) = scripted(
            "  thread='native-one' if r['id']==2 else 'native-two'\n  print(json.dumps({'method':'thread/started','params':{'thread':{'id':thread}}}),flush=True)\n  print(json.dumps({'id':r['id'],'result':{'thread':{'id':thread}}}),flush=True)",
        );
        peer.initialize().unwrap();
        assert_eq!(peer.start_empty_thread().unwrap(), "native-one");
        assert_eq!(peer.start_empty_thread().unwrap(), "native-two");
        for thread in ["native-one", "native-two"] {
            assert_eq!(
                peer.next_notice(thread),
                Some(Ok(NativeNotice::ThreadStarted {
                    thread_id: thread.into(),
                }))
            );
        }
        assert!(peer.next_notice("native-two").is_none());
    }
    #[test]
    fn native_identity_only_consumer_cannot_bypass_poisoned_notice_validation() {
        for notice in [
            "{'method':'item/started','params':{'threadId':'foreign','turnId':'turn-native','item':{'id':'item-native'}}}",
            "{'method':'turn/started','params':{'threadId':'native-123','turn':{'id':'turn-native','status':'invalid'}}}",
            "{'method':'unsupported','params':{}}",
        ] {
            let (mut peer, ledger) = scripted(&format!(
                "  print(json.dumps({notice}),flush=True)\n  print(json.dumps({{'id':r['id'],'result':{{'thread':{{'id':'native-123'}}}}}}),flush=True)"
            ));
            peer.initialize().unwrap();
            peer.start_empty_thread().unwrap();
            assert!(matches!(peer.take_notification("native-123"), Some(Err(_))));
            assert_eq!(peer.start_empty_thread(), Err(WireError::Closed));
            assert_eq!(peer.next_notice("native-123"), Some(Err(WireError::Closed)));
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
    fn native_hybrid_notifications_cannot_be_queued_or_decoded() {
        for field in ["result", "error"] {
            let notice =
                json!({"method":"thread/started","params":{"thread":{"id":"native-123"}},field:{}});
            assert_eq!(
                notification_ids(&notice, "native-123"),
                Err(WireError::Malformed)
            );
            let (mut peer, ledger) = scripted(&format!(
                "  print({:?},flush=True)\n  print(json.dumps({{'id':r['id'],'result':{{'thread':{{'id':'native-123'}}}}}}),flush=True)",
                notice.to_string()
            ));
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
    #[test]
    fn native_generic_result_null_is_present_not_a_missing_reply() {
        let (mut peer, ledger) =
            scripted("  print(json.dumps({'id':r['id'],'result':None}),flush=True)");
        peer.initialize().unwrap();
        assert_eq!(peer.rpc("fixture/nullable", json!({})), Ok(Value::Null));
        assert!(!peer.closed);
        assert_eq!(
            fs::read_to_string(ledger)
                .unwrap()
                .matches("fixture/nullable")
                .count(),
            1
        );
    }
    #[test]
    fn native_identity_consumer_preserves_validated_thread_turn_and_item() {
        let (mut peer, _) = scripted(
            "  print(json.dumps({'method':'thread/started','params':{'thread':{'id':'native-123'}}}),flush=True)\n  print(json.dumps({'method':'turn/started','params':{'threadId':'native-123','turn':{'id':'turn-native','status':'inProgress'}}}),flush=True)\n  print(json.dumps({'method':'item/started','params':{'threadId':'native-123','turnId':'turn-native','item':{'id':'item-native'}}}),flush=True)\n  print(json.dumps({'id':r['id'],'result':{'thread':{'id':'native-123'}}}),flush=True)"
        );
        peer.initialize().unwrap();
        peer.start_empty_thread().unwrap();
        for (turn, item) in [
            (None, None),
            (Some("turn-native"), None),
            (Some("turn-native"), Some("item-native")),
        ] {
            let ids = peer.take_notification("native-123").unwrap().unwrap();
            assert_eq!(ids.thread_id, "native-123");
            assert_eq!(ids.turn_id.as_deref(), turn);
            assert_eq!(ids.item_id.as_deref(), item);
        }
        assert!(peer.take_notification("native-123").is_none());
    }
    fn setup_child(piped_stdin: bool, piped_stdout: bool) -> (Child, PathBuf) {
        let directory = std::env::temp_dir().join(format!(
            "runspindle-setup-{}-{}",
            std::process::id(),
            SERIAL.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&directory).unwrap();
        let marker = directory.join("started");
        let mut child = Command::new("python3")
            .args([
                "-u",
                "-c",
                "import sys,time; open(sys.argv[1],'w').write('started'); time.sleep(60)",
            ])
            .arg(&marker)
            .stdin(if piped_stdin {
                Stdio::piped()
            } else {
                Stdio::null()
            })
            .stdout(if piped_stdout {
                Stdio::piped()
            } else {
                Stdio::null()
            })
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let deadline = Instant::now() + DEADLINE;
        while !marker.exists() {
            if Instant::now() >= deadline {
                let _ = child.kill();
                let _ = observe_exit(&mut child, Instant::now() + DEADLINE);
                panic!("synthetic setup child did not start");
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        assert!(child.try_wait().unwrap().is_none());
        (child, directory)
    }
    #[test]
    fn native_setup_worker_failures_reap_owned_child_and_join_started_worker() {
        for fail_at in [1, 2] {
            let (child, directory) = setup_child(true, true);
            let process_id = child.id();
            let finished = Arc::new(AtomicU64::new(0));
            let count = Arc::clone(&finished);
            let mut attempts = 0;
            let started = Instant::now();
            let (mut session, result) =
                StdioSession::configure_child(child, directory.clone(), |worker| {
                    attempts += 1;
                    if attempts == fail_at {
                        return Err(std::io::Error::other("synthetic worker setup failure"));
                    }
                    let count = Arc::clone(&count);
                    std::thread::Builder::new().spawn(move || {
                        worker();
                        count.fetch_add(1, Ordering::SeqCst);
                    })
                });
            assert_eq!(result, Err(WireError::Io));
            assert!(started.elapsed() < Duration::from_secs(7));
            assert_eq!(attempts, fail_at);
            assert_eq!(session.child.id(), process_id);
            // Independent owned OS handle readback, not just the shutdown receipt.
            assert!(session.child.try_wait().unwrap().is_some());
            assert_eq!(finished.load(Ordering::SeqCst), (fail_at - 1) as u64);
            let receipt = session.shutdown_receipt.unwrap();
            assert!(receipt.io_threads_stopped);
            assert!(matches!(receipt.child_exit, ChildExit::Exited { .. }));
            assert_eq!(session.close_observed(), receipt);
            assert_eq!(session.start_empty_thread(), Err(WireError::Unsupported));
            drop(session);
            assert!(!directory.exists());
        }
    }
    #[test]
    fn native_setup_missing_pipes_reaps_before_any_worker_starts() {
        for (stdin, stdout) in [(false, true), (true, false)] {
            let (child, directory) = setup_child(stdin, stdout);
            let mut attempts = 0;
            let (mut session, result) =
                StdioSession::configure_child(child, directory.clone(), |_worker| {
                    attempts += 1;
                    Err(std::io::Error::other("worker must not start"))
                });
            assert_eq!(result, Err(WireError::Io));
            assert_eq!(attempts, 0);
            assert!(session.child.try_wait().unwrap().is_some());
            assert!(session.shutdown_receipt.unwrap().io_threads_stopped);
            drop(session);
            assert!(!directory.exists());
        }
    }

    #[test]
    fn native_setup_worker_panic_reports_unknown_and_retains_directory() {
        let (child, directory) = setup_child(true, true);
        let mut attempts = 0;
        let (mut session, result) =
            StdioSession::configure_child(child, directory.clone(), |worker| {
                attempts += 1;
                if attempts == 2 {
                    return Err(std::io::Error::other("synthetic second worker failure"));
                }
                std::thread::Builder::new().spawn(move || {
                    drop(worker);
                    panic!("synthetic I/O worker panic");
                })
            });
        assert_eq!(result, Err(WireError::ShutdownUnknown));
        assert!(session.child.try_wait().unwrap().is_some());
        assert!(session.io_threads.is_empty()); // Panicked worker was joined.
        let receipt = session.shutdown_receipt.unwrap();
        assert!(!receipt.io_threads_stopped);
        assert_eq!(session.close_observed(), receipt);
        drop(session);
        assert!(directory.is_dir());
        // Test-only cleanup after independent child exit and worker-join checks.
        fs::remove_dir_all(directory).unwrap();
    }
    #[test]
    fn native_setup_public_constructor_drops_failed_owned_session() {
        let (child, directory) = setup_child(false, true);
        assert!(matches!(
            StdioSession::from_child(child, directory.clone()),
            Err(WireError::Io)
        ));
        assert!(!directory.exists());
    }

    #[test]
    fn native_observed_reader_failure_rejects_queued_success_without_retry() {
        for tail in [
            "  print('not-json',flush=True)",
            "  print('x'*65537,flush=True)",
            "  for _ in range(40): print(json.dumps({'method':'thread/started','params':{'thread':{'id':'native-123'}}}),flush=True)",
            "  sys.exit(0)",
        ] {
            let (mut peer, ledger) = scripted(&format!(
                "  print(json.dumps({{'id':r['id'],'result':{{'thread':{{'id':'native-123'}}}}}}),flush=True)
{tail}"
            ));
            peer.initialize().unwrap();
            // Delay only the write-completion acknowledgement. The real writer
            // and reader run normally; the RPC consumer cannot drain the queue
            // until the reader has independently observed the fixture failure.
            let original = peer.input.take().unwrap();
            let (relay, requests) = mpsc::sync_channel::<WriteRequest>(1);
            let failed = Arc::clone(&peer.reader_failed);
            peer.input = Some(relay);
            peer.io_threads.push(std::thread::spawn(move || {
                if let Ok((bytes, completion)) = requests.recv() {
                    let (done, written) = mpsc::channel();
                    original.send((bytes, done)).unwrap();
                    written.recv_timeout(DEADLINE).unwrap().unwrap();
                    let deadline = Instant::now() + DEADLINE;
                    while !failed.load(Ordering::Acquire) && Instant::now() < deadline {
                        std::thread::sleep(Duration::from_millis(1));
                    }
                    assert!(failed.load(Ordering::Acquire), "fixture reader did not fail");
                    completion.send(Ok(())).unwrap();
                }
            }));
            assert_eq!(peer.start_empty_thread(), Err(WireError::Closed));
            assert!(peer.reader_failed.load(Ordering::Acquire));
            assert_eq!(peer.start_empty_thread(), Err(WireError::Closed));
            assert_eq!(
                fs::read_to_string(ledger).unwrap().matches("thread/start").count(),
                1
            );
            let receipt = peer.close_observed();
            assert!(receipt.io_threads_stopped);
            assert!(matches!(receipt.child_exit, ChildExit::Exited { .. }));
        }
    }
    #[test]
    fn native_unpoisoned_reply_remains_accepted() {
        let (mut peer, ledger) = scripted(
            "  print(json.dumps({'id':r['id'],'result':{'thread':{'id':'native-123'}}}),flush=True)"
        );
        peer.initialize().unwrap();
        assert_eq!(peer.start_empty_thread(), Ok("native-123".into()));
        assert!(!peer.reader_failed.load(Ordering::Acquire));
        assert_eq!(
            fs::read_to_string(ledger)
                .unwrap()
                .matches("thread/start")
                .count(),
            1
        );
    }

    #[test]
    fn native_task_turn_maps_only_closed_profile_and_actual_id() {
        let (mut peer, ledger) = scripted(
            "  assert r['method']=='turn/start'
  assert r['params']=={'threadId':'thread-native','input':[{'type':'text','text':'synthetic','text_elements':[]}],'model':'synthetic-model','approvalPolicy':'never','sandboxPolicy':{'type':'readOnly','networkAccess':False}}
  print(json.dumps({'id':r['id'],'result':{'turn':{'id':'turn-native'}}}),flush=True)"
        );
        peer.initialize().unwrap();
        assert_eq!(
            peer.start_text_turn("thread-native", "synthetic", "synthetic-model"),
            Ok("turn-native".into())
        );
        assert_eq!(
            fs::read_to_string(ledger)
                .unwrap()
                .matches("turn/start")
                .count(),
            1
        );
    }
}
