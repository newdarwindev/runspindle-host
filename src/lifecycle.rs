//! Bounded, process-local lifecycle coordination over an injected peer.
//! This is not a native app-server protocol, durable store or authorization service.
use std::collections::{BTreeMap, VecDeque};

pub const VERSION: &str = "runspindle.runtime.v1";
pub const MAX_IDENTIFIER_BYTES: usize = 128;
pub const MAX_EVENT_BYTES: usize = 4096;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Binding {
    pub task_id: String,
    pub revision: u64,
    pub operation_id: String,
    pub policy_revision: String,
    pub host_id: String,
    pub workspace_id: String,
    pub lease_epoch: u64,
}
impl Binding {
    fn valid(&self) -> bool {
        [
            &self.task_id,
            &self.operation_id,
            &self.host_id,
            &self.workspace_id,
        ]
        .into_iter()
        .all(|s| valid_id(s))
            && self.revision > 0
            && self.lease_epoch > 0
            && valid_id(&self.policy_revision)
    }
}
fn valid_id(value: &str) -> bool {
    !value.is_empty()
        && value.as_bytes()[0].is_ascii_alphanumeric()
        && value.len() <= MAX_IDENTIFIER_BYTES
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-_.:".contains(&b))
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Request {
    pub version: String,
    pub binding: Binding,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Action {
    Launch,
    Reconcile,
    Cancel,
}
pub trait Verifier {
    fn verify(&self, binding: &Binding, action: Action) -> bool;
}
#[derive(Default)]
pub struct Deny;
impl Verifier for Deny {
    fn verify(&self, _: &Binding, _: Action) -> bool {
        false
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Terminal {
    Completed,
    Failed,
    Cancelled,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Observation {
    pub binding: Binding,
    pub native_session_id: String,
    pub sequence: u64,
    pub observed_at_ms: u64,
    pub terminal: Option<Terminal>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LaunchReply {
    Accepted(Observation),
    Rejected,
    LostReply,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CancelReply {
    Acknowledged,
    LostReply,
}
/// Implementations must not obtain credentials or broaden the supplied binding.
/// Errors after a possible effect must return LostReply, never an automatic retry.
pub trait Peer {
    fn launch(&mut self, binding: &Binding) -> LaunchReply;
    fn discover(&mut self, binding: &Binding) -> Vec<Observation>;
    fn cancel(&mut self, binding: &Binding, session_id: &str) -> CancelReply;
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Reachability {
    Online,
    Offline,
    Unknown,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Presence {
    pub host_id: String,
    pub workspace_id: String,
    pub reachability: Reachability,
    pub observed_at_ms: u64,
    pub valid_until_ms: u64,
}
impl Presence {
    fn ready(&self, binding: &Binding, now: u64) -> bool {
        self.host_id == binding.host_id
            && self.workspace_id == binding.workspace_id
            && self.reachability == Reachability::Online
            && self.observed_at_ms <= now
            && now <= self.valid_until_ms
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Execution {
    Queued,
    Scheduled,
    Acknowledged,
    Unknown,
    Rejected,
    Terminal(Terminal),
    CancelledBeforeDispatch,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Cancellation {
    None,
    Requested,
    Acknowledged,
    ObservedTerminal,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Snapshot {
    pub binding: Binding,
    pub execution: Execution,
    pub cancellation: Cancellation,
    pub native_session_id: Option<String>,
    pub last_sequence: u64,
    pub observed_at_ms: Option<u64>,
    pub cancel_dispatched: bool,
}
impl Snapshot {
    fn terminal(&self) -> bool {
        matches!(
            self.execution,
            Execution::Terminal(_) | Execution::Rejected | Execution::CancelledBeforeDispatch
        )
    }
}
/// Receipts deliberately contain correlation and lifecycle metadata only; no peer text.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Receipt {
    pub version: &'static str,
    pub snapshot: Snapshot,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    Malformed,
    Denied,
    Conflict,
    Capacity,
    Backpressure,
    NotFound,
    WrongBinding,
    Stale,
    Ambiguous,
    Unknown,
    Unsupported,
    Shutdown,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EventKind {
    Started,
    Progress,
    Terminal(Terminal),
    Unsupported,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Event {
    pub version: String,
    pub observation: Observation,
    pub kind: EventKind,
    pub encoded_size: usize,
}

pub struct Adapter<V = Deny> {
    verifier: V,
    entries: BTreeMap<String, Snapshot>,
    receipts: VecDeque<Receipt>,
    max_entries: usize,
    max_receipts: usize,
    closed: bool,
    max_age_ms: u64,
}
impl Adapter<Deny> {
    pub fn denied(max_entries: usize, max_receipts: usize, max_age_ms: u64) -> Result<Self, Error> {
        Self::new(Deny, max_entries, max_receipts, max_age_ms)
    }
}
impl<V: Verifier> Adapter<V> {
    pub fn new(
        verifier: V,
        max_entries: usize,
        max_receipts: usize,
        max_age_ms: u64,
    ) -> Result<Self, Error> {
        if max_entries == 0
            || max_entries > 1024
            || max_receipts == 0
            || max_receipts > 4096
            || max_age_ms == 0
        {
            return Err(Error::Malformed);
        }
        Ok(Self {
            verifier,
            entries: BTreeMap::new(),
            receipts: VecDeque::new(),
            max_entries,
            max_receipts,
            closed: false,
            max_age_ms,
        })
    }
    fn writable(&self) -> Result<(), Error> {
        if self.closed {
            Err(Error::Shutdown)
        } else if self.receipts.len() == self.max_receipts {
            Err(Error::Backpressure)
        } else {
            Ok(())
        }
    }
    fn authorized(&self, binding: &Binding, action: Action) -> Result<(), Error> {
        if !binding.valid() {
            return Err(Error::Malformed);
        }
        if !self.verifier.verify(binding, action) {
            return Err(Error::Denied);
        }
        Ok(())
    }
    fn get(&self, binding: &Binding) -> Result<Snapshot, Error> {
        let value = self
            .entries
            .get(&binding.operation_id)
            .ok_or(Error::NotFound)?;
        if &value.binding != binding {
            return Err(Error::Conflict);
        }
        Ok(value.clone())
    }
    fn put(&mut self, value: Snapshot) -> Snapshot {
        self.entries
            .insert(value.binding.operation_id.clone(), value.clone());
        self.receipts.push_back(Receipt {
            version: VERSION,
            snapshot: value.clone(),
        });
        value
    }
    pub fn snapshot(&self, binding: &Binding) -> Result<Snapshot, Error> {
        self.get(binding)
    }
    pub fn drain(&mut self) -> Vec<Receipt> {
        self.receipts.drain(..).collect()
    }
    pub fn shutdown(&mut self) {
        self.closed = true;
    }
    pub fn submit<P: Peer>(
        &mut self,
        request: Request,
        presence: &Presence,
        now: u64,
        peer: &mut P,
    ) -> Result<Snapshot, Error> {
        if request.version != VERSION {
            return Err(Error::Malformed);
        }
        let binding = request.binding;
        self.authorized(&binding, Action::Launch)?;
        if self.closed {
            return Err(Error::Shutdown);
        }
        if let Some(existing) = self.entries.get(&binding.operation_id) {
            if existing.binding != binding {
                return Err(Error::Conflict);
            }
            return Ok(existing.clone());
        }
        self.writable()?;
        if self.entries.len() == self.max_entries {
            return Err(Error::Capacity);
        }
        let mut value = Snapshot {
            binding,
            execution: Execution::Queued,
            cancellation: Cancellation::None,
            native_session_id: None,
            last_sequence: 0,
            observed_at_ms: None,
            cancel_dispatched: false,
        };
        if presence.ready(&value.binding, now)
            && now.saturating_sub(presence.observed_at_ms) <= self.max_age_ms
        {
            self.launch(&mut value, now, peer);
        }
        Ok(self.put(value))
    }
    fn launch<P: Peer>(&mut self, value: &mut Snapshot, now: u64, peer: &mut P) {
        value.execution = Execution::Scheduled;
        // Retain intent before entering the peer, including if a peer panics.
        self.entries
            .insert(value.binding.operation_id.clone(), value.clone());
        // There is no retry loop. Any unbound accepted reply remains uncertain.
        match peer.launch(&value.binding) {
            LaunchReply::Rejected => value.execution = Execution::Rejected,
            LaunchReply::LostReply => value.execution = Execution::Unknown,
            LaunchReply::Accepted(observation) => {
                value.execution = Execution::Unknown;
                let _ = Self::observe(value, observation, now, self.max_age_ms);
            }
        }
    }
    pub fn dispatch<P: Peer>(
        &mut self,
        binding: &Binding,
        presence: &Presence,
        now: u64,
        peer: &mut P,
    ) -> Result<Snapshot, Error> {
        self.authorized(binding, Action::Launch)?;
        self.writable()?;
        let mut value = self.get(binding)?;
        if value.execution != Execution::Queued
            || (!presence.ready(binding, now)
                || now.saturating_sub(presence.observed_at_ms) > self.max_age_ms)
        {
            return Ok(value);
        }
        self.launch(&mut value, now, peer);
        Ok(self.put(value))
    }
    fn observe(
        value: &mut Snapshot,
        observation: Observation,
        now: u64,
        max_age_ms: u64,
    ) -> Result<(), Error> {
        if observation.binding != value.binding || !valid_id(&observation.native_session_id) {
            return Err(Error::WrongBinding);
        }
        if let Execution::Terminal(previous) = value.execution {
            if observation.terminal == Some(previous)
                && value.native_session_id.as_ref() == Some(&observation.native_session_id)
                && observation.sequence == value.last_sequence
                && Some(observation.observed_at_ms) == value.observed_at_ms
            {
                return Ok(());
            }
            return Err(Error::Conflict);
        }
        if observation.sequence == 0
            || observation.sequence <= value.last_sequence
            || observation.observed_at_ms > now
            || now.saturating_sub(observation.observed_at_ms) > max_age_ms
            || value
                .observed_at_ms
                .is_some_and(|old| observation.observed_at_ms < old)
        {
            return Err(Error::Stale);
        }
        if value
            .native_session_id
            .as_ref()
            .is_some_and(|old| old != &observation.native_session_id)
        {
            return Err(Error::WrongBinding);
        }
        if value.terminal() {
            return Err(Error::Stale);
        }
        value.native_session_id = Some(observation.native_session_id);
        value.last_sequence = observation.sequence;
        value.observed_at_ms = Some(observation.observed_at_ms);
        value.execution = match observation.terminal {
            Some(t) => Execution::Terminal(t),
            None => Execution::Acknowledged,
        };
        if observation.terminal.is_some() && value.cancellation != Cancellation::None {
            value.cancellation = Cancellation::ObservedTerminal;
        }
        Ok(())
    }
    pub fn reconcile<P: Peer>(
        &mut self,
        binding: &Binding,
        now: u64,
        peer: &mut P,
    ) -> Result<Snapshot, Error> {
        self.authorized(binding, Action::Reconcile)?;
        self.writable()?;
        let mut value = self.get(binding)?;
        if value.terminal() {
            return Ok(value);
        }
        let observations = peer.discover(binding);
        // The entire discovery result must be exact and unique; never select a convenient match.
        if observations.len() != 1 {
            value.execution = Execution::Unknown;
            self.put(value);
            return Err(if observations.is_empty() {
                Error::Unknown
            } else {
                Error::Ambiguous
            });
        }
        Self::observe(
            &mut value,
            observations.into_iter().next().expect("one observation"),
            now,
            self.max_age_ms,
        )?;
        Ok(self.put(value))
    }
    pub fn event(&mut self, binding: &Binding, event: Event, now: u64) -> Result<Snapshot, Error> {
        self.writable()?;
        if event.version != VERSION
            || event.encoded_size == 0
            || event.encoded_size > MAX_EVENT_BYTES
        {
            return Err(Error::Malformed);
        }
        if event.kind == EventKind::Unsupported {
            return Err(Error::Unsupported);
        }
        let expected = match event.kind {
            EventKind::Terminal(t) => Some(t),
            _ => None,
        };
        if event.observation.terminal != expected {
            return Err(Error::Malformed);
        }
        let mut value = self.get(binding)?;
        if value.native_session_id.is_none() {
            return Err(Error::Unknown);
        }
        Self::observe(&mut value, event.observation, now, self.max_age_ms)?;
        Ok(self.put(value))
    }
    pub fn cancel<P: Peer>(&mut self, binding: &Binding, peer: &mut P) -> Result<Snapshot, Error> {
        self.authorized(binding, Action::Cancel)?;
        self.writable()?;
        let mut value = self.get(binding)?;
        if value.terminal() || value.cancel_dispatched {
            return Ok(value);
        }
        value.cancellation = Cancellation::Requested;
        match value.execution {
            Execution::Queued => value.execution = Execution::CancelledBeforeDispatch,
            _ => {
                if let Some(session) = &value.native_session_id {
                    value.cancel_dispatched = true;
                    self.entries
                        .insert(binding.operation_id.clone(), value.clone());
                    if peer.cancel(binding, session) == CancelReply::Acknowledged {
                        value.cancellation = Cancellation::Acknowledged;
                    }
                }
            }
        }
        Ok(self.put(value))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Support {
    Supported,
    Unsupported,
    Unknown,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Capabilities {
    pub platform: String,
    pub lifecycle: Support,
    pub capture: Support,
    pub text_transfer: Support,
    pub submission: Support,
}
impl Capabilities {
    /// Synthetic profile only, not an observation of a native implementation.
    pub fn scripted(platform: &str) -> Result<Self, Error> {
        if !matches!(platform, "macos" | "linux") {
            return Err(Error::Unsupported);
        }
        Ok(Self {
            platform: platform.into(),
            lifecycle: Support::Supported,
            capture: Support::Unsupported,
            text_transfer: Support::Unsupported,
            submission: Support::Unsupported,
        })
    }
}
