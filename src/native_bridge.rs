//! Native identity journal attached to the existing durable lifecycle Peer contract.
//! Counters/times are trusted local host observations, never fabricated native fields.
use crate::lifecycle::{
    Action, Adapter, Binding, CancelReply, Error, Event, EventKind, Execution, LaunchReply,
    Observation, Peer, Terminal, Verifier, VERSION,
};
use crate::native::{NativeIds, NativeNotice, StdioSession, WireError};
use crate::storage::Storage;
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::Path;

const MAX_ROWS: u64 = 1024;
const MAX_ITEMS: usize = 128;
const MAX_BYTES: usize = 65536;

/// Injected ports are scripted unless backed by the separately pinned StdioSession.
/// No port may discover an operation by guessing a native ID or reissuing start.
pub trait NativeIo {
    fn interrupt_turn(&mut self, _: crate::native_turn::CancelPermit<'_>) -> Result<(), WireError> {
        Err(WireError::Unsupported)
    }
    fn start_empty_thread(&mut self) -> Result<String, WireError>;
    fn start_text_turn(
        &mut self,
        _: crate::native_turn::TurnPermit<'_>,
    ) -> Result<String, WireError> {
        Err(WireError::Unsupported)
    }
    fn read_thread(&mut self, thread: &str) -> Result<NativeIds, WireError>;
    fn next_notice(&mut self, thread: &str) -> Option<Result<NativeNotice, WireError>>;
}
impl NativeIo for StdioSession {
    fn interrupt_turn(
        &mut self,
        permit: crate::native_turn::CancelPermit<'_>,
    ) -> Result<(), WireError> {
        self.interrupt(permit.thread_id(), permit.turn_id())
    }
    fn start_text_turn(
        &mut self,
        permit: crate::native_turn::TurnPermit<'_>,
    ) -> Result<String, WireError> {
        self.start_text_turn(permit.thread_id(), permit.text(), permit.model())
    }
    fn start_empty_thread(&mut self) -> Result<String, WireError> {
        self.start_empty_thread()
    }
    fn read_thread(&mut self, thread: &str) -> Result<NativeIds, WireError> {
        self.read_thread(thread)
    }
    fn next_notice(&mut self, thread: &str) -> Option<Result<NativeNotice, WireError>> {
        self.next_notice(thread)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct State {
    binding: Binding,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    task_turn: Option<crate::native_turn::Reservation>,
    // The row itself reserves launch. None is uncertain, not proven absent.
    thread: Option<String>,
    turn: Option<String>,
    items: BTreeMap<String, bool>,
    // Native turn metadata only; never task execution/acceptance.
    terminal: Option<Terminal>,
    receiving: bool,
    sequence: u64,
    observed_at_ms: u64,
    pending: Option<Observation>,
}
fn validate_cancel_target(
    state: &State,
    request: &crate::native_turn::CancelRequest,
) -> Result<(), Error> {
    let task = state.task_turn.as_ref().ok_or(Error::Unknown)?;
    if task.request != request.task || state.thread.as_deref() != Some(&request.task.thread_id) {
        return Err(Error::WrongBinding);
    }
    let turn = task.turn_id.as_ref().ok_or(Error::Unknown)?;
    if turn != &request.turn_id || state.turn.as_ref() != Some(turn) {
        return Err(Error::WrongBinding);
    }
    Ok(())
}
fn sql(_: rusqlite::Error) -> Error {
    Error::Storage
}
fn valid_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value.as_bytes()[0].is_ascii_alphanumeric()
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-_.:".contains(&b))
}
fn encode(state: &State) -> Result<Vec<u8>, Error> {
    let bytes = serde_json::to_vec(state).map_err(|_| Error::Storage)?;
    if bytes.len() > MAX_BYTES || state.items.len() > MAX_ITEMS {
        return Err(Error::Capacity);
    }
    Ok(bytes)
}

fn load_state(db: &Connection, binding: &Binding) -> Result<Option<(u64, State)>, Error> {
    let row: Option<(u64, u64, Vec<u8>)> = db
        .query_row(
            "SELECT version,generation,state FROM host_native_state WHERE operation=?1",
            [&binding.operation_id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .optional()
        .map_err(sql)?;
    row.map(|(version, generation, bytes)| {
        if version != 1 || bytes.len() > MAX_BYTES {
            return Err(Error::Unsupported);
        }
        let state: State = serde_json::from_slice(&bytes).map_err(|_| Error::Storage)?;
        if &state.binding != binding {
            return Err(Error::Conflict);
        }
        if state.pending.as_ref().is_some_and(|p| p.terminal.is_some()) {
            return Err(Error::Unsupported);
        }
        if state.task_turn.as_ref().is_some_and(|task| {
            !task.request.valid()
                || task.request.binding != state.binding
                || state.thread.as_deref() != Some(&task.request.thread_id)
                || task.cancellation.as_ref().is_some_and(|cancel| {
                    !cancel.request.valid()
                        || cancel.request.task != task.request
                        || task.turn_id.as_ref() != Some(&cancel.request.turn_id)
                })
                || task
                    .turn_id
                    .as_ref()
                    .is_some_and(|id| !valid_id(id) || state.turn.as_ref() != Some(id))
        }) {
            return Err(Error::Storage);
        }
        if state.items.len() > MAX_ITEMS {
            return Err(Error::Capacity);
        }
        Ok((generation, state))
    })
    .transpose()
}
// A generation is allocated by SQLite, not by the caller or peer. It fences
// transport ownership only; it is distinct from the immutable authority lease.
fn check_owner(db: &Connection, binding: &Binding, owner: u64) -> Result<(), Error> {
    let current: Option<u64> = db
        .query_row(
            "SELECT generation FROM host_native_owner WHERE operation=?1",
            [&binding.operation_id],
            |r| r.get(0),
        )
        .optional()
        .map_err(sql)?;
    if current != Some(owner) {
        return Err(Error::Conflict);
    }
    Ok(())
}

// The first committed ownership claim does not preserve authority while this
// second writer lock waits. Revalidate immediately before the factory effect.
fn make_owned_io<I>(
    db: &mut Connection,
    binding: &Binding,
    owner: u64,
    verifier: &impl Verifier,
    make_io: impl FnOnce() -> Result<I, WireError>,
) -> Result<I, Error> {
    let tx = db
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(sql)?;
    check_owner(&tx, binding, owner)?;
    if !verifier.verify(binding, Action::TakeOwnership) {
        return Err(Error::Denied);
    }
    let io = make_io().map_err(|_| Error::Unknown)?;
    tx.commit().map_err(sql)?;
    Ok(io)
}

/// Trusted local configuration, supplied independently of incoming request/Presence.
/// This is a pin comparison, not a credential or authority resolver.
pub struct LocalPin {
    host_id: String,
    workspace_id: String,
}
impl LocalPin {
    pub fn new(host_id: &str, workspace_id: &str) -> Result<Self, Error> {
        if !valid_id(host_id) || !valid_id(workspace_id) {
            return Err(Error::Malformed);
        }
        Ok(Self {
            host_id: host_id.into(),
            workspace_id: workspace_id.into(),
        })
    }
}
/// One exact operation/host/workspace/lease binding per bridge. Verifier authority
/// remains in Adapter; task turns require a separate exact-request authority contract.
pub struct NativeBridge<I, C> {
    db: Connection,
    owner: u64,
    binding: Binding,
    io: I,
    clock: C,
    retained_notice: Option<NativeNotice>,
}
impl<I: NativeIo, C: Fn() -> u64> NativeBridge<I, C> {
    pub fn open<F: FnOnce() -> Result<I, WireError>>(
        path: impl AsRef<Path>,
        local_pin: LocalPin,
        binding: Binding,
        verifier: &impl Verifier,
        make_io: F,
        clock: C,
    ) -> Result<Self, Error> {
        let path = path.as_ref();
        crate::storage::require_durable_path(path)?;
        if !binding.valid() {
            return Err(Error::Malformed);
        }
        if binding.host_id != local_pin.host_id || binding.workspace_id != local_pin.workspace_id {
            return Err(Error::WrongBinding);
        }
        // Opening another transport replaces the current owner, so it requires
        // explicit authority, including when there is no native session yet.
        if !verifier.verify(&binding, Action::TakeOwnership) {
            return Err(Error::Denied);
        }
        let mut db = Connection::open(path).map_err(sql)?;
        db.busy_timeout(std::time::Duration::from_secs(5))
            .map_err(sql)?;
        db.execute_batch("PRAGMA journal_mode=WAL;PRAGMA synchronous=FULL;PRAGMA max_page_count=8192;
            CREATE TABLE IF NOT EXISTS host_native_state(operation TEXT PRIMARY KEY CHECK(length(operation)<=128),version INTEGER NOT NULL CHECK(version=1),generation INTEGER NOT NULL,state BLOB NOT NULL CHECK(length(state)<=65536));
            CREATE TABLE IF NOT EXISTS host_native_owner(operation TEXT PRIMARY KEY CHECK(length(operation)<=128),generation INTEGER NOT NULL CHECK(generation>0),binding BLOB NOT NULL CHECK(length(binding)<=4096));").map_err(sql)?;
        load_state(&db, &binding)?;
        let tx = db
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql)?;
        // Recheck after acquiring the lock; waiting cannot preserve expired authority.
        if !verifier.verify(&binding, Action::TakeOwnership) {
            return Err(Error::Denied);
        }
        let previous: Option<(u64, Vec<u8>)> = tx
            .query_row(
                "SELECT generation,binding FROM host_native_owner WHERE operation=?1",
                [&binding.operation_id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()
            .map_err(sql)?;
        let owner = if let Some((generation, bytes)) = previous {
            let saved: Binding = serde_json::from_slice(&bytes).map_err(|_| Error::Storage)?;
            if saved != binding {
                return Err(Error::Conflict);
            }
            generation
                .checked_add(1)
                .filter(|n| *n <= i64::MAX as u64)
                .ok_or(Error::Capacity)?
        } else {
            let count: u64 = tx
                .query_row("SELECT count(*) FROM host_native_owner", [], |r| r.get(0))
                .map_err(sql)?;
            if count >= MAX_ROWS {
                return Err(Error::Capacity);
            }
            1
        };
        let bytes = serde_json::to_vec(&binding).map_err(|_| Error::Storage)?;
        tx.execute("INSERT INTO host_native_owner VALUES(?1,?2,?3) ON CONFLICT(operation) DO UPDATE SET generation=excluded.generation", params![binding.operation_id, owner, bytes]).map_err(sql)?;
        tx.commit().map_err(sql)?;
        // Persist ownership before factory I/O: a crash or failed factory never
        // restores authority to the old owner. Takeover is not launch permission.
        let io = make_owned_io(&mut db, &binding, owner, verifier, make_io)?;
        let bridge = Self {
            db,
            owner,
            binding,
            io,
            clock,
            retained_notice: None,
        };
        bridge.load()?;
        Ok(bridge)
    }
    fn load(&self) -> Result<Option<(u64, State)>, Error> {
        check_owner(&self.db, &self.binding, self.owner)?;
        load_state(&self.db, &self.binding)
    }
    /// Serialize the owner check and native call against ownership replacement.
    /// NativeIo must be bounded and must not reenter this SQLite database. The
    /// pinned StdioSession uses bounded I/O; injected ports must honor that contract.
    fn fenced_io<R>(&mut self, call: impl FnOnce(&mut I) -> R) -> Result<R, Error> {
        let tx = self
            .db
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql)?;
        check_owner(&tx, &self.binding, self.owner)?;
        let result = call(&mut self.io);
        tx.commit().map_err(sql)?;
        Ok(result)
    }
    fn save(&mut self, generation: u64, state: &State) -> Result<(), Error> {
        let bytes = encode(state)?;
        let next = generation
            .checked_add(1)
            .filter(|n| *n <= i64::MAX as u64)
            .ok_or(Error::Capacity)?;
        let tx = self
            .db
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql)?;
        check_owner(&tx, &self.binding, self.owner)?;
        if tx.execute("UPDATE host_native_state SET generation=?1,state=?2 WHERE operation=?3 AND version=1 AND generation=?4",params![next,bytes,self.binding.operation_id,generation]).map_err(sql)? != 1 { return Err(Error::Conflict); }
        tx.commit().map_err(sql)
    }
    fn reserve(&mut self) -> Result<(), Error> {
        let state = State {
            binding: self.binding.clone(),
            task_turn: None,
            thread: None,
            turn: None,
            items: BTreeMap::new(),
            terminal: None,
            receiving: false,
            sequence: 0,
            observed_at_ms: 0,
            pending: None,
        };
        let bytes = encode(&state)?;
        let tx = self
            .db
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql)?;
        check_owner(&tx, &self.binding, self.owner)?;
        let count: u64 = tx
            .query_row("SELECT count(*) FROM host_native_state", [], |r| r.get(0))
            .map_err(sql)?;
        if count >= MAX_ROWS {
            return Err(Error::Capacity);
        }
        if tx
            .execute(
                "INSERT OR IGNORE INTO host_native_state VALUES(?1,1,0,?2)",
                params![self.binding.operation_id, bytes],
            )
            .map_err(sql)?
            != 1
        {
            return Err(Error::Conflict);
        }
        tx.commit().map_err(sql)
    }
    fn observation(&self, state: &mut State) -> Result<Observation, Error> {
        let now = (self.clock)();
        if now < state.observed_at_ms {
            return Err(Error::Stale);
        }
        state.sequence = state.sequence.checked_add(1).ok_or(Error::Capacity)?;
        state.observed_at_ms = now;
        Ok(Observation {
            binding: self.binding.clone(),
            native_session_id: state.thread.clone().ok_or(Error::Unknown)?,
            sequence: state.sequence,
            observed_at_ms: now,
            terminal: None,
        })
    }
    /// One separately authorized task turn on the already bound session.
    /// A committed reservation is never retried, even after denial, crash or lost reply.
    pub fn submit_turn<V: crate::native_turn::Verifier, R: crate::native_turn::InputResolver>(
        &mut self,
        request: &crate::native_turn::Request,
        verifier: &V,
        resolver: &R,
    ) -> Result<crate::native_turn::Outcome, Error> {
        use crate::native_turn::{Outcome, Reservation};
        if !request.valid() {
            return Err(Error::Malformed);
        }
        if request.binding != self.binding {
            return Err(Error::WrongBinding);
        }
        if !verifier.verify_turn(request) {
            return Err(Error::Denied);
        }
        let (generation, mut state) = self.load()?.ok_or(Error::Unknown)?;
        if state.thread.as_deref() != Some(&request.thread_id) {
            return Err(Error::WrongBinding);
        }
        // SQLite reads can wait. Refresh authority before exposing a retained
        // outcome or opening the request's input.
        if !verifier.verify_turn(request) {
            return Err(Error::Denied);
        } // input-time authority
        if let Some(saved) = &state.task_turn {
            if &saved.request != request {
                return Err(Error::Conflict);
            }
            return Ok(saved.turn_id.clone().map_or(Outcome::Unknown, |turn_id| {
                Outcome::Acknowledged { turn_id }
            }));
        }
        if state.turn.is_some()
            || state.terminal.is_some()
            || state.receiving
            || state.pending.is_some()
        {
            return Err(Error::Conflict);
        }
        let text = crate::native_turn::resolve(resolver, &request.input)?;
        state.task_turn = Some(Reservation {
            request: request.clone(),
            turn_id: None,
            cancellation: None,
        });
        self.save(generation, &state)?;
        // Owner and exact task authority are checked after the writer lock wait,
        // immediately before the external effect. Rollback cannot undo that effect.
        let tx = self
            .db
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql)?;
        check_owner(&tx, &self.binding, self.owner)?;
        if !verifier.verify_turn(request) {
            return Err(Error::Denied);
        } // dispatch-time authority
        let result = self
            .io
            .start_text_turn(crate::native_turn::TurnPermit::new(request, &text));
        tx.commit().map_err(sql)?;
        let Ok(turn_id) = result else {
            return Ok(Outcome::Unknown);
        };
        if !valid_id(&turn_id) {
            return Ok(Outcome::Unknown);
        }
        let (generation, mut state) = self.load()?.ok_or(Error::Unknown)?;
        let saved = state.task_turn.as_mut().ok_or(Error::Unknown)?;
        if &saved.request != request || saved.turn_id.is_some() {
            return Err(Error::Conflict);
        }
        saved.turn_id = Some(turn_id.clone());
        state.turn = Some(turn_id.clone());
        self.save(generation, &state)?;
        // Retain the peer acknowledgement even if the caller can no longer receive it.
        if !verifier.verify_turn(request) {
            return Err(Error::Denied);
        }
        Ok(Outcome::Acknowledged { turn_id })
    }
    /// Reserve and dispatch at most one separately authorized exact-turn interrupt.
    /// An acknowledgement never promotes terminal state or permits retry.
    pub fn cancel_turn<V: crate::native_turn::CancelVerifier>(
        &mut self,
        request: &crate::native_turn::CancelRequest,
        verifier: &V,
    ) -> Result<crate::native_turn::CancelOutcome, Error> {
        use crate::native_turn::{CancelOutcome, CancelPermit, CancelReservation};
        if !request.valid() {
            return Err(Error::Malformed);
        }
        if request.task.binding != self.binding {
            return Err(Error::WrongBinding);
        }
        if !verifier.verify_cancel(request) {
            return Err(Error::Denied);
        }
        let (generation, mut state) = self.load()?.ok_or(Error::Unknown)?;
        validate_cancel_target(&state, request)?;
        // Replay can disclose a retained outcome without reaching the dispatch
        // fence, so recheck exact authority after the journal read.
        let retained = |outcome| {
            if !verifier.verify_cancel(request) {
                return Err(Error::Denied);
            }
            Ok(outcome)
        };
        if state.receiving {
            return retained(CancelOutcome::Unknown);
        }
        if let Some(terminal) = state.terminal {
            return retained(CancelOutcome::ObservedTerminal { terminal });
        }
        let task = state.task_turn.as_mut().ok_or(Error::Unknown)?;
        if let Some(saved) = &task.cancellation {
            if &saved.request != request {
                return Err(Error::Conflict);
            }
            return retained(if saved.acknowledged {
                CancelOutcome::Acknowledged
            } else {
                CancelOutcome::Unknown
            });
        }
        task.cancellation = Some(CancelReservation {
            request: request.clone(),
            acknowledged: false,
        });
        self.save(generation, &state)?;
        let tx = self
            .db
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql)?;
        check_owner(&tx, &self.binding, self.owner)?;
        let (_, current) = load_state(&tx, &self.binding)?.ok_or(Error::Unknown)?;
        validate_cancel_target(&current, request)?;
        if !verifier.verify_cancel(request) {
            return Err(Error::Denied);
        } // cancel dispatch authority
        if current.receiving {
            return Ok(CancelOutcome::Unknown);
        }
        if let Some(terminal) = current.terminal {
            return Ok(CancelOutcome::ObservedTerminal { terminal });
        }
        let saved = current
            .task_turn
            .as_ref()
            .and_then(|t| t.cancellation.as_ref())
            .ok_or(Error::Unknown)?;
        if &saved.request != request || saved.acknowledged {
            return Err(Error::Conflict);
        }
        let result = self.io.interrupt_turn(CancelPermit::new(request));
        tx.commit().map_err(sql)?;
        if result.is_err() {
            return Ok(CancelOutcome::Unknown);
        }
        let (generation, mut state) = self.load()?.ok_or(Error::Unknown)?;
        validate_cancel_target(&state, request)?;
        let saved = state
            .task_turn
            .as_mut()
            .and_then(|t| t.cancellation.as_mut())
            .ok_or(Error::Unknown)?;
        if &saved.request != request || saved.acknowledged {
            return Err(Error::Conflict);
        }
        saved.acknowledged = true;
        self.save(generation, &state)?;
        if !verifier.verify_cancel(request) {
            return Err(Error::Denied);
        }
        Ok(CancelOutcome::Acknowledged)
    }
    /// Durable sanitized identity readback, separate from a claim of current native liveness.
    pub fn identities(&self) -> Result<Option<NativeIds>, Error> {
        Ok(self.load()?.and_then(|(_, s)| {
            s.thread.map(|thread_id| NativeIds {
                thread_id,
                turn_id: s.turn,
                item_id: None,
            })
        }))
    }
    pub fn item_completed(&self, item: &str) -> Result<Option<bool>, Error> {
        Ok(self.load()?.and_then(|(_, s)| s.items.get(item).copied()))
    }
    /// Recorded native turn outcome is metadata, never task completion authority.
    pub fn turn_outcome(&self) -> Result<Option<Terminal>, Error> {
        Ok(self.load()?.and_then(|(_, s)| s.terminal))
    }
    pub fn items(&self) -> Result<BTreeMap<String, bool>, Error> {
        Ok(self.load()?.map(|(_, s)| s.items).unwrap_or_default())
    }
    /// Consume at most one notification only when the previous pending observation
    /// has been delivered. Receipt backpressure never discards terminal identity.
    fn apply_notice(state: &mut State, notice: NativeNotice) -> Result<bool, Error> {
        let changed = match notice {
            NativeNotice::ThreadStarted { .. } => false,
            NativeNotice::TurnStarted { turn_id, .. } => {
                if !valid_id(&turn_id) {
                    return Err(Error::Malformed);
                }
                if state.turn.as_ref() == Some(&turn_id) {
                    if state.terminal.is_some() {
                        return Err(Error::Stale);
                    }
                    false
                } else if state.turn.is_some() || state.terminal.is_some() {
                    return Err(Error::Conflict);
                } else {
                    state.turn = Some(turn_id);
                    true
                }
            }
            NativeNotice::ItemStarted {
                turn_id, item_id, ..
            } => {
                if !valid_id(&item_id) || !valid_id(&turn_id) {
                    return Err(Error::Malformed);
                }
                if state.turn.as_ref() != Some(&turn_id) || state.terminal.is_some() {
                    return Err(Error::Conflict);
                }
                if let Some(completed) = state.items.get(&item_id) {
                    if *completed {
                        return Err(Error::Stale);
                    }
                    false
                } else {
                    if state.items.len() >= MAX_ITEMS {
                        return Err(Error::Capacity);
                    }
                    state.items.insert(item_id, false);
                    true
                }
            }
            NativeNotice::ItemCompleted {
                turn_id, item_id, ..
            } => {
                if state.turn.as_ref() != Some(&turn_id) || state.terminal.is_some() {
                    return Err(Error::Conflict);
                }
                let item = state.items.get_mut(&item_id).ok_or(Error::Stale)?;
                if *item {
                    false
                } else {
                    *item = true;
                    true
                }
            }
            NativeNotice::TurnCompleted {
                turn_id, terminal, ..
            } => {
                if state.turn.as_ref() != Some(&turn_id) {
                    return Err(Error::Conflict);
                }
                if let Some(old) = state.terminal {
                    if old != terminal {
                        return Err(Error::Conflict);
                    }
                    false
                } else {
                    state.terminal = Some(terminal);
                    true
                }
            }
        };
        Ok(changed)
    }
    /// A durable receive fence precedes consuming a native notification. If the
    /// process dies between receive and journal commit, reopen stays Unknown;
    /// native notification replay is not fabricated. Same-process save failures
    /// retain the typed notice for retry without rereading the transport.
    pub fn poll(&mut self) -> Result<bool, Error> {
        let (mut generation, mut state) = self.load()?.ok_or(Error::Unknown)?;
        if state.pending.is_some() {
            return Err(Error::Backpressure);
        }
        let thread = state.thread.clone().ok_or(Error::Unknown)?;
        if state.receiving {
            if self.retained_notice.is_none() {
                return Err(Error::Unknown);
            }
        } else {
            state.receiving = true;
            self.save(generation, &state)?;
            generation = generation.checked_add(1).ok_or(Error::Capacity)?;
        }
        if self.retained_notice.is_none() {
            let Some(notice) = self.fenced_io(|io| io.next_notice(&thread))? else {
                state.receiving = false;
                self.save(generation, &state)?;
                return Ok(false);
            };
            self.retained_notice = Some(notice.map_err(|_| Error::Unknown)?);
        }
        let notice = self.retained_notice.clone().ok_or(Error::Unknown)?;
        let mut next = state.clone();
        let outcome = if notice.thread_id() != thread {
            Err(Error::WrongBinding)
        } else {
            Self::apply_notice(&mut next, notice)
        };
        let changed = match outcome {
            Ok(changed) => changed,
            Err(error) => {
                state.receiving = false;
                self.save(generation, &state)?;
                self.retained_notice = None;
                return Err(error);
            }
        };
        if changed {
            next.pending = Some(self.observation(&mut next)?);
        }
        next.receiving = false;
        self.save(generation, &next)?;
        self.retained_notice = None;
        Ok(changed)
    }
    /// Idempotently hand a journaled observation to the existing durable lifecycle.
    /// A crash after Adapter commits but before journal acknowledgement is reconciled
    /// by exact identity/sequence/time/outcome readback, without reissuing an effect.
    pub fn deliver<V: Verifier, S: Storage>(
        &mut self,
        adapter: &mut Adapter<V, S>,
        now: u64,
    ) -> Result<bool, Error> {
        let (generation, mut state) = self.load()?.ok_or(Error::Unknown)?;
        if state.receiving {
            return Err(Error::Unknown);
        }
        let Some(observation) = state.pending.clone() else {
            return Ok(false);
        };
        let snapshot = adapter.snapshot(&self.binding)?;
        let expected = Execution::Acknowledged;
        if snapshot.native_session_id.as_ref() != Some(&observation.native_session_id)
            || snapshot.last_sequence != observation.sequence
            || snapshot.observed_at_ms != Some(observation.observed_at_ms)
            || snapshot.execution != expected
        {
            let encoded_size = serde_json::to_vec(&observation)
                .map_err(|_| Error::Storage)?
                .len();
            let kind = EventKind::Progress;
            adapter.event(
                &self.binding,
                Event {
                    version: VERSION.into(),
                    observation,
                    kind,
                    encoded_size,
                },
                now,
            )?;
        }
        state.pending = None;
        self.save(generation, &state)?;
        Ok(true)
    }
}
impl<I: NativeIo, C: Fn() -> u64> Peer for NativeBridge<I, C> {
    fn launch(&mut self, binding: &Binding) -> LaunchReply {
        if binding != &self.binding {
            return LaunchReply::Rejected;
        }
        // Reopened or concurrent reservations must never call start a second time.
        if self.reserve().is_err() {
            return LaunchReply::LostReply;
        }
        let Ok(Ok(thread)) = self.fenced_io(|io| io.start_empty_thread()) else {
            return LaunchReply::LostReply;
        };
        if !valid_id(&thread) {
            return LaunchReply::LostReply;
        }
        let Ok(Some((generation, mut state))) = self.load() else {
            return LaunchReply::LostReply;
        };
        state.thread = Some(thread);
        let Ok(observation) = self.observation(&mut state) else {
            return LaunchReply::LostReply;
        };
        if self.save(generation, &state).is_err() {
            return LaunchReply::LostReply;
        }
        LaunchReply::Accepted(observation)
    }
    fn discover(&mut self, binding: &Binding) -> Vec<Observation> {
        if binding != &self.binding {
            return vec![];
        }
        let Ok(Some((generation, mut state))) = self.load() else {
            return vec![];
        };
        // No operation-scoped native discovery key exists in the qualified protocol.
        let Some(thread) = state.thread.as_deref() else {
            return vec![];
        };
        if state.pending.is_some() || state.receiving {
            return vec![];
        }
        let Ok(Ok(ids)) = self.fenced_io(|io| io.read_thread(thread)) else {
            return vec![];
        };
        if ids.thread_id != thread {
            return vec![];
        }
        let Ok(observation) = self.observation(&mut state) else {
            return vec![];
        };
        if self.save(generation, &state).is_err() {
            return vec![];
        }
        vec![observation]
    }
    fn cancel(&mut self, _: &Binding, _: &str) -> CancelReply {
        // Observed turn metadata is not authorization to interrupt that turn.
        // Binding-only Peer has no separately authorized exact task-turn binding.
        CancelReply::LostReply
    }
}

#[cfg(test)]
mod owner_tests {
    use super::*;
    use std::sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc, Arc,
    };
    use std::time::Duration;

    struct Revocable(Arc<AtomicBool>);
    impl Verifier for Revocable {
        fn verify(&self, _: &Binding, action: Action) -> bool {
            action == Action::TakeOwnership && self.0.load(Ordering::SeqCst)
        }
    }
    #[test]
    fn native_owner_revocation_during_factory_lock_wait_has_zero_effects() {
        let path =
            std::env::temp_dir().join(format!("native-owner-wait-{}.sqlite", std::process::id()));
        let binding = Binding {
            task_id: "task".into(),
            revision: 1,
            operation_id: "operation".into(),
            policy_revision: "policy".into(),
            host_id: "host".into(),
            workspace_id: "workspace".into(),
            lease_epoch: 1,
        };
        let blocker = Connection::open(&path).unwrap();
        blocker.execute_batch("CREATE TABLE host_native_owner(operation TEXT PRIMARY KEY,generation INTEGER);INSERT INTO host_native_owner VALUES('operation',2);BEGIN IMMEDIATE;").unwrap();
        let granted = Arc::new(AtomicBool::new(true));
        let calls = Arc::new(AtomicU64::new(0));
        let (ready, entered) = mpsc::channel();
        let worker_path = path.clone();
        let worker_grant = granted.clone();
        let worker_calls = calls.clone();
        let worker = std::thread::spawn(move || {
            let mut db = Connection::open(worker_path).unwrap();
            db.busy_timeout(Duration::from_secs(2)).unwrap();
            ready.send(()).unwrap();
            make_owned_io(&mut db, &binding, 2, &Revocable(worker_grant), || {
                worker_calls.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
        });
        entered.recv_timeout(Duration::from_secs(2)).unwrap();
        std::thread::sleep(Duration::from_millis(100));
        assert!(!worker.is_finished());
        granted.store(false, Ordering::SeqCst);
        blocker.execute_batch("COMMIT").unwrap();
        assert_eq!(worker.join().unwrap(), Err(Error::Denied));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        let generation: u64 = blocker
            .query_row("SELECT generation FROM host_native_owner", [], |r| r.get(0))
            .unwrap();
        assert_eq!(generation, 2); // Committed replacement ownership is retained.
        drop(blocker);
        std::fs::remove_file(path).unwrap();
    }
}

#[cfg(test)]
mod turn_input_authority_tests {
    use super::*;
    use crate::native_turn::{InputRef, InputResolver, Profile, Request};
    use std::sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    };
    use std::time::{Duration, Instant};
    static BUSY: AtomicBool = AtomicBool::new(false);
    static CANCEL_BUSY: AtomicBool = AtomicBool::new(false);
    static CANCEL_EFFECTS: AtomicU64 = AtomicU64::new(0);
    struct Io(Arc<AtomicU64>);
    impl NativeIo for Io {
        fn start_text_turn(
            &mut self,
            _: crate::native_turn::TurnPermit<'_>,
        ) -> Result<String, WireError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok("turn".into())
        }
        fn interrupt_turn(
            &mut self,
            _: crate::native_turn::CancelPermit<'_>,
        ) -> Result<(), WireError> {
            CANCEL_EFFECTS.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
        fn start_empty_thread(&mut self) -> Result<String, WireError> {
            Ok("thread".into())
        }
        fn read_thread(&mut self, _: &str) -> Result<NativeIds, WireError> {
            Err(WireError::Unsupported)
        }
        fn next_notice(&mut self, _: &str) -> Option<Result<NativeNotice, WireError>> {
            None
        }
    }
    struct Owner;
    impl Verifier for Owner {
        fn verify(&self, _: &Binding, a: Action) -> bool {
            a == Action::TakeOwnership
        }
    }
    struct Grant(Arc<AtomicBool>);
    impl crate::native_turn::Verifier for Grant {
        fn verify_turn(&self, _: &Request) -> bool {
            self.0.load(Ordering::SeqCst)
        }
    }
    struct Input(Arc<AtomicU64>);
    impl InputResolver for Input {
        fn open(&self, _: &InputRef) -> std::io::Result<Box<dyn std::io::Read>> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(Box::new(std::io::Cursor::new(b"x".to_vec())))
        }
    }
    #[test]
    fn native_turn_input_revocation_during_real_read_lock_wait_opens_nothing() {
        for retained in [false, true] {
            check_turn_read_lock_revocation(retained);
        }
    }

    fn check_turn_read_lock_revocation(retained: bool) {
        let path = std::env::temp_dir().join(format!(
            "native-turn-read-lock-{retained}-{}.sqlite",
            std::process::id()
        ));
        let binding = Binding {
            task_id: "task".into(),
            revision: 1,
            operation_id: "op".into(),
            policy_revision: "policy".into(),
            host_id: "host".into(),
            workspace_id: "workspace".into(),
            lease_epoch: 1,
        };
        let effects = Arc::new(AtomicU64::new(0));
        let mut bridge = NativeBridge::open(
            &path,
            LocalPin::new("host", "workspace").unwrap(),
            binding.clone(),
            &Owner,
            || Ok(Io(effects.clone())),
            || 120,
        )
        .unwrap();
        assert!(matches!(bridge.launch(&binding), LaunchReply::Accepted(_)));
        let request = Request {
            version: crate::native_turn::VERSION.into(),
            binding,
            thread_id: "thread".into(),
            input: InputRef {
                sha256: "2d711642b726b04401627ca9fbac32f5c8530fb1903cc4db02258717921a4881".into(),
                byte_length: 1,
            },
            profile: Profile {
                kind: "read_only_text_v1".into(),
                model: "synthetic".into(),
            },
        };
        if retained {
            let grant = Grant(Arc::new(AtomicBool::new(true)));
            let setup_input = Input(Arc::new(AtomicU64::new(0)));
            assert_eq!(
                bridge.submit_turn(&request, &grant, &setup_input).unwrap(),
                crate::native_turn::Outcome::Acknowledged {
                    turn_id: "turn".into()
                }
            );
        }
        // Rollback-journal exclusive locking blocks the actual read, unlike WAL.
        bridge
            .db
            .execute_batch("PRAGMA journal_mode=DELETE;")
            .unwrap();
        bridge
            .db
            .busy_handler(Some(|attempt| {
                BUSY.store(true, Ordering::SeqCst);
                std::thread::sleep(Duration::from_millis(1));
                attempt < 2000
            }))
            .unwrap();
        let blocker = Connection::open(&path).unwrap();
        blocker.execute_batch("BEGIN EXCLUSIVE;").unwrap();
        BUSY.store(false, Ordering::SeqCst);
        let authorized = Arc::new(AtomicBool::new(true));
        let opens = Arc::new(AtomicU64::new(0));
        let grant = Grant(authorized.clone());
        let input = Input(opens.clone());
        let worker = std::thread::spawn(move || bridge.submit_turn(&request, &grant, &input));
        let deadline = Instant::now() + Duration::from_secs(2);
        while !BUSY.load(Ordering::SeqCst) && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(1));
        }
        let observed_wait = BUSY.load(Ordering::SeqCst);
        authorized.store(false, Ordering::SeqCst);
        blocker.execute_batch("COMMIT;").unwrap();
        let outcome = worker.join();
        drop(blocker);
        std::fs::remove_file(path).unwrap();
        assert_eq!(outcome.unwrap(), Err(Error::Denied));
        assert!(observed_wait, "actual SQLite busy callback not observed");
        assert_eq!(opens.load(Ordering::SeqCst), 0);
        assert_eq!(effects.load(Ordering::SeqCst), u64::from(retained));
    }

    #[test]
    fn native_cancel_revocation_during_actual_writer_wait_has_zero_effects() {
        struct CancelGrant(Arc<AtomicBool>);
        impl crate::native_turn::CancelVerifier for CancelGrant {
            fn verify_cancel(&self, _: &crate::native_turn::CancelRequest) -> bool {
                self.0.load(Ordering::SeqCst)
            }
        }
        let path =
            std::env::temp_dir().join(format!("native-cancel-wait-{}.sqlite", std::process::id()));
        let binding = Binding {
            task_id: "task".into(),
            revision: 1,
            operation_id: "op".into(),
            policy_revision: "policy".into(),
            host_id: "host".into(),
            workspace_id: "workspace".into(),
            lease_epoch: 1,
        };
        let mut bridge = NativeBridge::open(
            &path,
            LocalPin::new("host", "workspace").unwrap(),
            binding.clone(),
            &Owner,
            || Ok(Io(Arc::new(AtomicU64::new(0)))),
            || 120,
        )
        .unwrap();
        assert!(matches!(bridge.launch(&binding), LaunchReply::Accepted(_)));
        let request = Request {
            version: crate::native_turn::VERSION.into(),
            binding,
            thread_id: "thread".into(),
            input: InputRef {
                sha256: "2d711642b726b04401627ca9fbac32f5c8530fb1903cc4db02258717921a4881".into(),
                byte_length: 1,
            },
            profile: Profile {
                kind: "read_only_text_v1".into(),
                model: "synthetic".into(),
            },
        };
        let grant = Grant(Arc::new(AtomicBool::new(true)));
        bridge
            .submit_turn(&request, &grant, &Input(Arc::new(AtomicU64::new(0))))
            .unwrap();
        bridge
            .db
            .busy_handler(Some(|attempt| {
                CANCEL_BUSY.store(true, Ordering::SeqCst);
                std::thread::sleep(Duration::from_millis(1));
                attempt < 2000
            }))
            .unwrap();
        let blocker = Connection::open(&path).unwrap();
        blocker.execute_batch("BEGIN IMMEDIATE;").unwrap();
        CANCEL_BUSY.store(false, Ordering::SeqCst);
        CANCEL_EFFECTS.store(0, Ordering::SeqCst);
        let allowed = Arc::new(AtomicBool::new(true));
        let cancel_grant = CancelGrant(allowed.clone());
        let worker = std::thread::spawn(move || {
            let request = crate::native_turn::CancelRequest {
                version: crate::native_turn::CANCEL_VERSION.into(),
                task: request,
                turn_id: "turn".into(),
            };
            bridge.cancel_turn(&request, &cancel_grant)
        });
        let deadline = Instant::now() + Duration::from_secs(2);
        while !CANCEL_BUSY.load(Ordering::SeqCst) && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(1));
        }
        let waited = CANCEL_BUSY.load(Ordering::SeqCst);
        allowed.store(false, Ordering::SeqCst);
        blocker.execute_batch("COMMIT;").unwrap();
        assert_eq!(worker.join().unwrap(), Err(Error::Denied));
        assert!(waited, "actual writer contention was not observed");
        assert_eq!(CANCEL_EFFECTS.load(Ordering::SeqCst), 0);
        drop(blocker);
        std::fs::remove_file(path).unwrap();
    }
}
