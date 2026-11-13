use runspindle_host::lifecycle::*;
use std::collections::VecDeque;

fn binding() -> Binding {
    Binding {
        task_id: "synthetic-task".into(),
        revision: 1,
        operation_id: "synthetic-operation".into(),
        policy_revision: "synthetic-policy-v1".into(),
        host_id: "host-a".into(),
        workspace_id: "workspace-a".into(),
        lease_epoch: 1,
    }
}
fn request(b: &Binding) -> Request {
    Request {
        version: VERSION.into(),
        binding: b.clone(),
    }
}
fn presence(reachability: Reachability) -> Presence {
    Presence {
        host_id: "host-a".into(),
        workspace_id: "workspace-a".into(),
        reachability,
        observed_at_ms: 90,
        valid_until_ms: 200,
    }
}
fn observation(b: &Binding, seq: u64, terminal: Option<Terminal>) -> Observation {
    Observation {
        binding: b.clone(),
        native_session_id: "observed-native-session".into(),
        sequence: seq,
        observed_at_ms: 100 + seq,
        terminal,
    }
}
fn event(b: &Binding, seq: u64, terminal: Option<Terminal>) -> Event {
    Event {
        version: VERSION.into(),
        observation: observation(b, seq, terminal),
        kind: terminal.map_or(EventKind::Progress, EventKind::Terminal),
        encoded_size: 256,
    }
}
struct Grant {
    binding: Binding,
    allow_cancel: bool,
}
impl Verifier for Grant {
    fn verify(&self, b: &Binding, action: Action) -> bool {
        b == &self.binding && (action != Action::Cancel || self.allow_cancel)
    }
}
fn adapter() -> Adapter<Grant> {
    Adapter::new(
        Grant {
            binding: binding(),
            allow_cancel: true,
        },
        8,
        32,
        50,
    )
    .unwrap()
}
#[derive(Default)]
struct ScriptedPeer {
    launches: VecDeque<LaunchReply>,
    discoveries: VecDeque<Vec<Observation>>,
    cancels: VecDeque<CancelReply>,
    calls: Vec<String>,
    observed_effects: Vec<Binding>,
}
impl Peer for ScriptedPeer {
    fn launch(&mut self, b: &Binding) -> LaunchReply {
        self.calls
            .push(format!("launch:{}:{}", b.host_id, b.operation_id));
        let reply = self.launches.pop_front().expect("unexpected launch");
        if !matches!(reply, LaunchReply::Rejected) {
            self.observed_effects.push(b.clone());
        }
        reply
    }
    fn discover(&mut self, b: &Binding) -> Vec<Observation> {
        self.calls
            .push(format!("discover:{}:{}", b.host_id, b.operation_id));
        self.discoveries.pop_front().expect("unexpected discovery")
    }
    fn cancel(&mut self, b: &Binding, s: &str) -> CancelReply {
        self.calls
            .push(format!("cancel:{}:{}:{s}", b.host_id, b.operation_id));
        self.cancels.pop_front().expect("unexpected cancel")
    }
}
fn accepted() -> ScriptedPeer {
    ScriptedPeer {
        launches: [LaunchReply::Accepted(observation(&binding(), 1, None))].into(),
        ..Default::default()
    }
}

#[test]
fn default_denial_and_wrong_grant_do_not_dispatch() {
    let b = binding();
    let mut peer = ScriptedPeer::default();
    let mut denied = Adapter::denied(8, 8, 50).unwrap();
    assert_eq!(
        denied.submit(request(&b), &presence(Reachability::Online), 110, &mut peer),
        Err(Error::Denied)
    );
    let mut a = adapter();
    let mut wrong = b.clone();
    wrong.lease_epoch += 1;
    assert_eq!(
        a.submit(
            request(&wrong),
            &presence(Reachability::Online),
            110,
            &mut peer
        ),
        Err(Error::Denied)
    );
    assert!(peer.calls.is_empty());
    assert!(a.drain().is_empty());
}
#[test]
fn malformed_envelopes_never_call_peer() {
    let b = binding();
    let mut peer = ScriptedPeer::default();
    let mut a = adapter();
    let mut r = request(&b);
    r.version = "unknown".into();
    assert_eq!(
        a.submit(r, &presence(Reachability::Online), 110, &mut peer),
        Err(Error::Malformed)
    );
    for invalid in ["", "_leading", "bad/path", "space here", "日本語"] {
        let mut r = request(&b);
        r.binding.task_id = invalid.into();
        assert_eq!(
            a.submit(r, &presence(Reachability::Online), 110, &mut peer),
            Err(Error::Malformed)
        );
    }
    let mut r = request(&b);
    r.binding.operation_id = "x".repeat(129);
    assert_eq!(
        a.submit(r, &presence(Reachability::Online), 110, &mut peer),
        Err(Error::Malformed)
    );
    assert!(peer.calls.is_empty());
}
#[test]
fn offline_unknown_stale_future_and_wrong_host_pins_queue_without_failover() {
    let b = binding();
    let mut stale = presence(Reachability::Online);
    stale.observed_at_ms = 0;
    let mut future = presence(Reachability::Online);
    future.observed_at_ms = 111;
    let mut expired = presence(Reachability::Online);
    expired.valid_until_ms = 109;
    let mut host = presence(Reachability::Online);
    host.host_id = "host-b".into();
    let mut workspace = presence(Reachability::Online);
    workspace.workspace_id = "workspace-b".into();
    for p in [
        presence(Reachability::Offline),
        presence(Reachability::Unknown),
        stale,
        future,
        expired,
        host,
        workspace,
    ] {
        let mut a = adapter();
        let mut peer = accepted();
        assert_eq!(
            a.submit(request(&b), &p, 110, &mut peer).unwrap().execution,
            Execution::Queued
        );
        assert!(peer.calls.is_empty());
        a.dispatch(&b, &presence(Reachability::Online), 110, &mut peer)
            .unwrap();
        assert_eq!(peer.observed_effects.as_slice(), std::slice::from_ref(&b));
    }
}
#[test]
fn accepted_lost_reply_reconciles_once_without_second_launch() {
    let b = binding();
    let mut a = adapter();
    let mut peer = ScriptedPeer {
        launches: [LaunchReply::LostReply].into(),
        discoveries: [vec![observation(&b, 1, None)]].into(),
        ..Default::default()
    };
    let lost = a
        .submit(request(&b), &presence(Reachability::Online), 110, &mut peer)
        .unwrap();
    assert_eq!(lost.execution, Execution::Unknown);
    assert_eq!(lost.native_session_id, None);
    assert_eq!(
        a.submit(request(&b), &presence(Reachability::Online), 110, &mut peer)
            .unwrap(),
        lost
    );
    assert_eq!(
        a.dispatch(&b, &presence(Reachability::Online), 110, &mut peer)
            .unwrap(),
        lost
    );
    let found = a.reconcile(&b, 110, &mut peer).unwrap();
    assert_eq!(
        found.native_session_id.as_deref(),
        Some("observed-native-session")
    );
    assert_eq!(found.execution, Execution::Acknowledged);
    assert_eq!(
        peer.calls,
        [
            "launch:host-a:synthetic-operation",
            "discover:host-a:synthetic-operation"
        ]
    );
    assert_eq!(peer.observed_effects, [b]);
}
#[test]
fn absent_and_ambiguous_discovery_do_not_authorize_retry() {
    let b = binding();
    let mut a = adapter();
    let mut peer = ScriptedPeer {
        launches: [LaunchReply::LostReply].into(),
        discoveries: [
            vec![],
            vec![observation(&b, 1, None), observation(&b, 2, None)],
        ]
        .into(),
        ..Default::default()
    };
    a.submit(request(&b), &presence(Reachability::Online), 110, &mut peer)
        .unwrap();
    assert_eq!(a.reconcile(&b, 110, &mut peer), Err(Error::Unknown));
    assert_eq!(a.reconcile(&b, 110, &mut peer), Err(Error::Ambiguous));
    assert_eq!(
        a.dispatch(&b, &presence(Reachability::Online), 110, &mut peer)
            .unwrap()
            .execution,
        Execution::Unknown
    );
    assert_eq!(peer.observed_effects.len(), 1);
    assert_eq!(peer.calls.len(), 3);
}
#[test]
fn malformed_accepted_binding_stays_unknown_and_cannot_relaunch() {
    let b = binding();
    let mut wrong = observation(&b, 1, None);
    wrong.binding.workspace_id = "wrong-workspace".into();
    let mut peer = ScriptedPeer {
        launches: [LaunchReply::Accepted(wrong)].into(),
        ..Default::default()
    };
    let mut a = adapter();
    assert_eq!(
        a.submit(request(&b), &presence(Reachability::Online), 110, &mut peer)
            .unwrap()
            .execution,
        Execution::Unknown
    );
    a.dispatch(&b, &presence(Reachability::Online), 110, &mut peer)
        .unwrap();
    assert_eq!(peer.calls.len(), 1);
}
#[test]
fn reconnect_preserves_observed_session_and_every_binding_field() {
    let b = binding();
    let mut a = adapter();
    let mut peer = accepted();
    let initial = a
        .submit(request(&b), &presence(Reachability::Online), 110, &mut peer)
        .unwrap();
    let mut wrongs = vec![];
    for field in 0..8 {
        let mut o = observation(&b, 2, None);
        match field {
            0 => o.binding.task_id += "x",
            1 => o.binding.revision += 1,
            2 => o.binding.operation_id += "x",
            3 => o.binding.policy_revision += "x",
            4 => o.binding.host_id += "x",
            5 => o.binding.workspace_id += "x",
            6 => o.binding.lease_epoch += 1,
            _ => o.native_session_id += "x",
        }
        wrongs.push(vec![o]);
    }
    peer.discoveries = wrongs.into();
    for _ in 0..8 {
        assert_eq!(a.reconcile(&b, 110, &mut peer), Err(Error::WrongBinding));
        assert_eq!(a.snapshot(&b).unwrap(), initial);
    }
    peer.discoveries.push_back(vec![observation(&b, 2, None)]);
    let recovered = a.reconcile(&b, 110, &mut peer).unwrap();
    assert_eq!(recovered.native_session_id, initial.native_session_id);
    assert_eq!(peer.observed_effects.len(), 1);
}
#[test]
fn stale_reordered_future_and_unsupported_events_do_not_mutate() {
    let b = binding();
    let mut a = adapter();
    let mut peer = accepted();
    let initial = a
        .submit(request(&b), &presence(Reachability::Online), 110, &mut peer)
        .unwrap();
    for e in [event(&b, 1, None), event(&b, 2, None)] {
        let now = if e.observation.sequence == 1 {
            110
        } else {
            1000
        };
        assert_eq!(a.event(&b, e, now), Err(Error::Stale));
    }
    assert_eq!(a.event(&b, event(&b, 20, None), 110), Err(Error::Stale));
    let mut e = event(&b, 2, None);
    e.kind = EventKind::Unsupported;
    assert_eq!(a.event(&b, e, 110), Err(Error::Unsupported));
    let mut e = event(&b, 2, None);
    e.encoded_size = MAX_EVENT_BYTES + 1;
    assert_eq!(a.event(&b, e, 110), Err(Error::Malformed));
    let mut e = event(&b, 2, None);
    e.version = "old".into();
    assert_eq!(a.event(&b, e, 110), Err(Error::Malformed));
    assert_eq!(a.snapshot(&b).unwrap(), initial);
    assert_eq!(peer.calls.len(), 1);
}
#[test]
fn cancellation_ack_is_not_terminal_and_completion_race_keeps_actual_outcome() {
    let b = binding();
    let mut a = adapter();
    let mut peer = accepted();
    peer.cancels.push_back(CancelReply::Acknowledged);
    a.submit(request(&b), &presence(Reachability::Online), 110, &mut peer)
        .unwrap();
    let cancelled = a.cancel(&b, &mut peer).unwrap();
    assert_eq!(cancelled.cancellation, Cancellation::Acknowledged);
    assert_eq!(cancelled.execution, Execution::Acknowledged);
    assert_eq!(a.cancel(&b, &mut peer).unwrap(), cancelled);
    let done = a
        .event(&b, event(&b, 2, Some(Terminal::Completed)), 110)
        .unwrap();
    assert_eq!(done.execution, Execution::Terminal(Terminal::Completed));
    assert_eq!(done.cancellation, Cancellation::ObservedTerminal);
    assert_eq!(
        a.event(&b, event(&b, 2, Some(Terminal::Completed)), 110)
            .unwrap(),
        done
    );
    assert_eq!(a.event(&b, event(&b, 3, None), 110), Err(Error::Conflict));
    assert_eq!(
        a.event(&b, event(&b, 3, Some(Terminal::Cancelled)), 110),
        Err(Error::Conflict)
    );
    assert_eq!(a.snapshot(&b).unwrap(), done);
    assert_eq!(peer.calls.len(), 2);
}
#[test]
fn cancel_before_start_forbids_launch_and_is_not_native_terminal() {
    let b = binding();
    let mut a = adapter();
    let mut peer = ScriptedPeer::default();
    a.submit(
        request(&b),
        &presence(Reachability::Offline),
        110,
        &mut peer,
    )
    .unwrap();
    let cancelled = a.cancel(&b, &mut peer).unwrap();
    assert_eq!(cancelled.execution, Execution::CancelledBeforeDispatch);
    assert_eq!(cancelled.cancellation, Cancellation::Requested);
    assert_eq!(cancelled.native_session_id, None);
    assert_eq!(
        a.dispatch(&b, &presence(Reachability::Online), 110, &mut peer)
            .unwrap(),
        cancelled
    );
    assert!(peer.calls.is_empty());
}
#[test]
fn cancel_lost_reply_does_not_retry_and_unknown_launch_remains_reconcilable() {
    let b = binding();
    let mut a = adapter();
    let mut peer = ScriptedPeer {
        launches: [LaunchReply::LostReply].into(),
        discoveries: [
            vec![observation(&b, 1, None)],
            vec![observation(&b, 2, Some(Terminal::Cancelled))],
        ]
        .into(),
        cancels: [CancelReply::LostReply].into(),
        ..Default::default()
    };
    a.submit(request(&b), &presence(Reachability::Online), 110, &mut peer)
        .unwrap();
    assert_eq!(
        a.cancel(&b, &mut peer).unwrap().cancellation,
        Cancellation::Requested
    );
    assert_eq!(peer.calls.len(), 1);
    a.reconcile(&b, 110, &mut peer).unwrap();
    let pending = a.cancel(&b, &mut peer).unwrap();
    assert!(pending.cancel_dispatched);
    assert_eq!(pending.cancellation, Cancellation::Requested);
    assert_eq!(a.cancel(&b, &mut peer).unwrap(), pending);
    let terminal = a.reconcile(&b, 110, &mut peer).unwrap();
    assert_eq!(terminal.execution, Execution::Terminal(Terminal::Cancelled));
    assert_eq!(terminal.cancellation, Cancellation::ObservedTerminal);
    assert_eq!(peer.calls.len(), 4);
    assert_eq!(peer.observed_effects.len(), 1);
}
#[test]
fn denied_cancel_has_zero_forbidden_calls() {
    let b = binding();
    let mut a = Adapter::new(
        Grant {
            binding: b.clone(),
            allow_cancel: false,
        },
        8,
        8,
        50,
    )
    .unwrap();
    let mut peer = accepted();
    a.submit(request(&b), &presence(Reachability::Online), 110, &mut peer)
        .unwrap();
    assert_eq!(a.cancel(&b, &mut peer), Err(Error::Denied));
    assert_eq!(peer.calls.len(), 1);
}
#[test]
fn queue_backpressure_precedes_effect_and_terminal_is_retryable_after_drain() {
    let b = binding();
    let mut a = Adapter::new(
        Grant {
            binding: b.clone(),
            allow_cancel: true,
        },
        1,
        1,
        50,
    )
    .unwrap();
    let mut peer = accepted();
    a.submit(
        request(&b),
        &presence(Reachability::Offline),
        110,
        &mut peer,
    )
    .unwrap();
    assert_eq!(
        a.dispatch(&b, &presence(Reachability::Online), 110, &mut peer),
        Err(Error::Backpressure)
    );
    assert!(peer.calls.is_empty());
    a.drain();
    a.dispatch(&b, &presence(Reachability::Online), 110, &mut peer)
        .unwrap();
    let terminal = event(&b, 2, Some(Terminal::Completed));
    assert_eq!(a.event(&b, terminal.clone(), 110), Err(Error::Backpressure));
    assert_eq!(a.snapshot(&b).unwrap().execution, Execution::Acknowledged);
    a.drain();
    assert_eq!(
        a.event(&b, terminal, 110).unwrap().execution,
        Execution::Terminal(Terminal::Completed)
    );
    assert_eq!(peer.calls.len(), 1);
}
struct AllSynthetic;
impl Verifier for AllSynthetic {
    fn verify(&self, _: &Binding, _: Action) -> bool {
        true
    }
}
#[test]
fn capacity_keeps_known_duplicates_and_rejects_conflicts_without_eviction() {
    let b = binding();
    let mut a = Adapter::new(AllSynthetic, 1, 8, 50).unwrap();
    let mut peer = accepted();
    let first = a
        .submit(request(&b), &presence(Reachability::Online), 110, &mut peer)
        .unwrap();
    assert_eq!(
        a.submit(request(&b), &presence(Reachability::Online), 110, &mut peer)
            .unwrap(),
        first
    );
    let mut other = b.clone();
    other.operation_id += "-second";
    assert_eq!(
        a.submit(
            request(&other),
            &presence(Reachability::Online),
            110,
            &mut peer
        ),
        Err(Error::Capacity)
    );
    let mut conflict = b.clone();
    conflict.workspace_id += "-wrong";
    assert_eq!(
        a.submit(
            request(&conflict),
            &presence(Reachability::Online),
            110,
            &mut peer
        ),
        Err(Error::Conflict)
    );
    assert_eq!(a.snapshot(&b).unwrap(), first);
    assert_eq!(peer.calls.len(), 1);
}
#[test]
fn shutdown_forbids_dispatch_and_capabilities_stay_explicit() {
    let b = binding();
    let mut a = adapter();
    let mut peer = ScriptedPeer::default();
    a.shutdown();
    assert_eq!(
        a.submit(request(&b), &presence(Reachability::Online), 110, &mut peer),
        Err(Error::Shutdown)
    );
    assert!(peer.calls.is_empty());
    for os in ["macos", "linux"] {
        let c = Capabilities::scripted(os).unwrap();
        assert_eq!(c.lifecycle, Support::Supported);
        assert_eq!(
            (c.capture, c.text_transfer, c.submission),
            (
                Support::Unsupported,
                Support::Unsupported,
                Support::Unsupported
            )
        );
    }
    assert_eq!(Capabilities::scripted("other"), Err(Error::Unsupported));
}

#[test]
fn explicit_rejection_does_not_retry_and_failed_terminal_is_preserved() {
    let b = binding();
    let mut a = adapter();
    let mut peer = ScriptedPeer {
        launches: [LaunchReply::Rejected].into(),
        ..Default::default()
    };
    let rejected = a
        .submit(request(&b), &presence(Reachability::Online), 110, &mut peer)
        .unwrap();
    assert_eq!(rejected.execution, Execution::Rejected);
    assert_eq!(
        a.dispatch(&b, &presence(Reachability::Online), 110, &mut peer)
            .unwrap(),
        rejected
    );
    assert_eq!(peer.calls.len(), 1);
    assert!(peer.observed_effects.is_empty());
    let mut a = adapter();
    let mut peer = accepted();
    a.submit(request(&b), &presence(Reachability::Online), 110, &mut peer)
        .unwrap();
    let failed = a
        .event(&b, event(&b, 2, Some(Terminal::Failed)), 110)
        .unwrap();
    assert_eq!(failed.execution, Execution::Terminal(Terminal::Failed));
    assert_eq!(a.cancel(&b, &mut peer).unwrap(), failed);
    assert_eq!(peer.calls.len(), 1);
}
#[test]
fn peer_panic_retains_scheduled_identity_and_forbids_blind_retry() {
    struct Panics {
        calls: usize,
    }
    impl Peer for Panics {
        fn launch(&mut self, _: &Binding) -> LaunchReply {
            self.calls += 1;
            panic!("synthetic disconnect");
        }
        fn discover(&mut self, _: &Binding) -> Vec<Observation> {
            vec![]
        }
        fn cancel(&mut self, _: &Binding, _: &str) -> CancelReply {
            panic!("unexpected cancel");
        }
    }
    let b = binding();
    let mut a = adapter();
    let mut peer = Panics { calls: 0 };
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| a.submit(
            request(&b),
            &presence(Reachability::Online),
            110,
            &mut peer
        )))
        .is_err()
    );
    assert_eq!(a.snapshot(&b).unwrap().execution, Execution::Scheduled);
    a.submit(request(&b), &presence(Reachability::Online), 110, &mut peer)
        .unwrap();
    a.dispatch(&b, &presence(Reachability::Online), 110, &mut peer)
        .unwrap();
    assert_eq!(peer.calls, 1);
    assert_eq!(a.reconcile(&b, 110, &mut peer), Err(Error::Unknown));
}

#[test]
fn cancel_peer_panic_retains_requested_state_without_resending() {
    struct Panics {
        calls: usize,
    }
    impl Peer for Panics {
        fn launch(&mut self, b: &Binding) -> LaunchReply {
            LaunchReply::Accepted(observation(b, 1, None))
        }
        fn discover(&mut self, _: &Binding) -> Vec<Observation> {
            vec![]
        }
        fn cancel(&mut self, _: &Binding, _: &str) -> CancelReply {
            self.calls += 1;
            panic!("synthetic lost cancellation reply");
        }
    }
    let b = binding();
    let mut a = adapter();
    let mut peer = Panics { calls: 0 };
    a.submit(request(&b), &presence(Reachability::Online), 110, &mut peer)
        .unwrap();
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| a.cancel(&b, &mut peer))).is_err()
    );
    let pending = a.snapshot(&b).unwrap();
    assert_eq!(pending.cancellation, Cancellation::Requested);
    assert!(pending.cancel_dispatched);
    assert_eq!(a.cancel(&b, &mut peer).unwrap(), pending);
    assert_eq!(peer.calls, 1);
}
