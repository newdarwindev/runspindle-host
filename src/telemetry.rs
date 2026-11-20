//! Lossy metadata-only observation. No exporter callback executes on the native path.
use std::collections::VecDeque;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Mutex,
};
use std::time::SystemTime;

pub const VERSION: &str = "runspindle.telemetry.v1";
pub const CAPACITY: usize = 128;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Operation {
    Handshake,
    ThreadStart,
    ThreadRead,
    TurnStart,
    Interrupt,
    Shutdown,
    OtherRpc,
}
impl Operation {
    pub(crate) fn rpc(method: &str) -> Self {
        match method {
            "initialize" => Self::Handshake,
            "thread/start" => Self::ThreadStart,
            "thread/read" => Self::ThreadRead,
            "turn/start" => Self::TurnStart,
            "turn/interrupt" => Self::Interrupt,
            _ => Self::OtherRpc,
        }
    }
    pub fn name(self) -> &'static str {
        match self {
            Self::Handshake => "host.native.handshake",
            Self::ThreadStart => "host.native.thread_start",
            Self::ThreadRead => "host.native.thread_read",
            Self::TurnStart => "host.native.turn_start",
            Self::Interrupt => "host.native.interrupt",
            Self::Shutdown => "host.native.shutdown",
            Self::OtherRpc => "host.native.rpc",
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    Acknowledged,
    Error,
    Observed,
    Unknown,
}
impl Outcome {
    pub fn name(self) -> &'static str {
        match self {
            Self::Acknowledged => "acknowledged",
            Self::Error => "error",
            Self::Observed => "observed",
            Self::Unknown => "unknown",
        }
    }
}
#[derive(Clone, Copy, Debug)]
pub struct Observation {
    pub operation: Operation,
    pub outcome: Outcome,
    pub started: SystemTime,
    pub ended: SystemTime,
}
/// A fixed-capacity, best-effort buffer. Contention/poison/full drops telemetry,
/// never blocks native work. Drop does not flush or run user code.
pub struct Buffer {
    entries: Mutex<VecDeque<Observation>>,
    dropped: AtomicU64,
}
impl Default for Buffer {
    fn default() -> Self {
        Self {
            entries: Mutex::new(VecDeque::with_capacity(CAPACITY)),
            dropped: AtomicU64::new(0),
        }
    }
}
impl Buffer {
    pub(crate) fn record(&self, observation: Observation) {
        if let Ok(mut entries) = self.entries.try_lock() {
            if entries.len() < CAPACITY {
                entries.push_back(observation);
                return;
            }
        }
        self.dropped.fetch_add(1, Ordering::Relaxed);
    }
    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
    /// Caller-controlled, nonblocking drain. Export ONLY outside native execution.
    pub fn drain(&self) -> Vec<Observation> {
        self.entries
            .try_lock()
            .map(|mut entries| entries.drain(..).collect())
            .unwrap_or_default()
    }
}
/// Convert one completed observation through the actual OpenTelemetry API.
/// Caller owns provider/exporter, deadlines and privacy of its Resource. This
/// explicit call may run exporter code; it is NEVER invoked by StdioSession.
/// A fresh root context deliberately excludes ambient baggage and private IDs.
#[cfg(feature = "otel")]
pub fn export<T: opentelemetry::trace::Tracer>(tracer: &T, observation: Observation) {
    use opentelemetry::{
        trace::{Span, SpanBuilder},
        Context, KeyValue,
    };
    let builder = SpanBuilder::from_name(observation.operation.name())
        .with_start_time(observation.started)
        .with_attributes([
            KeyValue::new("runspindle.telemetry.version", VERSION),
            KeyValue::new("runspindle.component", "host"),
            KeyValue::new("runspindle.outcome", observation.outcome.name()),
        ]);
    let mut span = tracer.build_with_context(builder, &Context::new());
    span.end_with_timestamp(observation.ended.max(observation.started));
}
#[cfg(test)]
mod tests {
    use super::*;
    fn entry() -> Observation {
        Observation {
            operation: Operation::Interrupt,
            outcome: Outcome::Acknowledged,
            started: SystemTime::now(),
            ended: SystemTime::now(),
        }
    }
    #[test]
    fn capacity_and_contention_drop_without_blocking() {
        let buffer = Buffer::default();
        for _ in 0..CAPACITY + 1 {
            buffer.record(entry());
        }
        assert_eq!(buffer.dropped(), 1);
        assert_eq!(buffer.drain().len(), CAPACITY);
        let _guard = buffer.entries.lock().unwrap();
        buffer.record(entry());
        assert_eq!(buffer.dropped(), 2);
        assert!(buffer.drain().is_empty());
    }
    #[test]
    fn poisoned_buffer_remains_lossy_not_a_task_failure() {
        let buffer = Buffer::default();
        let _ = std::panic::catch_unwind(|| {
            let _guard = buffer.entries.lock().unwrap();
            panic!("synthetic observer fault");
        });
        buffer.record(entry());
        assert_eq!(buffer.dropped(), 1);
        assert!(buffer.drain().is_empty());
    }
    #[test]
    fn unknown_method_never_copies_input() {
        assert_eq!(
            Operation::rpc("PRIVATE_TOKEN=/private/path"),
            Operation::OtherRpc
        );
        assert_eq!(Operation::rpc("turn/interrupt"), Operation::Interrupt);
    }
    #[cfg(feature = "otel")]
    #[test]
    fn actual_sdk_exports_allowlisted_span_and_no_ambient_context() {
        use opentelemetry::trace::TracerProvider;
        use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider};
        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        let tracer = provider.tracer("runspindle-host");
        use opentelemetry::{
            trace::{SpanContext, SpanId, TraceContextExt, TraceFlags, TraceId, TraceState},
            Context,
        };
        let private_parent = SpanContext::new(
            TraceId::from(123u128),
            SpanId::from(456u64),
            TraceFlags::SAMPLED,
            true,
            TraceState::default(),
        );
        let _ambient = Context::new()
            .with_remote_span_context(private_parent)
            .attach();
        export(&tracer, entry());
        let spans = exporter.get_finished_spans().unwrap();
        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0].name, "host.native.interrupt");
        assert_eq!(spans[0].attributes.len(), 3);
        assert_eq!(
            spans[0].parent_span_id,
            opentelemetry::trace::SpanId::INVALID
        );
        assert_eq!(spans[0].attributes[2].value.as_str(), "acknowledged");
    }
}
