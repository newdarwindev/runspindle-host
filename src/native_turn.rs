//! Explicit native task input contract, separate from session/admission authority.
//! Resolvers are injected trusted local code; no input store or grant is installed.
use crate::lifecycle::Binding;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::io::Read;

pub const VERSION: &str = "runspindle.native-turn.v1";
pub const MAX_INPUT_BYTES: u64 = 2048;
pub const MAX_ENVELOPE_BYTES: usize = 4096;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InputRef {
    pub sha256: String,
    pub byte_length: u64,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Profile {
    pub kind: String,
    pub model: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    pub version: String,
    pub binding: Binding,
    pub thread_id: String,
    pub input: InputRef,
    pub profile: Profile,
}
fn identifier(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 128
        && s.as_bytes()[0].is_ascii_alphanumeric()
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-_.:".contains(&b))
}
impl Request {
    pub fn valid(&self) -> bool {
        self.version == VERSION
            && self.binding.valid()
            && identifier(&self.thread_id)
            && self.input.byte_length > 0
            && self.input.byte_length <= MAX_INPUT_BYTES
            && self.input.sha256.len() == 64
            && self
                .input
                .sha256
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            && self.profile.kind == "read_only_text_v1"
            && identifier(&self.profile.model)
    }
    pub fn encode(&self) -> Result<Vec<u8>, crate::lifecycle::Error> {
        use crate::lifecycle::Error;
        if !self.valid() {
            return Err(Error::Malformed);
        }
        let value = serde_json::to_value(self).map_err(|_| Error::Malformed)?;
        let bytes = serde_json::to_vec(&value).map_err(|_| Error::Malformed)?;
        if bytes.len() > MAX_ENVELOPE_BYTES {
            return Err(Error::Capacity);
        }
        Ok(bytes)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, crate::lifecycle::Error> {
        use crate::lifecycle::Error;
        if bytes.len() > MAX_ENVELOPE_BYTES {
            return Err(Error::Capacity);
        }
        let value = crate::native::unique_json(bytes).map_err(|_| Error::Malformed)?;
        let binding = value
            .get("binding")
            .and_then(|b| b.as_object())
            .ok_or(Error::Malformed)?;
        if binding.len() != 7 {
            return Err(Error::Malformed);
        }
        let request: Self = serde_json::from_value(value).map_err(|_| Error::Malformed)?;
        if request.encode()?.as_slice() != bytes {
            return Err(Error::Malformed);
        }
        Ok(request)
    }
}
/// Must verify every field of this exact request, independently of Admission grants.
/// Implementations are trusted bounded callbacks; they must not reenter the journal.
pub trait Verifier {
    fn verify_turn(&self, request: &Request) -> bool;
}
#[derive(Default)]
pub struct Deny;
impl Verifier for Deny {
    fn verify_turn(&self, _: &Request) -> bool {
        false
    }
}
/// Returns an actual bounded reader for the exact content address, not a URL/path.
/// The bridge rehashes the bytes; opening a reader is not evidence of authorization.
pub trait InputResolver {
    fn open(&self, input: &InputRef) -> std::io::Result<Box<dyn Read>>;
}
pub(crate) fn resolve(
    resolver: &impl InputResolver,
    input: &InputRef,
) -> Result<String, crate::lifecycle::Error> {
    use crate::lifecycle::Error;
    let mut bytes = Vec::new();
    resolver
        .open(input)
        .map_err(|_| Error::Unknown)?
        .take(MAX_INPUT_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| Error::Unknown)?;
    if bytes.len() as u64 != input.byte_length
        || format!("{:x}", Sha256::digest(&bytes)) != input.sha256
    {
        return Err(Error::WrongBinding);
    }
    String::from_utf8(bytes).map_err(|_| Error::Malformed)
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Reservation {
    pub request: Request,
    pub turn_id: Option<String>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome {
    Acknowledged { turn_id: String },
    Unknown,
}

/// A single dispatch capability minted only by the bridge after its durable
/// reservation, owner fence and exact-request verification. Not Clone or Deserialize.
/// External callers cannot construct one to bypass those checks:
/// ```compile_fail
/// use runspindle_host::native_turn::TurnPermit;
/// let permit = TurnPermit { thread_id: "thread", text: "input", model: "model" };
/// ```
pub struct TurnPermit<'a> {
    thread_id: &'a str,
    text: &'a str,
    model: &'a str,
}
impl<'a> TurnPermit<'a> {
    pub(crate) fn new(request: &'a Request, text: &'a str) -> Self {
        Self {
            thread_id: &request.thread_id,
            text,
            model: &request.profile.model,
        }
    }
    pub fn thread_id(&self) -> &str {
        self.thread_id
    }
    pub fn text(&self) -> &str {
        self.text
    }
    pub fn model(&self) -> &str {
        self.model
    }
}
