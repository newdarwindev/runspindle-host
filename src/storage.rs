//! Bounded transactional state persistence. Peer calls never execute inside a transaction.
use crate::lifecycle::{Error, Receipt, Snapshot};
use rusqlite::{params, Connection, TransactionBehavior};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, VecDeque};

const MAX_STORE_BYTES: usize = 16 * 1024 * 1024;
const LIFECYCLE_STORE_VERSION: u32 = 1;
#[derive(Clone, Default, Serialize, Deserialize)]
pub struct Stored {
    pub config: Option<(usize, usize, u64)>,
    pub entries: BTreeMap<String, Snapshot>,
    pub receipts: VecDeque<Receipt>,
}
/// CAS failure grants no permission to retry a peer effect. Refresh and reconcile.
pub trait Storage {
    fn load(&self) -> Result<(u64, Stored), Error>;
    fn compare_and_swap(&mut self, generation: u64, state: &Stored) -> Result<u64, Error>;
}
#[derive(Default)]
pub struct Memory {
    generation: u64,
    state: Stored,
}
impl Storage for Memory {
    fn load(&self) -> Result<(u64, Stored), Error> {
        Ok((self.generation, self.state.clone()))
    }
    fn compare_and_swap(&mut self, generation: u64, state: &Stored) -> Result<u64, Error> {
        if generation != self.generation {
            return Err(Error::Conflict);
        }
        self.generation = generation.checked_add(1).ok_or(Error::Capacity)?;
        self.state = state.clone();
        Ok(self.generation)
    }
}
fn storage_error(_: rusqlite::Error) -> Error {
    Error::Storage
}
/// Durable entrypoints accept ordinary filesystem paths, not SQLite connection
/// strings. Inspect raw OS bytes so ./ prefixes and non-UTF8 filenames survive.
pub(crate) fn require_durable_path(path: &std::path::Path) -> Result<(), Error> {
    let bytes = path.as_os_str().as_encoded_bytes();
    if bytes.is_empty() || bytes == b":memory:" || bytes.starts_with(b"file:") {
        return Err(Error::Unsupported);
    }
    Ok(())
}
pub struct Sqlite {
    connection: Connection,
}
impl Sqlite {
    pub fn open(path: impl AsRef<std::path::Path>) -> Result<Self, Error> {
        let path = path.as_ref();
        require_durable_path(path)?;
        let mut connection = Connection::open(path).map_err(storage_error)?;
        connection
            .busy_timeout(std::time::Duration::from_secs(5))
            .map_err(storage_error)?;
        connection
            .execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA foreign_keys=ON; PRAGMA max_page_count=8192;")
            .map_err(storage_error)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage_error)?;
        let marker: u32 = tx
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .map_err(storage_error)?;
        if marker != 0 && marker != LIFECYCLE_STORE_VERSION {
            return Err(Error::Unsupported);
        }
        let table_exists: bool = tx
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name='host_state')",
                [],
                |row| row.get(0),
            )
            .map_err(storage_error)?;
        if marker == 0 && !table_exists {
            tx.execute_batch(
                "CREATE TABLE host_state (
                    id INTEGER PRIMARY KEY CHECK(id=1),
                    version INTEGER NOT NULL CHECK(version=1),
                    generation INTEGER NOT NULL,
                    state BLOB NOT NULL CHECK(length(state)<=16777216)
                );",
            )
            .map_err(storage_error)?;
            let bytes = serde_json::to_vec(&Stored::default()).map_err(|_| Error::Storage)?;
            tx.execute("INSERT INTO host_state VALUES (1,1,0,?1)", [bytes])
                .map_err(storage_error)?;
        } else {
            // An existing schema or marker means initialization may already
            // have reserved an effect. A missing row is never an empty store.
            Self::read_state(&tx)?;
        }
        if marker == 0 {
            tx.pragma_update(None, "user_version", LIFECYCLE_STORE_VERSION)
                .map_err(storage_error)?;
        }
        tx.commit().map_err(storage_error)?;
        Ok(Self { connection })
    }
    fn read_state(connection: &Connection) -> Result<(u64, Stored), Error> {
        let (version, generation, bytes): (u64, u64, Vec<u8>) = connection
            .query_row(
                "SELECT version,generation,state FROM host_state WHERE id=1",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .map_err(storage_error)?;
        if version != 1 || bytes.len() > MAX_STORE_BYTES {
            return Err(Error::Unsupported);
        }
        let state: Stored = serde_json::from_slice(&bytes).map_err(|_| Error::Storage)?;
        if state.entries.len() > 1024 || state.receipts.len() > 4096 {
            return Err(Error::Capacity);
        }
        Ok((generation, state))
    }
}
impl Storage for Sqlite {
    fn load(&self) -> Result<(u64, Stored), Error> {
        Self::read_state(&self.connection)
    }
    fn compare_and_swap(&mut self, generation: u64, state: &Stored) -> Result<u64, Error> {
        let bytes = serde_json::to_vec(state).map_err(|_| Error::Storage)?;
        if bytes.len() > MAX_STORE_BYTES
            || state.entries.len() > 1024
            || state.receipts.len() > 4096
        {
            return Err(Error::Capacity);
        }
        let next = generation
            .checked_add(1)
            .filter(|v| *v <= i64::MAX as u64)
            .ok_or(Error::Capacity)?;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage_error)?;
        if tx.execute("UPDATE host_state SET generation=?1,state=?2 WHERE id=1 AND version=1 AND generation=?3",params![next,bytes,generation]).map_err(storage_error)? != 1 { return Err(Error::Conflict); }
        tx.commit().map_err(storage_error)?;
        Ok(next)
    }
}

#[cfg(test)]
mod durable_path_tests {
    use super::*;
    #[test]
    fn durable_paths_preserve_raw_ordinary_path_spelling() {
        for path in [
            "./:memory:",
            "./file:ordinary",
            "File:ordinary",
            "FILE:ordinary",
            "file",
            "directory/:memory:",
        ] {
            assert_eq!(require_durable_path(std::path::Path::new(path)), Ok(()));
        }
        for path in [
            "",
            ":memory:",
            "file:",
            "file:ordinary",
            "file::memory:?cache=shared",
        ] {
            assert_eq!(
                require_durable_path(std::path::Path::new(path)),
                Err(Error::Unsupported)
            );
        }
    }
}
