//! Bounded transactional state persistence. Peer calls never execute inside a transaction.
use crate::lifecycle::{Error, Receipt, Snapshot};
use rusqlite::{params, Connection, TransactionBehavior};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, VecDeque};

const MAX_STORE_BYTES: usize = 16 * 1024 * 1024;
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
pub struct Sqlite {
    connection: Connection,
}
impl Sqlite {
    pub fn open(path: impl AsRef<std::path::Path>) -> Result<Self, Error> {
        let connection = Connection::open(path).map_err(storage_error)?;
        connection
            .busy_timeout(std::time::Duration::from_secs(5))
            .map_err(storage_error)?;
        connection.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA foreign_keys=ON; PRAGMA max_page_count=8192;
            CREATE TABLE IF NOT EXISTS host_state (id INTEGER PRIMARY KEY CHECK(id=1), version INTEGER NOT NULL CHECK(version=1), generation INTEGER NOT NULL, state BLOB NOT NULL CHECK(length(state)<=16777216));").map_err(storage_error)?;
        let bytes = serde_json::to_vec(&Stored::default()).map_err(|_| Error::Storage)?;
        connection
            .execute(
                "INSERT OR IGNORE INTO host_state VALUES (1,1,0,?1)",
                [bytes],
            )
            .map_err(storage_error)?;
        Ok(Self { connection })
    }
}
impl Storage for Sqlite {
    fn load(&self) -> Result<(u64, Stored), Error> {
        let (version, generation, bytes): (u64, u64, Vec<u8>) = self
            .connection
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
