//! The sample store: one `SQLite` database shared by the collector and every
//! reader.
//!
//! The collector is the only process that records samples, and it holds an
//! exclusive lock for as long as it runs. Readers open the same file at any
//! time and see a consistent history while it grows.

use std::cmp::Ordering;
use std::fs::{self, File, TryLockError};
use std::io;
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use rusqlite::types::Type;
use rusqlite::{Connection, OpenFlags, OptionalExtension, Row, TransactionBehavior, params};

use crate::network::SessionNetwork;
use crate::prober::{LossReason, ProbeOutcome};
use crate::time::{TimeRange, Timestamp, micros};

/// Schema migrations in order. A store's `user_version` is the number of
/// entries already applied to it.
const MIGRATIONS: &[&str] = &[include_str!("store/v1.sql"), include_str!("store/v2.sql")];

/// The schema version this build reads and writes.
#[expect(clippy::cast_possible_truncation, reason = "a handful of migrations")]
const SCHEMA_VERSION: u32 = MIGRATIONS.len() as u32;

/// How long a connection waits for another process's write to finish.
const BUSY_TIMEOUT: Duration = Duration::from_secs(5);

/// Rows removed per transaction while pruning, so a running collector is never
/// locked out for long.
const PRUNE_CHUNK: u32 = 50_000;

/// A failure to open, read, or write the store.
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    /// There is no store at this path yet.
    #[error("no pingr data at {}: start collecting with `pingr run <target>`", .0.display())]
    NotFound(PathBuf),
    /// Another collector is already recording into this store.
    #[error("another pingr collector is already recording into {}", .0.display())]
    AlreadyCollecting(PathBuf),
    /// The store was written by a newer pingr than this one.
    #[error(
        "the store has schema version {found}, but this pingr only understands up to {supported}"
    )]
    SchemaTooNew {
        /// The store's schema version.
        found: u32,
        /// The newest version this build knows.
        supported: u32,
    },
    /// The store was written by an older pingr and has not been upgraded.
    #[error(
        "the store has schema version {found}, older than this pingr's {current}: \
         start `pingr run` once to upgrade it"
    )]
    SchemaTooOld {
        /// The store's schema version.
        found: u32,
        /// The version this build uses.
        current: u32,
    },
    /// No target with this name has ever been recorded.
    #[error("no data for target `{0}`")]
    UnknownTarget(String),
    /// No network has this label.
    #[error("no network labeled `{0}`")]
    UnknownNetwork(String),
    /// Another network already has this label.
    #[error("another network is already labeled `{0}`")]
    LabelTaken(String),
    /// The database rejected an operation.
    #[error("store error")]
    Sqlite(#[from] rusqlite::Error),
    /// A file operation next to the database failed.
    #[error("store file error")]
    Io(#[from] io::Error),
}

/// Identifies a target within one store.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TargetId(i64);

/// Identifies one resolved address of a target within one store.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AddressId(i64);

/// Identifies one uninterrupted collector run within one store.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SessionId(i64);

/// Identifies a network within one store.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NetworkId(i64);

/// What became of one probe, as stored.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// The destination answered after `rtt`.
    Reply {
        /// Round-trip time.
        rtt: Duration,
    },
    /// The probe went unanswered.
    Lost(Loss),
}

/// Why a stored probe counts as lost.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Loss {
    /// Nothing came back within the timeout.
    Timeout,
    /// The network reported the destination as unreachable.
    Unreachable,
    /// The probe failed in a way that could not be classified.
    Other,
    /// The target's name did not resolve, so nothing could be sent.
    Unresolved,
}

impl Loss {
    fn code(self) -> i64 {
        match self {
            Self::Timeout => 1,
            Self::Unreachable => 2,
            Self::Other => 3,
            Self::Unresolved => 4,
        }
    }

    fn from_code(code: i64) -> Option<Self> {
        match code {
            1 => Some(Self::Timeout),
            2 => Some(Self::Unreachable),
            3 => Some(Self::Other),
            4 => Some(Self::Unresolved),
            _ => None,
        }
    }
}

impl From<ProbeOutcome> for Outcome {
    fn from(outcome: ProbeOutcome) -> Self {
        match outcome {
            ProbeOutcome::Reply { rtt } => Self::Reply { rtt },
            ProbeOutcome::Lost(LossReason::Timeout) => Self::Lost(Loss::Timeout),
            ProbeOutcome::Lost(LossReason::Unreachable) => Self::Lost(Loss::Unreachable),
            ProbeOutcome::Lost(LossReason::Other) => Self::Lost(Loss::Other),
        }
    }
}

/// A probe result on its way into the store.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NewSample {
    /// The collector run that sent the probe.
    pub session: SessionId,
    /// The target that was probed.
    pub target: TargetId,
    /// Wall-clock time the probe was sent.
    pub sent: Timestamp,
    /// The probe's position in its session, per target, counting from zero.
    pub seq: u32,
    /// The address probed, or `None` if the name did not resolve.
    pub address: Option<AddressId>,
    /// What came back.
    pub outcome: Outcome,
}

/// A probe result read back from the store.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Sample {
    /// Wall-clock time the probe was sent.
    pub sent: Timestamp,
    /// The collector run that sent it.
    pub session: SessionId,
    /// The network that run was on, if it could be told.
    pub network: Option<NetworkId>,
    /// The probe's position in its session, per target, counting from zero.
    pub seq: u32,
    /// The address probed, or `None` if the name did not resolve.
    pub address: Option<IpAddr>,
    /// What came back.
    pub outcome: Outcome,
}

/// The settings a collector run was started with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionSettings {
    /// Time between probes to one target.
    pub interval: Duration,
    /// How long a probe may go unanswered before it counts as lost.
    pub timeout: Duration,
}

/// Why a collector run ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EndReason {
    /// The collector was asked to stop.
    Stopped,
    /// The wall clock and the monotonic clock diverged (suspend, clock change),
    /// so the run was cut and a new one begun.
    ClockJump,
    /// The collector went much longer than one interval without probing, so
    /// the run was cut and a new one begun.
    Stalled,
    /// The machine moved to another network, so the run was cut and a new one
    /// begun.
    NetworkChanged,
}

impl EndReason {
    fn as_str(self) -> &'static str {
        match self {
            Self::Stopped => "stopped",
            Self::ClockJump => "clock-jump",
            Self::Stalled => "stalled",
            Self::NetworkChanged => "network-change",
        }
    }

    fn parse(text: &str) -> Option<Self> {
        match text {
            "stopped" => Some(Self::Stopped),
            "clock-jump" => Some(Self::ClockJump),
            "stalled" => Some(Self::Stalled),
            "network-change" => Some(Self::NetworkChanged),
            _ => None,
        }
    }
}

/// One uninterrupted collector run.
///
/// Time not covered by any session is time the collector was not running:
/// there is no data for it, which is different from loss.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Session {
    /// The session's identity.
    pub id: SessionId,
    /// The network it ran on, if it could be told.
    pub network: Option<NetworkId>,
    /// When collection began.
    pub started: Timestamp,
    /// The last moment the collector is known to have been recording.
    pub last_alive: Timestamp,
    /// When and why the run ended, or `None` if it is still running or died
    /// without a clean stop.
    pub ended: Option<(Timestamp, EndReason)>,
}

/// A target known to the store and the span its samples cover.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TargetSummary {
    /// The target as the user named it.
    pub name: String,
    /// Send time of its oldest stored sample, if it has any.
    pub first: Option<Timestamp>,
    /// Send time of its newest stored sample, if it has any.
    pub last: Option<Timestamp>,
}

/// A network known to the store and the span its sessions cover.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NetworkSummary {
    /// The network's identity.
    pub id: NetworkId,
    /// What the user calls it.
    pub label: String,
    /// The default gateway's hardware address, if it was ever detected.
    pub gateway_mac: Option<String>,
    /// When the collector first ran on it, if any run on it is still stored.
    pub first: Option<Timestamp>,
    /// When the collector last ran on it.
    pub last: Option<Timestamp>,
}

/// A handle on the store for reading and pruning.
pub struct Store {
    conn: Connection,
    path: PathBuf,
}

impl Store {
    /// Opens the store at `path` for collecting, creating it if it does not
    /// exist yet and bringing an older schema up to date.
    pub fn create_or_open(path: &Path) -> Result<Self, StoreError> {
        let mut store = Self::configure(Connection::open(path)?, path)?;
        // WAL lets readers see a consistent snapshot while the collector
        // writes. The setting is stored in the file, so only this path, which
        // may write anyway, needs to make it.
        store.conn.pragma_update(None, "journal_mode", "WAL")?;
        store.migrate()?;
        Ok(store)
    }

    /// Opens a store that already exists, without writing to it: opening
    /// succeeds whatever another process is doing to the store at the time.
    pub fn open(path: &Path) -> Result<Self, StoreError> {
        if !path.exists() {
            return Err(StoreError::NotFound(path.to_owned()));
        }
        let flags = OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX;
        let store = Self::configure(Connection::open_with_flags(path, flags)?, path)?;
        let found = store.schema_version()?;
        match found.cmp(&SCHEMA_VERSION) {
            Ordering::Equal => Ok(store),
            Ordering::Greater => Err(StoreError::SchemaTooNew {
                found,
                supported: SCHEMA_VERSION,
            }),
            Ordering::Less => Err(StoreError::SchemaTooOld {
                found,
                current: SCHEMA_VERSION,
            }),
        }
    }

    /// Connection settings that live in the connection, not in the file.
    fn configure(conn: Connection, path: &Path) -> Result<Self, StoreError> {
        conn.busy_timeout(BUSY_TIMEOUT)?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        conn.pragma_update(None, "foreign_keys", true)?;
        Ok(Self {
            conn,
            path: path.to_owned(),
        })
    }

    /// Applies the migrations this store has not seen. Writes nothing when it
    /// is already current.
    fn migrate(&mut self) -> Result<(), StoreError> {
        if self.schema_version()? == SCHEMA_VERSION {
            return Ok(());
        }
        // Take the write lock first, then look again: another process may
        // have migrated in the meantime.
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let found: u32 = tx.pragma_query_value(None, "user_version", |row| row.get(0))?;
        if found > SCHEMA_VERSION {
            return Err(StoreError::SchemaTooNew {
                found,
                supported: SCHEMA_VERSION,
            });
        }
        for migration in MIGRATIONS.iter().skip(found as usize) {
            tx.execute_batch(migration)?;
        }
        tx.pragma_update(None, "user_version", SCHEMA_VERSION)?;
        tx.commit()?;
        Ok(())
    }

    /// Changes how long this handle waits on another process's write before
    /// reporting the store as busy.
    pub fn set_busy_timeout(&self, timeout: Duration) -> Result<(), StoreError> {
        Ok(self.conn.busy_timeout(timeout)?)
    }

    /// The schema version of this store.
    pub fn schema_version(&self) -> Result<u32, StoreError> {
        Ok(self
            .conn
            .pragma_query_value(None, "user_version", |row| row.get(0))?)
    }

    /// Where the store lives.
    pub fn path(&self) -> &Path {
        &self.path
    }

    fn lock_path(&self) -> PathBuf {
        let mut name = self.path.clone().into_os_string();
        name.push(".lock");
        PathBuf::from(name)
    }

    /// Claims this store for collecting. Fails if another collector holds it.
    ///
    /// The claim is released when the returned handle is dropped, or by the
    /// operating system if the process dies.
    pub fn collect(self) -> Result<Collecting, StoreError> {
        let lock = File::options()
            .create(true)
            .truncate(false)
            .write(true)
            .open(self.lock_path())?;
        match lock.try_lock() {
            Ok(()) => Ok(Collecting {
                store: self,
                _lock: lock,
            }),
            Err(TryLockError::WouldBlock) => Err(StoreError::AlreadyCollecting(self.path)),
            Err(TryLockError::Error(error)) => Err(error.into()),
        }
    }

    /// Whether a collector currently holds this store.
    pub fn is_collecting(&self) -> Result<bool, StoreError> {
        let lock = match File::open(self.lock_path()) {
            Ok(lock) => lock,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
            Err(error) => return Err(error.into()),
        };
        match lock.try_lock_shared() {
            Ok(()) => Ok(false),
            Err(TryLockError::WouldBlock) => Ok(true),
            Err(TryLockError::Error(error)) => Err(error.into()),
        }
    }

    /// Bytes the store occupies on disk, including its write-ahead log.
    pub fn size_bytes(&self) -> Result<u64, StoreError> {
        let mut total = fs::metadata(&self.path)?.len();
        let mut wal = self.path.clone().into_os_string();
        wal.push("-wal");
        match fs::metadata(wal) {
            Ok(metadata) => total += metadata.len(),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        Ok(total)
    }

    /// Every target that has ever been recorded, by name.
    pub fn targets(&self) -> Result<Vec<TargetSummary>, StoreError> {
        let mut statement = self.conn.prepare(
            "SELECT name,
                    (SELECT min(sent_us) FROM samples WHERE target_id = targets.id),
                    (SELECT max(sent_us) FROM samples WHERE target_id = targets.id)
             FROM targets ORDER BY name",
        )?;
        let rows = statement.query_map([], |row| {
            Ok(TargetSummary {
                name: row.get(0)?,
                first: row.get::<_, Option<i64>>(1)?.map(Timestamp::from_micros),
                last: row.get::<_, Option<i64>>(2)?.map(Timestamp::from_micros),
            })
        })?;
        Ok(rows.collect::<Result<_, _>>()?)
    }

    fn known_target(&self, name: &str) -> Result<TargetId, StoreError> {
        self.conn
            .query_row("SELECT id FROM targets WHERE name = ?1", [name], |row| {
                row.get(0)
            })
            .optional()?
            .map(TargetId)
            .ok_or_else(|| StoreError::UnknownTarget(name.to_owned()))
    }

    /// Every network the collector has run on, or been told it is on, by
    /// label.
    pub fn networks(&self) -> Result<Vec<NetworkSummary>, StoreError> {
        let mut statement = self.conn.prepare(
            "SELECT n.id, n.label, n.gateway_mac,
                    min(s.started_us), max(coalesce(s.ended_us, s.last_alive_us))
             FROM networks n LEFT JOIN sessions s ON s.network_id = n.id
             GROUP BY n.id ORDER BY n.label",
        )?;
        let rows = statement.query_map([], |row| {
            Ok(NetworkSummary {
                id: NetworkId(row.get(0)?),
                label: row.get(1)?,
                gateway_mac: row.get(2)?,
                first: row.get::<_, Option<i64>>(3)?.map(Timestamp::from_micros),
                last: row.get::<_, Option<i64>>(4)?.map(Timestamp::from_micros),
            })
        })?;
        Ok(rows.collect::<Result<_, _>>()?)
    }

    /// The network labeled `label`.
    pub fn network_by_label(&self, label: &str) -> Result<NetworkId, StoreError> {
        self.conn
            .query_row("SELECT id FROM networks WHERE label = ?1", [label], |row| {
                row.get(0)
            })
            .optional()?
            .map(NetworkId)
            .ok_or_else(|| StoreError::UnknownNetwork(label.to_owned()))
    }

    /// Gives the network labeled `label` the label `new` instead.
    ///
    /// Safe to run while a collector is recording.
    pub fn rename_network(&self, label: &str, new: &str) -> Result<(), StoreError> {
        let network = self.network_by_label(label)?;
        match self.conn.execute(
            "UPDATE networks SET label = ?2 WHERE id = ?1",
            params![network.0, new],
        ) {
            Ok(_) => Ok(()),
            Err(rusqlite::Error::SqliteFailure(error, _))
                if error.code == rusqlite::ErrorCode::ConstraintViolation =>
            {
                Err(StoreError::LabelTaken(new.to_owned()))
            }
            Err(error) => Err(error.into()),
        }
    }

    /// The samples sent to `target` within `range`, oldest first.
    pub fn samples(&self, target: &str, range: TimeRange) -> Result<Vec<Sample>, StoreError> {
        let target = self.known_target(target)?;
        let mut statement = self.conn.prepare_cached(
            "SELECT s.sent_us, s.session_id, s.seq, a.ip, s.rtt_us, s.loss, n.network_id
             FROM samples s LEFT JOIN addresses a ON a.id = s.address_id
               JOIN sessions n ON n.id = s.session_id
             WHERE s.target_id = ?1 AND s.sent_us >= ?2 AND s.sent_us < ?3
             ORDER BY s.sent_us, s.session_id, s.seq",
        )?;
        let rows = statement.query_map(
            params![target.0, range.since.as_micros(), range.until.as_micros()],
            read_sample,
        )?;
        Ok(rows.collect::<Result<_, _>>()?)
    }

    /// The collector runs that probed `target` and overlap `range`, oldest
    /// first.
    pub fn sessions(&self, target: &str, range: TimeRange) -> Result<Vec<Session>, StoreError> {
        let target = self.known_target(target)?;
        let mut statement = self.conn.prepare_cached(
            "SELECT s.id, s.started_us, s.last_alive_us, s.ended_us, s.end_reason, s.network_id
             FROM sessions s JOIN session_targets st ON st.session_id = s.id
             WHERE st.target_id = ?1
               AND s.started_us < ?3
               AND coalesce(s.ended_us, s.last_alive_us) >= ?2
             ORDER BY s.started_us, s.id",
        )?;
        let rows = statement.query_map(
            params![target.0, range.since.as_micros(), range.until.as_micros()],
            read_session,
        )?;
        Ok(rows.collect::<Result<_, _>>()?)
    }

    /// How many samples [`prune`](Self::prune) would remove for `before`.
    pub fn count_before(&self, before: Timestamp) -> Result<u64, StoreError> {
        let count: i64 = self.conn.query_row(
            "SELECT count(*) FROM samples WHERE sent_us < ?1",
            [before.as_micros()],
            |row| row.get(0),
        )?;
        Ok(u64::try_from(count).unwrap_or(0))
    }

    /// Removes every sample sent before `before`, and the sessions that lie
    /// entirely before it. Returns the number of samples removed.
    ///
    /// Safe to run while a collector is recording.
    pub fn prune(&self, before: Timestamp) -> Result<u64, StoreError> {
        let targets: Vec<i64> = self
            .conn
            .prepare("SELECT id FROM targets")?
            .query_map([], |row| row.get(0))?
            .collect::<Result<_, _>>()?;

        let mut removed = 0;
        for target in targets {
            loop {
                // Each chunk commits on its own, releasing the write lock.
                let chunk = self.conn.execute(
                    "DELETE FROM samples
                     WHERE (target_id, sent_us, session_id, seq) IN (
                       SELECT target_id, sent_us, session_id, seq FROM samples
                       WHERE target_id = ?1 AND sent_us < ?2
                       LIMIT ?3)",
                    params![target, before.as_micros(), PRUNE_CHUNK],
                )?;
                if chunk == 0 {
                    break;
                }
                removed += chunk as u64;
            }
        }

        self.conn.execute_batch("BEGIN IMMEDIATE")?;
        let swept = self.sweep_sessions(before);
        self.conn
            .execute_batch(if swept.is_ok() { "COMMIT" } else { "ROLLBACK" })?;
        swept?;
        Ok(removed)
    }

    /// Removes sessions that ended (or were last alive) before `before` and
    /// have no samples left.
    fn sweep_sessions(&self, before: Timestamp) -> Result<(), StoreError> {
        const DEAD: &str = "SELECT id FROM sessions
             WHERE coalesce(ended_us, last_alive_us) < ?1
               AND NOT EXISTS (SELECT 1 FROM samples WHERE session_id = sessions.id)";
        self.conn.execute(
            &format!("DELETE FROM session_targets WHERE session_id IN ({DEAD})"),
            [before.as_micros()],
        )?;
        self.conn.execute(
            &format!("DELETE FROM sessions WHERE id IN ({DEAD})"),
            [before.as_micros()],
        )?;
        Ok(())
    }
}

fn corrupt(column: usize, kind: Type, what: &str) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(column, kind, what.into())
}

fn read_sample(row: &Row<'_>) -> rusqlite::Result<Sample> {
    let address = row
        .get::<_, Option<String>>(3)?
        .map(|ip| {
            ip.parse()
                .map_err(|_| corrupt(3, Type::Text, "not an IP address"))
        })
        .transpose()?;
    let outcome = match (row.get::<_, Option<i64>>(4)?, row.get::<_, Option<i64>>(5)?) {
        (Some(rtt), None) => Outcome::Reply {
            rtt: Duration::from_micros(u64::try_from(rtt).unwrap_or(0)),
        },
        (None, Some(code)) => Outcome::Lost(
            Loss::from_code(code).ok_or_else(|| corrupt(5, Type::Integer, "unknown loss code"))?,
        ),
        _ => return Err(corrupt(4, Type::Integer, "neither answered nor lost")),
    };
    Ok(Sample {
        sent: Timestamp::from_micros(row.get(0)?),
        session: SessionId(row.get(1)?),
        network: row.get::<_, Option<i64>>(6)?.map(NetworkId),
        seq: row.get(2)?,
        address,
        outcome,
    })
}

fn read_session(row: &Row<'_>) -> rusqlite::Result<Session> {
    let ended = match (
        row.get::<_, Option<i64>>(3)?,
        row.get::<_, Option<String>>(4)?,
    ) {
        (Some(at), Some(reason)) => Some((
            Timestamp::from_micros(at),
            EndReason::parse(&reason)
                .ok_or_else(|| corrupt(4, Type::Text, "unknown end reason"))?,
        )),
        _ => None,
    };
    Ok(Session {
        id: SessionId(row.get(0)?),
        network: row.get::<_, Option<i64>>(5)?.map(NetworkId),
        started: Timestamp::from_micros(row.get(1)?),
        last_alive: Timestamp::from_micros(row.get(2)?),
        ended,
    })
}

/// A label no network has: `ssid`, `ssid-2`, `ssid-3`, … for a Wi-Fi
/// network, otherwise the first of `network-1`, `network-2`, … counting from
/// how many networks there are.
fn unused_label(conn: &Connection, ssid: Option<&str>) -> rusqlite::Result<String> {
    let taken = |label: &str| {
        conn.query_row("SELECT 1 FROM networks WHERE label = ?1", [label], |_| {
            Ok(())
        })
        .optional()
        .map(|found| found.is_some())
    };
    if let Some(ssid) = ssid {
        if !taken(ssid)? {
            return Ok(ssid.to_owned());
        }
        for n in 2.. {
            let label = format!("{ssid}-{n}");
            if !taken(&label)? {
                return Ok(label);
            }
        }
    }
    let mut n: i64 = conn.query_row("SELECT count(*) FROM networks", [], |row| row.get(0))?;
    loop {
        n += 1;
        let label = format!("network-{n}");
        if !taken(&label)? {
            return Ok(label);
        }
    }
}

/// The store, claimed by the one collector allowed to record into it.
pub struct Collecting {
    store: Store,
    /// Held for its lock; released on drop.
    _lock: File,
}

impl Collecting {
    /// The underlying store, for reading.
    pub fn store(&self) -> &Store {
        &self.store
    }

    /// The identity of the target called `name`, registering it if it is new.
    pub fn target(&mut self, name: &str) -> Result<TargetId, StoreError> {
        let conn = &self.store.conn;
        conn.execute("INSERT OR IGNORE INTO targets (name) VALUES (?1)", [name])?;
        Ok(TargetId(conn.query_row(
            "SELECT id FROM targets WHERE name = ?1",
            [name],
            |row| row.get(0),
        )?))
    }

    /// The identity of `ip` as an address of `target`, registering it if it is
    /// new.
    pub fn address(&mut self, target: TargetId, ip: IpAddr) -> Result<AddressId, StoreError> {
        let conn = &self.store.conn;
        let ip = ip.to_string();
        conn.execute(
            "INSERT OR IGNORE INTO addresses (target_id, ip) VALUES (?1, ?2)",
            params![target.0, ip],
        )?;
        Ok(AddressId(conn.query_row(
            "SELECT id FROM addresses WHERE target_id = ?1 AND ip = ?2",
            params![target.0, ip],
            |row| row.get(0),
        )?))
    }

    /// The identity of `network`, registering it if it is new.
    ///
    /// A newly detected network is labeled with its Wi-Fi name if it has one
    /// (`Home`, or `Home-2` if that is taken), and otherwise `network-1`,
    /// `network-2`, and so on, for the user to rename. A named one keeps its
    /// name.
    pub fn network(&mut self, network: &SessionNetwork) -> Result<NetworkId, StoreError> {
        let tx = self.store.conn.transaction()?;
        let id = match network {
            SessionNetwork::Named(label) => {
                tx.execute(
                    "INSERT OR IGNORE INTO networks (label) VALUES (?1)",
                    [label],
                )?;
                tx.query_row("SELECT id FROM networks WHERE label = ?1", [label], |row| {
                    row.get(0)
                })?
            }
            SessionNetwork::Detected(observed) => {
                let known = tx
                    .query_row(
                        "SELECT id FROM networks WHERE gateway_mac = ?1",
                        [&observed.gateway_mac],
                        |row| row.get(0),
                    )
                    .optional()?;
                if let Some(id) = known {
                    id
                } else {
                    tx.execute(
                        "INSERT INTO networks (gateway_mac, label) VALUES (?1, ?2)",
                        params![
                            observed.gateway_mac,
                            unused_label(&tx, observed.ssid.as_deref())?
                        ],
                    )?;
                    tx.last_insert_rowid()
                }
            }
        };
        tx.commit()?;
        Ok(NetworkId(id))
    }

    /// Begins a collector run at `now` on `network` that will probe `targets`.
    pub fn start_session(
        &mut self,
        now: Timestamp,
        settings: SessionSettings,
        network: Option<NetworkId>,
        targets: &[TargetId],
    ) -> Result<SessionId, StoreError> {
        let tx = self.store.conn.transaction()?;
        tx.execute(
            "INSERT INTO sessions
               (started_us, last_alive_us, interval_us, timeout_us, pingr_version, network_id)
             VALUES (?1, ?1, ?2, ?3, ?4, ?5)",
            params![
                now.as_micros(),
                micros(settings.interval),
                micros(settings.timeout),
                env!("CARGO_PKG_VERSION"),
                network.map(|network| network.0),
            ],
        )?;
        let session = SessionId(tx.last_insert_rowid());
        for target in targets {
            tx.execute(
                "INSERT INTO session_targets (session_id, target_id) VALUES (?1, ?2)",
                params![session.0, target.0],
            )?;
        }
        tx.commit()?;
        Ok(session)
    }

    /// Stores `samples` and notes that the collector, now running `session`,
    /// was alive at `now`: all or nothing.
    ///
    /// Samples may belong to earlier sessions, when their answers arrive after
    /// a new one has begun.
    pub fn record(
        &mut self,
        session: SessionId,
        now: Timestamp,
        samples: &[NewSample],
    ) -> Result<(), StoreError> {
        let tx = self.store.conn.transaction()?;
        {
            let mut insert = tx.prepare_cached(
                "INSERT INTO samples
                   (target_id, sent_us, session_id, seq, address_id, rtt_us, loss)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            )?;
            for sample in samples {
                let (rtt, loss) = match sample.outcome {
                    Outcome::Reply { rtt } => (Some(micros(rtt)), None),
                    Outcome::Lost(loss) => (None, Some(loss.code())),
                };
                insert.execute(params![
                    sample.target.0,
                    sample.sent.as_micros(),
                    sample.session.0,
                    sample.seq,
                    sample.address.map(|address| address.0),
                    rtt,
                    loss,
                ])?;
            }
        }
        tx.execute(
            "UPDATE sessions SET last_alive_us = ?2 WHERE id = ?1",
            params![session.0, now.as_micros()],
        )?;
        tx.commit()?;
        Ok(())
    }

    /// Marks `session` as ended at `at` for `reason`.
    pub fn end_session(
        &mut self,
        session: SessionId,
        at: Timestamp,
        reason: EndReason,
    ) -> Result<(), StoreError> {
        self.store.conn.execute(
            "UPDATE sessions SET last_alive_us = ?2, ended_us = ?2, end_reason = ?3 WHERE id = ?1",
            params![session.0, at.as_micros(), reason.as_str()],
        )?;
        Ok(())
    }
}
