//! The one thread that writes to the store.
//!
//! Commands are applied strictly in order. One that fails stays at the head of
//! the queue and is tried again when the next arrives, so a busy store (a
//! prune in progress, say) costs nothing but delay. Only when the backlog
//! outgrows its bound does the writer give up.

use std::collections::{HashMap, VecDeque};
use std::net::IpAddr;
use std::sync::mpsc::{self, RecvTimeoutError};
use std::thread;
use std::time::Duration;

use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio::sync::oneshot;

use crate::store::{
    AddressId, Collecting, EndReason, NewSample, Outcome, SessionId, SessionSettings, StoreError,
    TargetId,
};
use crate::time::Timestamp;

/// Samples held in memory, waiting on an unwritable store, before the
/// collector stops rather than grow without bound.
const MAX_BACKLOG: usize = 100_000;

/// How often a stuck queue is retried when no new command arrives.
const RETRY_EVERY: Duration = Duration::from_secs(1);

/// How many times the queue is retried after the collector has stopped.
const FINAL_ATTEMPTS: u32 = 5;

/// One finished probe, before it has store identities.
#[derive(Debug, Clone, Copy)]
pub(super) struct Record {
    /// The session, counted within this run.
    pub generation: u64,
    /// The target's position in the configuration.
    pub target: usize,
    pub sent: Timestamp,
    pub seq: u32,
    pub ip: Option<IpAddr>,
    pub outcome: Outcome,
}

pub(super) enum Command {
    /// Begin the session numbered `generation`.
    Start { generation: u64, at: Timestamp },
    /// Store `records` and mark session `generation` alive at `at`.
    Record {
        generation: u64,
        at: Timestamp,
        records: Vec<Record>,
    },
    /// End the session numbered `generation`.
    End {
        generation: u64,
        at: Timestamp,
        reason: EndReason,
    },
}

pub(super) struct Writer {
    store: Collecting,
    settings: SessionSettings,
    targets: Vec<TargetId>,
    sessions: HashMap<u64, SessionId>,
    addresses: HashMap<(usize, IpAddr), AddressId>,
    queue: VecDeque<Command>,
    notices: UnboundedSender<String>,
}

type Done = oneshot::Receiver<Result<(), StoreError>>;

impl Writer {
    /// Starts the writer thread. Returns where to send commands, where its
    /// complaints about the store arrive, and where its final result lands
    /// once the command channel is closed (or it gives up).
    pub(super) fn spawn(
        store: Collecting,
        settings: SessionSettings,
        targets: Vec<TargetId>,
    ) -> (mpsc::Sender<Command>, UnboundedReceiver<String>, Done) {
        let (commands, inbox) = mpsc::channel();
        let (notices, complaints) = unbounded_channel();
        let (done, finished) = oneshot::channel();
        let writer = Self {
            store,
            settings,
            targets,
            sessions: HashMap::new(),
            addresses: HashMap::new(),
            queue: VecDeque::new(),
            notices,
        };
        thread::spawn(move || {
            let _ = done.send(writer.run(&inbox));
        });
        (commands, complaints, finished)
    }

    fn run(mut self, inbox: &mpsc::Receiver<Command>) -> Result<(), StoreError> {
        loop {
            let wait = if self.queue.is_empty() {
                inbox.recv().map_err(|_| RecvTimeoutError::Disconnected)
            } else {
                inbox.recv_timeout(RETRY_EVERY)
            };
            match wait {
                Ok(command) => self.queue.push_back(command),
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => break,
            }
            self.drain()?;
        }

        // The collector has stopped; whatever is still queued gets a few
        // more chances before it is reported as lost.
        let mut last = Ok(());
        for attempt in 0..FINAL_ATTEMPTS {
            if attempt > 0 {
                thread::sleep(RETRY_EVERY);
            }
            last = self.apply_queue();
            if last.is_ok() {
                break;
            }
        }
        last
    }

    /// Applies queued commands until one fails. A failure is reported and
    /// left for later unless the backlog has outgrown its bound.
    fn drain(&mut self) -> Result<(), StoreError> {
        let Err(error) = self.apply_queue() else {
            return Ok(());
        };
        if self.backlog() > MAX_BACKLOG {
            return Err(error);
        }
        let _ = self.notices.send(error.to_string());
        Ok(())
    }

    fn apply_queue(&mut self) -> Result<(), StoreError> {
        while let Some(command) = self.queue.pop_front() {
            if let Err(error) = self.apply(&command) {
                self.queue.push_front(command);
                return Err(error);
            }
        }
        Ok(())
    }

    /// Samples queued and not yet stored.
    fn backlog(&self) -> usize {
        self.queue
            .iter()
            .map(|command| match command {
                Command::Record { records, .. } => records.len(),
                Command::Start { .. } | Command::End { .. } => 0,
            })
            .sum()
    }

    /// Applies one command. Must be safe to repeat after a failure.
    fn apply(&mut self, command: &Command) -> Result<(), StoreError> {
        match command {
            Command::Start { generation, at } => {
                if !self.sessions.contains_key(generation) {
                    let session = self
                        .store
                        .start_session(*at, self.settings, &self.targets)?;
                    self.sessions.insert(*generation, session);
                }
            }
            Command::Record {
                generation,
                at,
                records,
            } => {
                let samples = records
                    .iter()
                    .map(|record| self.sample(record))
                    .collect::<Result<Vec<_>, _>>()?;
                self.store
                    .record(self.sessions[generation], *at, &samples)?;
            }
            Command::End {
                generation,
                at,
                reason,
            } => {
                self.store
                    .end_session(self.sessions[generation], *at, *reason)?;
            }
        }
        Ok(())
    }

    fn sample(&mut self, record: &Record) -> Result<NewSample, StoreError> {
        let target = self.targets[record.target];
        let address = match record.ip {
            None => None,
            Some(ip) => Some(self.address(record.target, ip)?),
        };
        Ok(NewSample {
            session: self.sessions[&record.generation],
            target,
            sent: record.sent,
            seq: record.seq,
            address,
            outcome: record.outcome,
        })
    }

    fn address(&mut self, target: usize, ip: IpAddr) -> Result<AddressId, StoreError> {
        if let Some(&address) = self.addresses.get(&(target, ip)) {
            return Ok(address);
        }
        let address = self.store.address(self.targets[target], ip)?;
        self.addresses.insert((target, ip), address);
        Ok(address)
    }
}
