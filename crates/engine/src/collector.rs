//! The collector: probes every target on a fixed cadence and records what
//! comes back.
//!
//! One loop owns the schedule. Each probe runs in its own task, so a target
//! that never answers cannot delay another. Results are handed to a writer
//! thread, which is the only thing that touches the store; a slow or busy
//! store therefore never holds up probing.

mod writer;

use std::future::Future;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{broadcast, mpsc, oneshot};
use tokio::time::{Instant, MissedTickBehavior};

use crate::clock::WallClock;
use crate::network::{NetworkDetector, Observed, SessionNetwork};
use crate::prober::{ProbeError, Prober};
use crate::resolve::Resolver;
use crate::store::{Collecting, EndReason, Loss, Outcome, SessionSettings, StoreError};
use crate::time::{Timestamp, micros};
use writer::{Command, Record, Writer};

/// What to probe and how often.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    /// Host names or IP literals, as the user gave them.
    pub targets: Vec<String>,
    /// Time between probes to one target.
    pub interval: Duration,
    /// How long a probe may go unanswered before it counts as lost. Must
    /// match the prober's own timeout.
    pub timeout: Duration,
    /// How often a host name is looked up again.
    pub resolve_interval: Duration,
    /// How soon a failed lookup is retried.
    pub resolve_retry: Duration,
    /// How far the schedule may slip, or the wall clock may disagree with
    /// elapsed time, before the run is cut and a new session begun.
    pub gap_tolerance: Duration,
    /// The network to record every session as, with detection turned off.
    /// `None` detects it.
    pub network: Option<String>,
    /// How often the network is checked for a change.
    pub network_interval: Duration,
}

impl Config {
    /// Probes `targets` once a second with the default settings.
    pub fn new(targets: Vec<String>) -> Self {
        Self {
            targets,
            interval: Duration::from_secs(1),
            timeout: Duration::from_secs(2),
            resolve_interval: Duration::from_secs(300),
            resolve_retry: Duration::from_secs(10),
            gap_tolerance: Duration::from_secs(5),
            network: None,
            network_interval: Duration::from_secs(10),
        }
    }
}

/// Something that happened while collecting, for whoever is displaying it.
///
/// Targets are identified by their position in [`Config::targets`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    /// A session began.
    SessionStarted {
        /// When.
        at: Timestamp,
    },
    /// Sessions from now on are on this network; `None` means it cannot be
    /// told.
    Network {
        /// The network.
        network: Option<SessionNetwork>,
    },
    /// The session was cut short; another follows unless the collector is
    /// stopping.
    SessionEnded {
        /// The last moment the session covers.
        at: Timestamp,
        /// Why it ended.
        reason: EndReason,
    },
    /// A target is now being probed at this address.
    Resolved {
        /// Which target.
        target: usize,
        /// Its address.
        ip: IpAddr,
    },
    /// A target's name could not be looked up.
    ResolveFailed {
        /// Which target.
        target: usize,
        /// What the resolver said.
        error: String,
    },
    /// A probe finished.
    Probe {
        /// Which target.
        target: usize,
        /// When it was sent.
        sent: Timestamp,
        /// Its position in the session, per target.
        seq: u32,
        /// The address probed, if the name resolved.
        ip: Option<IpAddr>,
        /// What came back.
        outcome: Outcome,
    },
    /// The store rejected a write. Samples are held in memory and retried.
    StoreTrouble {
        /// What the store said.
        error: String,
    },
}

/// Why collecting could not start or had to stop.
#[derive(Debug, thiserror::Error)]
pub enum CollectError {
    /// There is nothing to probe.
    #[error("no targets to probe")]
    NoTargets,
    /// This system will not let pingr send probes.
    #[error(transparent)]
    Probe(#[from] ProbeError),
    /// The store stayed unwritable for too long, or failed outright.
    #[error("could not record samples")]
    Store(#[from] StoreError),
}

/// What the collector needs from the outside world.
pub struct Deps<P, R, W, N> {
    /// Sends the probes.
    pub prober: Arc<P>,
    /// Looks up host names.
    pub resolver: Arc<R>,
    /// Tells the time.
    pub wall: Arc<W>,
    /// Tells which network the machine is on.
    pub network: Arc<N>,
}

/// A message from a probe or lookup task back to the loop.
enum Message {
    Probed {
        generation: u64,
        target: usize,
        sent: Timestamp,
        seq: u32,
        ip: IpAddr,
        result: Result<Outcome, ProbeError>,
    },
    Resolved {
        target: usize,
        result: Result<Vec<IpAddr>, String>,
    },
    Network(Option<Observed>),
}

/// Where a target stands.
struct TargetState {
    name: String,
    /// The address being probed, once known.
    ip: Option<IpAddr>,
    /// IP literals are never looked up.
    literal: bool,
    /// When to look the name up next.
    resolve_at: Instant,
    resolving: bool,
    next_seq: u32,
}

/// Collects until `stop` completes, then finishes the session cleanly.
///
/// Every probe result and state change is also sent to `events`.
pub async fn run<P, R, W, N>(
    config: Config,
    deps: Deps<P, R, W, N>,
    mut store: Collecting,
    events: broadcast::Sender<Event>,
    stop: impl Future<Output = ()>,
) -> Result<(), CollectError>
where
    P: Prober + 'static,
    R: Resolver + 'static,
    W: WallClock + 'static,
    N: NetworkDetector + 'static,
{
    if config.targets.is_empty() {
        return Err(CollectError::NoTargets);
    }
    deps.prober.preflight()?;

    let settings = SessionSettings {
        interval: config.interval,
        timeout: config.timeout,
    };
    let ids = config
        .targets
        .iter()
        .map(|name| store.target(name))
        .collect::<Result<Vec<_>, _>>()?;
    let (commands, notices, mut writer_done) = Writer::spawn(store, settings, ids);

    let (outbox, inbox) = mpsc::unbounded_channel();
    let mut collector = Collector {
        targets: targets(&config),
        network: config.network.clone().map(SessionNetwork::Named),
        detect_at: Instant::now() + config.network_interval,
        detecting: false,
        config,
        deps,
        events,
        commands,
        outbox,
        generation: 0,
        pending: Vec::new(),
        in_flight: 0,
        last_tick: None,
        writer_finished: false,
    };
    let outcome = collector
        .collect(inbox, notices, &mut writer_done, stop)
        .await;
    if collector.writer_finished {
        return outcome;
    }
    // Closing the command channel lets the writer finish what it holds.
    drop(collector);
    let written = writer_done.await.unwrap_or(Ok(()));
    outcome.and(written.map_err(CollectError::from))
}

fn targets(config: &Config) -> Vec<TargetState> {
    let now = Instant::now();
    config
        .targets
        .iter()
        .map(|name| {
            let literal = name.parse::<IpAddr>().ok();
            TargetState {
                name: name.clone(),
                ip: literal,
                literal: literal.is_some(),
                resolve_at: now,
                resolving: false,
                next_seq: 0,
            }
        })
        .collect()
}

struct Collector<P, R, W, N> {
    config: Config,
    deps: Deps<P, R, W, N>,
    events: broadcast::Sender<Event>,
    commands: std::sync::mpsc::Sender<Command>,
    outbox: mpsc::UnboundedSender<Message>,
    targets: Vec<TargetState>,
    /// The network sessions are recorded as on.
    network: Option<SessionNetwork>,
    /// When to check the network next.
    detect_at: Instant,
    detecting: bool,
    /// Counts sessions within this run; the writer maps it to a session id.
    generation: u64,
    /// Results waiting for the next write.
    pending: Vec<Record>,
    /// Probes sent and not yet finished.
    in_flight: usize,
    /// Monotonic and wall time of the previous tick.
    last_tick: Option<(Instant, Timestamp)>,
    /// The writer has already reported its result; do not wait for it again.
    writer_finished: bool,
}

impl<P, R, W, N> Collector<P, R, W, N>
where
    P: Prober + 'static,
    R: Resolver + 'static,
    W: WallClock + 'static,
    N: NetworkDetector + 'static,
{
    async fn collect(
        &mut self,
        mut inbox: mpsc::UnboundedReceiver<Message>,
        mut notices: mpsc::UnboundedReceiver<String>,
        writer_done: &mut oneshot::Receiver<Result<(), StoreError>>,
        stop: impl Future<Output = ()>,
    ) -> Result<(), CollectError> {
        self.resolve_all(&mut inbox).await;
        if self.detects() {
            self.network = self
                .deps
                .network
                .detect()
                .await
                .map(SessionNetwork::Detected);
        }
        self.emit(Event::Network {
            network: self.network.clone(),
        });
        self.start_session(self.deps.wall.now());

        let mut ticker = tokio::time::interval(self.config.interval);
        // After a stall, carry on from now rather than replaying missed ticks.
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        tokio::pin!(stop);

        loop {
            tokio::select! {
                biased;
                () = &mut stop => break,
                written = &mut *writer_done => {
                    // The writer only finishes early when it has given up.
                    self.writer_finished = true;
                    return Err(match written {
                        Ok(Err(error)) => error,
                        Ok(Ok(())) | Err(_) => StoreError::Io(std::io::Error::other(
                            "the store writer stopped unexpectedly",
                        )),
                    }
                    .into());
                }
                Some(error) = notices.recv() => self.emit(Event::StoreTrouble { error }),
                Some(message) = inbox.recv() => self.handle(message)?,
                _ = ticker.tick() => self.tick(),
            }
        }

        // Let probes already on the wire finish; each ends by its timeout.
        while self.in_flight > 0 {
            match inbox.recv().await {
                // Too late to start another session.
                Some(Message::Network(_)) => {}
                Some(message) => self.handle(message)?,
                None => break,
            }
        }
        let now = self.deps.wall.now();
        self.flush(now);
        self.end_session(now, EndReason::Stopped);
        Ok(())
    }

    fn emit(&self, event: Event) {
        // Nobody listening is fine.
        let _ = self.events.send(event);
    }

    fn command(&self, command: Command) {
        // If the writer is gone, the loop learns of it through `writer_done`.
        let _ = self.commands.send(command);
    }

    fn start_session(&mut self, at: Timestamp) {
        self.command(Command::Start {
            generation: self.generation,
            at,
            network: self.network.clone(),
        });
        self.emit(Event::SessionStarted { at });
    }

    fn end_session(&mut self, at: Timestamp, reason: EndReason) {
        self.command(Command::End {
            generation: self.generation,
            at,
            reason,
        });
        self.emit(Event::SessionEnded { at, reason });
    }

    /// Hands everything finished so far to the writer and marks the session
    /// alive at `at`.
    fn flush(&mut self, at: Timestamp) {
        let records = std::mem::take(&mut self.pending);
        self.command(Command::Record {
            generation: self.generation,
            at,
            records,
        });
    }

    /// Looks up every host name once before the first probe, so a name that
    /// resolves is never recorded as unresolved at startup.
    async fn resolve_all(&mut self, inbox: &mut mpsc::UnboundedReceiver<Message>) {
        let mut waiting = 0;
        for target in 0..self.targets.len() {
            if !self.targets[target].literal {
                self.resolve(target);
                waiting += 1;
            }
        }
        while waiting > 0 {
            match inbox.recv().await {
                Some(Message::Resolved { target, result }) => {
                    self.resolved(target, result);
                    waiting -= 1;
                }
                Some(Message::Probed { .. } | Message::Network(_)) | None => break,
            }
        }
    }

    fn resolve(&mut self, target: usize) {
        let state = &mut self.targets[target];
        state.resolving = true;
        let name = state.name.clone();
        let resolver = Arc::clone(&self.deps.resolver);
        let outbox = self.outbox.clone();
        tokio::spawn(async move {
            let result = resolver
                .resolve(&name)
                .await
                .map_err(|error| error.to_string());
            let _ = outbox.send(Message::Resolved { target, result });
        });
    }

    fn resolved(&mut self, target: usize, result: Result<Vec<IpAddr>, String>) {
        let now = Instant::now();
        let state = &mut self.targets[target];
        state.resolving = false;
        let addresses = result.and_then(|addresses| {
            if addresses.is_empty() {
                Err("the name has no addresses".to_owned())
            } else {
                Ok(addresses)
            }
        });
        match addresses {
            Ok(addresses) => {
                state.resolve_at = now + self.config.resolve_interval;
                // Stay on the current address while it is still listed.
                let ip = state
                    .ip
                    .filter(|current| addresses.contains(current))
                    .unwrap_or(addresses[0]);
                if state.ip != Some(ip) {
                    let previous = state.ip.replace(ip);
                    self.emit(Event::Resolved { target, ip });
                    // Over months a name can move through many addresses;
                    // hold on to none that no target still uses.
                    if let Some(previous) = previous
                        && !self.targets.iter().any(|other| other.ip == Some(previous))
                    {
                        self.deps.prober.forget(previous);
                    }
                }
            }
            Err(error) => {
                // Keep probing the last known address, if there is one.
                state.resolve_at = now + self.config.resolve_retry;
                self.emit(Event::ResolveFailed { target, error });
            }
        }
    }

    /// Whether the network is detected, rather than named by the user.
    fn detects(&self) -> bool {
        self.config.network.is_none()
    }

    fn detect(&mut self) {
        self.detecting = true;
        let detector = Arc::clone(&self.deps.network);
        let outbox = self.outbox.clone();
        tokio::spawn(async move {
            let _ = outbox.send(Message::Network(detector.detect().await));
        });
    }

    /// Cuts the session if the machine is now on another network. A network
    /// that cannot be told is no change: losing the default route is
    /// something to record as loss on the network it happened to.
    fn detected(&mut self, observed: Option<Observed>) {
        self.detecting = false;
        self.detect_at = Instant::now() + self.config.network_interval;
        let Some(observed) = observed.map(SessionNetwork::Detected) else {
            return;
        };
        if self.network.as_ref() == Some(&observed) {
            return;
        }
        let now = self.deps.wall.now();
        self.flush(now);
        self.end_session(now, EndReason::NetworkChanged);
        self.network = Some(observed);
        self.emit(Event::Network {
            network: self.network.clone(),
        });
        self.next_session(now);
    }

    /// Begins a new session at `at`, the previous one having ended.
    fn next_session(&mut self, at: Timestamp) {
        self.generation += 1;
        for target in &mut self.targets {
            target.next_seq = 0;
        }
        self.start_session(at);
    }

    fn handle(&mut self, message: Message) -> Result<(), CollectError> {
        match message {
            Message::Network(observed) => self.detected(observed),
            Message::Resolved { target, result } => self.resolved(target, result),
            Message::Probed {
                generation,
                target,
                sent,
                seq,
                ip,
                result,
            } => {
                self.in_flight -= 1;
                let outcome = match result {
                    Ok(outcome) => outcome,
                    Err(error @ ProbeError::NotPermitted(_)) => return Err(error.into()),
                    Err(ProbeError::Io { .. }) => Outcome::Lost(Loss::Other),
                };
                self.finished(generation, target, sent, seq, Some(ip), outcome);
            }
        }
        Ok(())
    }

    fn finished(
        &mut self,
        generation: u64,
        target: usize,
        sent: Timestamp,
        seq: u32,
        ip: Option<IpAddr>,
        outcome: Outcome,
    ) {
        self.pending.push(Record {
            generation,
            target,
            sent,
            seq,
            ip,
            outcome,
        });
        self.emit(Event::Probe {
            target,
            sent,
            seq,
            ip,
            outcome,
        });
    }

    /// Why the time since the previous tick means the session cannot simply
    /// continue, if it does.
    fn interruption(&self, mono: Instant, wall: Timestamp) -> Option<(Timestamp, EndReason)> {
        let (last_mono, last_wall) = self.last_tick?;
        let elapsed = mono.duration_since(last_mono);
        let tolerance = micros(self.config.gap_tolerance);
        let reason = if elapsed > self.config.interval + self.config.gap_tolerance {
            EndReason::Stalled
        } else if (wall.as_micros() - last_wall.as_micros() - micros(elapsed)).abs() > tolerance {
            EndReason::ClockJump
        } else {
            return None;
        };
        Some((last_wall, reason))
    }

    fn tick(&mut self) {
        let mono = Instant::now();
        let wall = self.deps.wall.now();

        if let Some((last_alive, reason)) = self.interruption(mono, wall) {
            self.flush(last_alive);
            self.end_session(last_alive, reason);
            self.next_session(wall);
        }
        self.last_tick = Some((mono, wall));
        self.flush(wall);

        if self.detects() && !self.detecting && mono >= self.detect_at {
            self.detect();
        }
        for target in 0..self.targets.len() {
            let state = &self.targets[target];
            if !state.literal && !state.resolving && mono >= state.resolve_at {
                self.resolve(target);
            }
            self.probe(target, wall);
        }
    }

    fn probe(&mut self, target: usize, sent: Timestamp) {
        let state = &mut self.targets[target];
        let seq = state.next_seq;
        state.next_seq += 1;
        let Some(ip) = state.ip else {
            let unresolved = Outcome::Lost(Loss::Unresolved);
            self.finished(self.generation, target, sent, seq, None, unresolved);
            return;
        };

        self.in_flight += 1;
        let generation = self.generation;
        let prober = Arc::clone(&self.deps.prober);
        let outbox = self.outbox.clone();
        tokio::spawn(async move {
            let result = prober.probe(ip).await.map(Outcome::from);
            let _ = outbox.send(Message::Probed {
                generation,
                target,
                sent,
                seq,
                ip,
                result,
            });
        });
    }
}
