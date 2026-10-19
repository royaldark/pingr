//! The collector, driven by a simulated prober on tokio's paused clock: no
//! network, no real waiting, and the same result every run.

use std::collections::HashMap;
use std::io;
use std::net::IpAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Mutex, PoisonError};
use std::time::Duration;

use pingr_engine::clock::WallClock;
use pingr_engine::collector::{CollectError, Config, Deps, Event, run};
use pingr_engine::network::{NetworkDetector, Observed, SessionNetwork};
use pingr_engine::prober::ProbeError;
use pingr_engine::prober::sim::{Sim, SimProber};
use pingr_engine::resolve::Resolver;
use pingr_engine::store::{EndReason, Loss, Outcome, Sample, Store};
use pingr_engine::time::{TimeRange, Timestamp};
use tempfile::TempDir;
use tokio::sync::broadcast;
use tokio::time::Instant;

const SECOND: Duration = Duration::from_secs(1);
const EVERYTHING: TimeRange = TimeRange {
    since: Timestamp::from_micros(i64::MIN),
    until: Timestamp::from_micros(i64::MAX),
};
/// Wall-clock time when each test begins.
const START: Timestamp = Timestamp::from_micros(1_700_000_000_000_000);

/// A wall clock that follows tokio's clock, and can be made to jump.
struct TestClock {
    began: Instant,
    jumped_micros: AtomicI64,
}

impl TestClock {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            began: Instant::now(),
            jumped_micros: AtomicI64::new(0),
        })
    }

    fn jump(&self, by: Duration) {
        let micros = i64::try_from(by.as_micros()).expect("small jump");
        self.jumped_micros.fetch_add(micros, Ordering::SeqCst);
    }
}

impl WallClock for TestClock {
    fn now(&self) -> Timestamp {
        let jumped = self.jumped_micros.load(Ordering::SeqCst);
        Timestamp::from_micros(START.plus(self.began.elapsed()).as_micros() + jumped)
    }
}

/// A resolver that knows a fixed set of names.
struct Names(HashMap<&'static str, IpAddr>);

impl Resolver for Names {
    fn resolve(&self, name: &str) -> impl Future<Output = io::Result<Vec<IpAddr>>> + Send {
        std::future::ready(
            self.0
                .get(name)
                .map(|&ip| vec![ip])
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "no such host")),
        )
    }
}

/// A network the test moves the machine on and off: a gateway, and the
/// Wi-Fi name the system reports alongside it.
struct Gateway(Mutex<(Option<&'static str>, Option<&'static str>)>);

impl Gateway {
    fn new(mac: Option<&'static str>) -> Arc<Self> {
        Arc::new(Self(Mutex::new((mac, None))))
    }

    fn set(&self, mac: Option<&'static str>) {
        self.0.lock().unwrap_or_else(PoisonError::into_inner).0 = mac;
    }

    fn set_ssid(&self, ssid: Option<&'static str>) {
        self.0.lock().unwrap_or_else(PoisonError::into_inner).1 = ssid;
    }
}

impl NetworkDetector for Gateway {
    fn detect(&self) -> impl Future<Output = Option<Observed>> + Send {
        let (mac, ssid) = *self.0.lock().unwrap_or_else(PoisonError::into_inner);
        std::future::ready(mac.map(|mac| Observed {
            gateway_mac: mac.to_owned(),
            ssid: ssid.map(str::to_owned),
        }))
    }
}

const HOME: &str = "02:00:00:00:00:01";
const CAFE: &str = "02:00:00:00:00:02";

fn ip(text: &str) -> IpAddr {
    text.parse().expect("literal address")
}

struct Harness {
    _dir: TempDir,
    path: PathBuf,
    clock: Arc<TestClock>,
    gateway: Arc<Gateway>,
    names: HashMap<&'static str, IpAddr>,
}

impl Harness {
    fn new() -> Self {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("pingr.db");
        Self {
            _dir: dir,
            path,
            clock: TestClock::new(),
            gateway: Gateway::new(Some(HOME)),
            names: HashMap::new(),
        }
    }

    /// Collects `targets` against `prober` until `script` returns.
    async fn collect<F>(
        &self,
        targets: &[&str],
        prober: SimProber<F>,
        script: impl Future<Output = ()>,
    ) -> (Result<(), CollectError>, Vec<Event>)
    where
        F: Fn(IpAddr, u64) -> Sim + Send + Sync + 'static,
    {
        let config = Config::new(targets.iter().map(|&name| name.to_owned()).collect());
        self.collect_with(config, prober, script).await
    }

    /// Collects as `config` says against `prober` until `script` returns.
    async fn collect_with<F>(
        &self,
        config: Config,
        prober: SimProber<F>,
        script: impl Future<Output = ()>,
    ) -> (Result<(), CollectError>, Vec<Event>)
    where
        F: Fn(IpAddr, u64) -> Sim + Send + Sync + 'static,
    {
        let store = Store::create_or_open(&self.path)
            .expect("create store")
            .collect()
            .expect("claim store");
        let deps = Deps {
            prober: Arc::new(prober),
            resolver: Arc::new(Names(self.names.clone())),
            wall: Arc::clone(&self.clock),
            network: Arc::clone(&self.gateway),
        };
        let (events, mut seen) = broadcast::channel(4096);
        let result = run(config, deps, store, events, script).await;
        let mut log = Vec::new();
        while let Ok(event) = seen.try_recv() {
            log.push(event);
        }
        (result, log)
    }

    fn samples(&self, target: &str) -> Vec<Sample> {
        Store::open(&self.path)
            .expect("open")
            .samples(target, EVERYTHING)
            .expect("samples")
    }

    fn store(&self) -> Store {
        Store::open(&self.path).expect("open")
    }
}

fn prompt(timeout: Duration) -> SimProber<impl Fn(IpAddr, u64) -> Sim + Send + Sync> {
    SimProber::new(timeout, |_, _| Sim::Reply(Duration::from_millis(20)))
}

/// Seconds after [`START`] at which each sample was sent.
fn sent_at(samples: &[Sample]) -> Vec<f64> {
    samples
        .iter()
        .map(|sample| {
            Duration::from_micros(
                u64::try_from(sample.sent.as_micros() - START.as_micros()).expect("after start"),
            )
            .as_secs_f64()
        })
        .collect()
}

#[tokio::test(start_paused = true)]
async fn probes_go_out_on_a_cadence_that_does_not_drift() {
    let harness = Harness::new();
    // Answers take most of the interval; the schedule must not wait on them.
    let slow = SimProber::new(2 * SECOND, |_, _| Sim::Reply(Duration::from_millis(900)));

    let (result, _) = harness
        .collect(
            &["192.0.2.1"],
            slow,
            tokio::time::sleep(Duration::from_millis(9_500)),
        )
        .await;

    result.expect("clean stop");
    let samples = harness.samples("192.0.2.1");
    assert_eq!(
        sent_at(&samples),
        [0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0]
    );
    let seqs: Vec<_> = samples.iter().map(|sample| sample.seq).collect();
    assert_eq!(seqs, (0..10).collect::<Vec<_>>());
}

#[tokio::test(start_paused = true)]
async fn a_target_that_never_answers_does_not_delay_one_that_does() {
    let harness = Harness::new();
    let dead = ip("192.0.2.66");
    let prober = SimProber::new(2 * SECOND, move |addr, _| {
        if addr == dead {
            Sim::Silent
        } else {
            Sim::Reply(Duration::from_millis(20))
        }
    });

    let (result, _) = harness
        .collect(
            &["192.0.2.1", "192.0.2.66"],
            prober,
            tokio::time::sleep(Duration::from_millis(4_500)),
        )
        .await;

    result.expect("clean stop");
    let live = harness.samples("192.0.2.1");
    assert_eq!(sent_at(&live), [0.0, 1.0, 2.0, 3.0, 4.0]);
    assert!(
        live.iter()
            .all(|sample| matches!(sample.outcome, Outcome::Reply { .. }))
    );
    // Every probe to the dead target is still accounted for, as lost,
    // including those in flight when the collector was asked to stop.
    let dead = harness.samples("192.0.2.66");
    assert_eq!(dead.len(), 5);
    assert!(
        dead.iter()
            .all(|sample| sample.outcome == Outcome::Lost(Loss::Timeout))
    );
}

#[tokio::test(start_paused = true)]
async fn a_suspend_becomes_a_gap_between_sessions_without_a_burst_of_probes() {
    let harness = Harness::new();
    let clock = Arc::clone(&harness.clock);

    let (result, events) = harness
        .collect(&["192.0.2.1"], prompt(2 * SECOND), async {
            tokio::time::sleep(Duration::from_millis(2_500)).await;
            // The machine sleeps for an hour: the wall clock moves on, the
            // monotonic clock does not.
            clock.jump(Duration::from_secs(3600));
            tokio::time::sleep(Duration::from_millis(3_000)).await;
        })
        .await;

    result.expect("clean stop");
    let samples = harness.samples("192.0.2.1");
    assert_eq!(
        sent_at(&samples),
        [0.0, 1.0, 2.0, 3603.0, 3604.0, 3605.0],
        "one probe per tick on both sides of the gap, none inside it"
    );

    let sessions = harness
        .store()
        .sessions("192.0.2.1", EVERYTHING)
        .expect("sessions");
    assert_eq!(sessions.len(), 2);
    assert_eq!(
        sessions[0].ended,
        Some((START.plus(2 * SECOND), EndReason::ClockJump))
    );
    assert_eq!(sessions[1].started, START.plus(Duration::from_secs(3603)));
    // Sequence numbers restart with the new session.
    assert_eq!(samples[3].seq, 0);
    assert_ne!(samples[2].session, samples[3].session);
    assert!(events.contains(&Event::SessionEnded {
        at: START.plus(2 * SECOND),
        reason: EndReason::ClockJump,
    }));
}

#[tokio::test(start_paused = true)]
async fn a_stalled_schedule_becomes_a_gap_not_a_burst() {
    let harness = Harness::new();

    let (result, _) = harness
        .collect(&["192.0.2.1"], prompt(2 * SECOND), async {
            tokio::time::sleep(Duration::from_millis(1_500)).await;
            // Both clocks move a minute on with no tick served in between.
            tokio::time::advance(Duration::from_secs(60)).await;
            tokio::time::sleep(Duration::from_millis(1_000)).await;
        })
        .await;

    result.expect("clean stop");
    let samples = harness.samples("192.0.2.1");
    assert!(
        samples.len() <= 5,
        "missed ticks were replayed: {samples:?}"
    );
    let sessions = harness
        .store()
        .sessions("192.0.2.1", EVERYTHING)
        .expect("sessions");
    assert_eq!(sessions.len(), 2);
    assert_eq!(
        sessions[0].ended,
        Some((START.plus(SECOND), EndReason::Stalled))
    );
}

#[tokio::test(start_paused = true)]
async fn a_name_is_probed_at_its_address_and_an_unknown_name_is_recorded_as_unresolved() {
    let mut harness = Harness::new();
    harness.names.insert("known.test", ip("192.0.2.9"));

    let (result, events) = harness
        .collect(
            &["known.test", "unknown.test"],
            prompt(2 * SECOND),
            tokio::time::sleep(Duration::from_millis(1_500)),
        )
        .await;

    result.expect("clean stop");
    let known = harness.samples("known.test");
    assert_eq!(known.len(), 2);
    assert!(
        known
            .iter()
            .all(|sample| sample.address == Some(ip("192.0.2.9")))
    );
    let unknown = harness.samples("unknown.test");
    assert_eq!(unknown.len(), 2);
    assert!(unknown.iter().all(
        |sample| sample.outcome == Outcome::Lost(Loss::Unresolved) && sample.address.is_none()
    ));
    assert!(events.contains(&Event::Resolved {
        target: 0,
        ip: ip("192.0.2.9"),
    }));
    assert!(
        events
            .iter()
            .any(|event| matches!(event, Event::ResolveFailed { target: 1, .. }))
    );
}

#[tokio::test(start_paused = true)]
async fn a_clean_stop_closes_the_session() {
    let harness = Harness::new();

    let (result, _) = harness
        .collect(
            &["192.0.2.1"],
            prompt(2 * SECOND),
            tokio::time::sleep(Duration::from_millis(2_500)),
        )
        .await;

    result.expect("clean stop");
    let sessions = harness
        .store()
        .sessions("192.0.2.1", EVERYTHING)
        .expect("sessions");
    assert_eq!(sessions.len(), 1);
    assert_eq!(
        sessions[0].ended,
        Some((START.plus(Duration::from_millis(2_500)), EndReason::Stopped))
    );
    assert!(!harness.store().is_collecting().expect("status"));
}

#[tokio::test(start_paused = true)]
async fn a_system_that_forbids_unprivileged_pings_is_refused_at_startup_with_the_fix() {
    let harness = Harness::new();

    let (result, _) = harness
        .collect(
            &["192.0.2.1"],
            prompt(2 * SECOND).forbidden(),
            std::future::pending(),
        )
        .await;

    let error = result.expect_err("must not start");
    assert!(matches!(
        error,
        CollectError::Probe(ProbeError::NotPermitted(_))
    ));
    if cfg!(target_os = "linux") {
        assert!(error.to_string().contains("net.ipv4.ping_group_range"));
    }
    // Nothing was recorded: not even a session.
    assert_eq!(harness.store().targets().expect("targets"), []);
}

/// Real time, real contention: another process holds the store's write lock
/// for a while, as a long prune would.
#[tokio::test]
async fn samples_taken_while_the_store_is_busy_are_kept_and_written_later() {
    let harness = Harness::new();
    let store = Store::create_or_open(&harness.path).expect("create store");
    // Give up on a busy store quickly, so the retry path is what gets used.
    store
        .set_busy_timeout(Duration::from_millis(5))
        .expect("busy timeout");
    let mut config = Config::new(vec!["192.0.2.1".to_owned()]);
    config.interval = Duration::from_millis(40);
    config.timeout = Duration::from_millis(30);
    let deps = Deps {
        prober: Arc::new(SimProber::new(config.timeout, |_, _| {
            Sim::Reply(Duration::from_millis(1))
        })),
        resolver: Arc::new(Names(HashMap::new())),
        wall: Arc::new(pingr_engine::clock::SystemClock),
        network: Gateway::new(Some(HOME)),
    };
    let (events, mut seen) = broadcast::channel(4096);
    let path = harness.path.clone();

    let result = run(
        config,
        deps,
        store.collect().expect("claim"),
        events,
        async {
            tokio::time::sleep(Duration::from_millis(150)).await;
            let blocker = rusqlite::Connection::open(&path).expect("second connection");
            blocker
                .execute_batch("BEGIN IMMEDIATE")
                .expect("take the write lock");
            tokio::time::sleep(Duration::from_millis(300)).await;
            blocker
                .execute_batch("COMMIT")
                .expect("release the write lock");
            tokio::time::sleep(Duration::from_millis(150)).await;
        },
    )
    .await;

    result.expect("clean stop");
    let mut troubled = false;
    while let Ok(event) = seen.try_recv() {
        troubled |= matches!(event, Event::StoreTrouble { .. });
    }
    assert!(
        troubled,
        "the store was never reported busy: the test proved nothing"
    );
    let seqs: Vec<_> = harness
        .samples("192.0.2.1")
        .iter()
        .map(|sample| sample.seq)
        .collect();
    let expected: Vec<_> = (0..u32::try_from(seqs.len()).expect("few samples")).collect();
    assert_eq!(seqs, expected, "a sample went missing");
    assert!(
        seqs.len() >= 10,
        "probing stopped while the store was busy: {seqs:?}"
    );
}

/// A name that resolves to one address at first and another ever after.
struct Moving {
    lookups: std::sync::atomic::AtomicUsize,
    first: IpAddr,
    then: IpAddr,
}

impl Resolver for Moving {
    fn resolve(&self, _: &str) -> impl Future<Output = io::Result<Vec<IpAddr>>> + Send {
        let nth = self.lookups.fetch_add(1, Ordering::SeqCst);
        std::future::ready(Ok(vec![if nth == 0 { self.first } else { self.then }]))
    }
}

#[tokio::test(start_paused = true)]
async fn when_a_name_moves_the_new_address_is_probed_and_the_old_one_released() {
    let harness = Harness::new();
    let (old, new) = (ip("192.0.2.10"), ip("192.0.2.20"));
    let prober = Arc::new(prompt(2 * SECOND));
    let deps = Deps {
        prober: Arc::clone(&prober),
        resolver: Arc::new(Moving {
            lookups: std::sync::atomic::AtomicUsize::new(0),
            first: old,
            then: new,
        }),
        wall: Arc::clone(&harness.clock),
        network: Arc::clone(&harness.gateway),
    };
    let store = Store::create_or_open(&harness.path)
        .expect("create store")
        .collect()
        .expect("claim store");
    let (events, _seen) = broadcast::channel(4096);

    // Names are looked up again every five minutes; run just past that.
    run(
        Config::new(vec!["moving.test".to_owned()]),
        deps,
        store,
        events,
        tokio::time::sleep(Duration::from_millis(302_500)),
    )
    .await
    .expect("clean stop");

    let samples = harness.samples("moving.test");
    assert_eq!(samples.first().and_then(|sample| sample.address), Some(old));
    let at_new = samples
        .iter()
        .filter(|sample| sample.address == Some(new))
        .count();
    assert_eq!(at_new, 2, "probes at 301 s and 302 s go to the new address");
    assert_eq!(prober.forgotten(), [old]);
}

#[tokio::test(start_paused = true)]
async fn moving_to_another_network_starts_a_new_session_on_it() {
    let harness = Harness::new();
    let gateway = Arc::clone(&harness.gateway);

    let (result, events) = harness
        .collect(&["192.0.2.1"], prompt(2 * SECOND), async {
            tokio::time::sleep(Duration::from_millis(2_500)).await;
            gateway.set(Some(CAFE));
            tokio::time::sleep(Duration::from_secs(20)).await;
        })
        .await;

    result.expect("clean stop");
    let store = harness.store();
    let sessions = store.sessions("192.0.2.1", EVERYTHING).expect("sessions");
    assert_eq!(sessions.len(), 2);
    assert!(matches!(
        sessions[0].ended,
        Some((_, EndReason::NetworkChanged))
    ));
    assert_ne!(sessions[0].network, sessions[1].network);

    // Every sample carries the network of the session that sent it, and
    // none is lost in the switch.
    let samples = harness.samples("192.0.2.1");
    let first_on_cafe = samples
        .iter()
        .position(|sample| sample.network == sessions[1].network)
        .expect("samples on the second network");
    assert!(
        samples[..first_on_cafe]
            .iter()
            .all(|sample| sample.network == sessions[0].network)
    );
    assert_eq!(
        sent_at(&samples),
        (0..23).map(f64::from).collect::<Vec<_>>()
    );

    let gateways: Vec<_> = store
        .networks()
        .expect("networks")
        .into_iter()
        .map(|network| network.gateway_mac)
        .collect();
    assert_eq!(gateways, [Some(HOME.to_owned()), Some(CAFE.to_owned())]);
    let announced: Vec<_> = events
        .iter()
        .filter_map(|event| match event {
            Event::Network { network } => Some(network.clone()),
            _ => None,
        })
        .collect();
    let detected = |mac: &str| {
        Some(SessionNetwork::Detected(Observed {
            gateway_mac: mac.to_owned(),
            ssid: None,
        }))
    };
    assert_eq!(announced, [detected(HOME), detected(CAFE)]);
}

#[tokio::test(start_paused = true)]
async fn losing_the_default_route_is_loss_on_the_same_network_not_a_new_session() {
    let harness = Harness::new();
    let gateway = Arc::clone(&harness.gateway);

    let (result, _) = harness
        .collect(&["192.0.2.1"], prompt(2 * SECOND), async {
            tokio::time::sleep(Duration::from_millis(2_500)).await;
            gateway.set(None);
            tokio::time::sleep(Duration::from_secs(15)).await;
            gateway.set(Some(HOME));
            tokio::time::sleep(Duration::from_secs(15)).await;
        })
        .await;

    result.expect("clean stop");
    let sessions = harness
        .store()
        .sessions("192.0.2.1", EVERYTHING)
        .expect("sessions");
    assert_eq!(sessions.len(), 1);
}

#[tokio::test(start_paused = true)]
async fn a_network_first_seen_after_startup_ends_the_unknown_session() {
    let harness = Harness::new();
    harness.gateway.set(None);
    let gateway = Arc::clone(&harness.gateway);

    let (result, _) = harness
        .collect(&["192.0.2.1"], prompt(2 * SECOND), async {
            tokio::time::sleep(Duration::from_millis(2_500)).await;
            gateway.set(Some(HOME));
            tokio::time::sleep(Duration::from_secs(15)).await;
        })
        .await;

    result.expect("clean stop");
    let store = harness.store();
    let sessions = store.sessions("192.0.2.1", EVERYTHING).expect("sessions");
    assert_eq!(sessions.len(), 2);
    assert_eq!(sessions[0].network, None);
    assert_eq!(
        sessions[1].network,
        Some(store.network_by_label("network-1").expect("labeled"))
    );
}

#[tokio::test(start_paused = true)]
async fn a_named_network_is_used_for_every_session_and_never_detected() {
    let harness = Harness::new();
    let gateway = Arc::clone(&harness.gateway);
    let mut config = Config::new(vec!["192.0.2.1".to_owned()]);
    config.network = Some("cafe".to_owned());

    let (result, _) = harness
        .collect_with(config, prompt(2 * SECOND), async {
            tokio::time::sleep(Duration::from_millis(2_500)).await;
            gateway.set(Some(CAFE));
            tokio::time::sleep(Duration::from_secs(15)).await;
        })
        .await;

    result.expect("clean stop");
    let store = harness.store();
    let sessions = store.sessions("192.0.2.1", EVERYTHING).expect("sessions");
    assert_eq!(sessions.len(), 1);
    assert_eq!(
        sessions[0].network,
        Some(store.network_by_label("cafe").expect("labeled"))
    );
    let networks = store.networks().expect("networks");
    assert_eq!(networks.len(), 1);
    assert_eq!(networks[0].gateway_mac, None);
}

#[tokio::test(start_paused = true)]
async fn a_wifi_name_names_a_new_network_but_its_change_alone_is_no_move() {
    let harness = Harness::new();
    harness.gateway.set_ssid(Some("Home"));
    let gateway = Arc::clone(&harness.gateway);

    let (result, _) = harness
        .collect(&["192.0.2.1"], prompt(2 * SECOND), async {
            tokio::time::sleep(Duration::from_millis(2_500)).await;
            // The name drops out, as it does when the tool times out.
            gateway.set_ssid(None);
            tokio::time::sleep(Duration::from_secs(25)).await;
        })
        .await;

    result.expect("clean stop");
    let store = harness.store();
    let sessions = store.sessions("192.0.2.1", EVERYTHING).expect("sessions");
    assert_eq!(sessions.len(), 1);
    assert_eq!(
        sessions[0].network,
        Some(
            store
                .network_by_label("Home")
                .expect("labeled by its Wi-Fi name")
        )
    );
}
