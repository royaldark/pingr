//! `pingr run`: collect until interrupted, showing progress as it goes.

use std::fs;
use std::io::{self, IsTerminal, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use pingr_engine::clock::SystemClock;
use pingr_engine::collector::{self, CollectError, Config, Deps, Event};
use pingr_engine::prober::icmp::IcmpProber;
use pingr_engine::resolve::SystemResolver;
use pingr_engine::store::Store;
use serde::Deserialize;
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::{broadcast, mpsc, oneshot};

use crate::chain;
use crate::output::ping::Ping;
use crate::output::summary::Summary;
use crate::output::tui::Tui;
use crate::output::{Display, Mode, Target, Totals};
use crate::when::Span;

#[derive(Debug, clap::Args)]
pub struct RunArgs {
    /// Host names or IP addresses to probe
    targets: Vec<String>,
    /// A TOML file listing targets and settings
    #[arg(long, value_name = "FILE")]
    config: Option<PathBuf>,
    /// Time between probes to each target [default: 1s]
    #[arg(long, value_name = "SPAN")]
    interval: Option<Span>,
    /// How long to wait for an answer before a probe counts as lost [default: 2s]
    #[arg(long, value_name = "SPAN")]
    timeout: Option<Span>,
    /// What to show while collecting [default: tui in a terminal, summary otherwise]
    #[arg(long, value_enum)]
    output: Option<Mode>,
    /// How often summary mode reports on each target
    #[arg(long, value_name = "SPAN", default_value = "1m")]
    report_every: Span,
    /// Stop once every target has this many results
    #[arg(long, value_name = "N")]
    count: Option<u64>,
}

/// The settings file: everything in it can also be given as arguments, which
/// win.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileConfig {
    #[serde(default)]
    targets: Vec<String>,
    interval: Option<String>,
    timeout: Option<String>,
}

fn file_config(path: Option<&Path>) -> Result<FileConfig, String> {
    let Some(path) = path else {
        return Ok(FileConfig::default());
    };
    let text = fs::read_to_string(path)
        .map_err(|error| format!("cannot read {}: {error}", path.display()))?;
    toml::from_str(&text).map_err(|error| format!("{}: {error}", path.display()))
}

fn settings(args: &RunArgs) -> Result<Config, String> {
    let file = file_config(args.config.as_deref())?;
    let span = |text: Option<String>| text.map(|text| text.parse::<Span>()).transpose();

    let mut targets = args.targets.clone();
    for target in file.targets {
        if !targets.contains(&target) {
            targets.push(target);
        }
    }
    if targets.is_empty() {
        return Err(
            "nothing to probe: name at least one target, e.g. `pingr 1.1.1.1 example.com`"
                .to_owned(),
        );
    }
    let mut config = Config::new(targets);
    if let Some(Span(interval)) = args.interval.or(span(file.interval)?) {
        config.interval = interval;
    }
    if let Some(Span(timeout)) = args.timeout.or(span(file.timeout)?) {
        config.timeout = timeout;
    }
    if config.interval.is_zero() || config.timeout.is_zero() {
        return Err("--interval and --timeout must be longer than zero".to_owned());
    }
    Ok(config)
}

/// Waits for the quit key in modes that have one; never completes otherwise.
async fn quit_requested(quit_key: &mut Option<mpsc::Receiver<()>>) -> Option<()> {
    match quit_key {
        Some(quit_key) => quit_key.recv().await,
        None => std::future::pending().await,
    }
}

fn collect_error(error: &CollectError) -> String {
    chain(error)
}

pub fn run(db: &Path, args: &RunArgs) -> Result<(), String> {
    let config = settings(args)?;
    let interactive = io::stdout().is_terminal();
    let mode = args.output.unwrap_or(if interactive {
        Mode::Tui
    } else {
        Mode::Summary
    });
    if mode == Mode::Tui && !interactive {
        return Err(
            "--output tui needs a terminal: use --output summary or --output ping here".to_owned(),
        );
    }

    if let Some(dir) = db.parent().filter(|dir| !dir.as_os_str().is_empty()) {
        fs::create_dir_all(dir)
            .map_err(|error| format!("cannot create {}: {error}", dir.display()))?;
    }
    let store = Store::create_or_open(db)
        .and_then(Store::collect)
        .map_err(|error| chain(&error))?;

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|error| chain(&error))?;
    runtime.block_on(collect(db, config, store, mode, args))
}

async fn collect(
    db: &Path,
    config: Config,
    store: pingr_engine::store::Collecting,
    mode: Mode,
    args: &RunArgs,
) -> Result<(), String> {
    let targets: Vec<Target> = config
        .targets
        .iter()
        .map(|name| Target {
            name: name.clone(),
            ip: None,
        })
        .collect();
    let deps = Deps {
        prober: Arc::new(IcmpProber::new(config.timeout)),
        resolver: Arc::new(SystemResolver),
        wall: Arc::new(SystemClock),
    };

    let mut out = io::stdout();
    let io_error = |error: io::Error| chain(&error);
    let heading = format!(
        "pingr: probing {} every {:?}, recording to {}",
        config.targets.join(", "),
        config.interval,
        db.display()
    );
    let mut quit_key = None;
    let mut display: Box<dyn Display> = match mode {
        Mode::Summary => Box::new(Summary::new(targets.clone())),
        Mode::Ping => Box::new(Ping::new(targets.clone())),
        Mode::Tui => {
            let (tui, quit) = Tui::start(heading.clone(), targets.clone());
            quit_key = Some(quit);
            Box::new(tui)
        }
    };
    if mode != Mode::Tui {
        writeln!(out, "{heading}").map_err(io_error)?;
    }

    let (events, mut seen) = broadcast::channel(1024);
    let (stop, stopped) = oneshot::channel::<()>();
    let mut stop = Some(stop);
    let mut collector = tokio::spawn(collector::run(config, deps, store, events, async {
        // Either an explicit stop or the sender going away ends the run.
        let _ = stopped.await;
    }));

    let mut totals = vec![Totals::default(); targets.len()];
    let mut report = tokio::time::interval(args.report_every.0.max(Duration::from_millis(100)));
    report.tick().await;
    let mut redraw = tokio::time::interval(Duration::from_millis(200));
    let interrupt = tokio::signal::ctrl_c();
    tokio::pin!(interrupt);

    let result = loop {
        tokio::select! {
            finished = &mut collector => break finished,
            _ = &mut interrupt, if stop.is_some() => {
                stop.take();
            }
            Some(()) = quit_requested(&mut quit_key), if stop.is_some() => {
                stop.take();
            }
            _ = redraw.tick() => display.redraw().map_err(io_error)?,
            _ = report.tick() => display.report(&mut out).map_err(io_error)?,
            event = seen.recv() => match event {
                Ok(event) => {
                    if let Event::Probe { target, outcome, .. } = &event {
                        totals[*target].add(*outcome);
                    }
                    display.event(&event, &mut out).map_err(io_error)?;
                    let enough = args
                        .count
                        .is_some_and(|count| totals.iter().all(|total| total.sent >= count));
                    if enough {
                        stop.take();
                    }
                }
                Err(RecvError::Lagged(_)) => {}
                Err(RecvError::Closed) => break (&mut collector).await,
            },
        }
    };

    // Whatever was announced before the collector finished still gets shown.
    while let Ok(event) = seen.try_recv() {
        if let Event::Probe {
            target, outcome, ..
        } = &event
        {
            totals[*target].add(*outcome);
        }
        display.event(&event, &mut out).map_err(io_error)?;
    }
    // Hands the screen back before anything more is printed.
    drop(display);
    writeln!(out).map_err(io_error)?;
    for (target, total) in targets.iter().zip(&totals) {
        writeln!(out, "{}", total.line(&target.name)).map_err(io_error)?;
    }

    match result {
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) => Err(collect_error(&error)),
        Err(panic) => Err(format!("the collector crashed: {panic}")),
    }
}
