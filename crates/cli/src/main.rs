//! Command-line interface for the pingr network stability tracker.

mod fmt;
mod output;
mod render;
mod run;
mod when;

use std::env;
use std::error::Error;
use std::io::{self, BufRead, IsTerminal, Write};
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::SystemTime;

use clap::error::ErrorKind;
use clap::parser::ValueSource;
use clap::{CommandFactory, FromArgMatches, Parser, Subcommand};
use pingr_engine::store::{Store, StoreError};
use pingr_engine::time::Timestamp;

use render::ReadError;
use when::{RangeArgs, When};

/// Track network stability: ping targets around the clock, then ask how the
/// connection has been.
#[derive(Debug, Parser)]
#[command(
    name = "pingr",
    version,
    about,
    max_term_width = 100,
    arg_required_else_help = true
)]
struct Cli {
    /// Where the data lives [default: pingr/pingr.db in your user data directory]
    #[arg(long, global = true, env = "PINGR_DB", value_name = "FILE")]
    db: Option<PathBuf>,
    #[command(subcommand)]
    command: Option<Command>,
    /// With no command, the arguments are those of `run`.
    #[command(flatten)]
    run: run::RunArgs,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Probe targets and record the results until interrupted (the default)
    Run(run::RunArgs),
    /// Loss and latency statistics per target over a time range
    Stats {
        /// Targets to report on [default: all]
        targets: Vec<String>,
        #[command(flatten)]
        range: RangeArgs,
        /// Print JSON instead of a table
        #[arg(long)]
        json: bool,
    },
    /// Every sample for one target over a time range
    History {
        target: String,
        #[command(flatten)]
        range: RangeArgs,
        /// Print JSON instead of text
        #[arg(long)]
        json: bool,
    },
    /// The targets that have data, and the span it covers
    Targets {
        /// Print JSON instead of a table
        #[arg(long)]
        json: bool,
    },
    /// Whether a collector is running, and how large the store has grown
    Status {
        /// Print JSON instead of text
        #[arg(long)]
        json: bool,
    },
    /// Delete samples older than a point in time
    Prune {
        /// Delete everything sent before this: a time, or a span ago such as 90d
        #[arg(long, value_name = "WHEN")]
        before: When,
        /// Show how much would be deleted, and delete nothing
        #[arg(long)]
        dry_run: bool,
        /// Do not ask for confirmation
        #[arg(long, short)]
        yes: bool,
    },
}

/// The per-user data directory, by each platform's convention.
fn data_dir() -> Option<PathBuf> {
    let var = |name| env::var_os(name).filter(|value| !value.is_empty());
    if cfg!(windows) {
        var("LOCALAPPDATA").map(PathBuf::from)
    } else if cfg!(target_os = "macos") {
        var("HOME").map(|home| PathBuf::from(home).join("Library/Application Support"))
    } else {
        var("XDG_DATA_HOME")
            .map(PathBuf::from)
            .or_else(|| var("HOME").map(|home| PathBuf::from(home).join(".local/share")))
    }
}

/// The store's default home: one file in the per-user data directory.
fn default_db() -> Result<PathBuf, String> {
    data_dir()
        .map(|dir| dir.join("pingr").join("pingr.db"))
        .ok_or_else(|| {
            "cannot tell where your user data directory is: pass --db or set PINGR_DB".to_owned()
        })
}

/// Parses the command line, refusing `run`'s arguments ahead of another
/// command: clap's own `args_conflicts_with_subcommands` would also stop
/// `--db` from coming before a command.
fn parse() -> Cli {
    let matches = Cli::command().get_matches();
    let given = |id: &str| matches.value_source(id) == Some(ValueSource::CommandLine);
    let stray = matches
        .ids()
        .map(clap::Id::as_str)
        .any(|id| id != "db" && given(id));
    if let (Some((name, _)), true) = (matches.subcommand(), stray) {
        Cli::command()
            .error(
                ErrorKind::ArgumentConflict,
                format!("targets and collecting options cannot come before `{name}`"),
            )
            .exit();
    }
    Cli::from_arg_matches(&matches).unwrap_or_else(|error| error.exit())
}

fn main() -> ExitCode {
    let cli = parse();
    match dispatch(cli) {
        Ok(()) => ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("error: {message}");
            ExitCode::FAILURE
        }
    }
}

/// An error and everything that caused it, on one line.
fn chain(error: &dyn Error) -> String {
    let mut text = error.to_string();
    let mut source = error.source();
    while let Some(cause) = source {
        text.push_str(": ");
        text.push_str(&cause.to_string());
        source = cause.source();
    }
    text
}

fn read_error(error: ReadError) -> String {
    match error {
        ReadError::Store(error) => chain(&error),
        ReadError::Io(error) => chain(&error),
        ReadError::Usage(message) => message,
    }
}

fn dispatch(cli: Cli) -> Result<(), String> {
    let db = match cli.db {
        Some(db) => db,
        None => default_db()?,
    };
    let now = Timestamp::from(SystemTime::now());
    let open = || Store::open(&db).map_err(|error: StoreError| chain(&error));
    let mut out = io::stdout().lock();

    match cli.command.unwrap_or(Command::Run(cli.run)) {
        Command::Run(args) => run::run(&db, &args),
        Command::Stats {
            targets,
            range,
            json,
        } => {
            let range = range.resolve(now)?;
            render::stats(&open()?, &targets, range, now, json, &mut out).map_err(read_error)
        }
        Command::History {
            target,
            range,
            json,
        } => {
            let range = range.resolve(now)?;
            render::history(&open()?, &target, range, json, &mut out).map_err(read_error)
        }
        Command::Targets { json } => render::targets(&open()?, json, &mut out).map_err(read_error),
        Command::Status { json } => {
            render::status(&open()?, now, json, &mut out).map_err(read_error)
        }
        Command::Prune {
            before,
            dry_run,
            yes,
        } => prune(&open()?, before.resolve(now), dry_run, yes, &mut out),
    }
}

fn prune(
    store: &Store,
    before: Timestamp,
    dry_run: bool,
    yes: bool,
    out: &mut impl Write,
) -> Result<(), String> {
    let io_error = |error: io::Error| chain(&error);
    let store_error = |error: StoreError| chain(&error);
    let count = store.count_before(before).map_err(store_error)?;
    let cutoff = fmt::datetime(before);
    if count == 0 {
        writeln!(out, "Nothing to delete: no samples before {cutoff}.").map_err(io_error)?;
        return Ok(());
    }
    if dry_run {
        writeln!(out, "Would delete {count} samples sent before {cutoff}.").map_err(io_error)?;
        return Ok(());
    }
    if !yes {
        if !io::stdin().is_terminal() {
            return Err(format!(
                "this would delete {count} samples sent before {cutoff}: \
                 pass --yes to confirm, or --dry-run to only look"
            ));
        }
        write!(out, "Delete {count} samples sent before {cutoff}? [y/N] ").map_err(io_error)?;
        out.flush().map_err(io_error)?;
        let mut answer = String::new();
        io::stdin()
            .lock()
            .read_line(&mut answer)
            .map_err(io_error)?;
        if !matches!(answer.trim(), "y" | "Y" | "yes") {
            writeln!(out, "Nothing deleted.").map_err(io_error)?;
            return Ok(());
        }
    }
    let removed = store.prune(before).map_err(store_error)?;
    writeln!(out, "Deleted {removed} samples.").map_err(io_error)?;
    Ok(())
}
