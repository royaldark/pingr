# pingr

Track how stable a network connection is over weeks and months. pingr pings
any number of destinations around the clock, keeps every result, and answers
"how was my connection to X between A and B?" with loss, latency percentiles,
and jitter.

It runs on Linux, macOS, and Windows, and never needs root or administrator
rights.

## Install

Download the archive for your system from the
[releases page](https://github.com/royaldark/pingr/releases), unpack it, and
put `pingr` (`pingr.exe` on Windows) somewhere on your `PATH`. Each archive
has a `.sha256` file beside it to check the download against.

## Build

Tooling is managed by [mise](https://mise.jdx.dev):

```
mise install        # the pinned Rust toolchain and lint tools
mise run build      # debug build at target/debug/pingr
cargo install --path crates/cli    # or install `pingr` into ~/.cargo/bin
```

To run it without installing anything, use `mise run pingr --` in place of
`pingr`, e.g. `mise run pingr -- 1.1.1.1`.

## Use

Start collecting. This runs until you stop it (Ctrl-C, or `q` in the table
view):

```
pingr 1.1.1.1 example.com
```

`pingr run 1.1.1.1 example.com` is the same thing spelled out; use it for a
target that shares a name with a command.

In a terminal this shows a live table. Elsewhere (piped, under a service
manager) it prints a summary line per target every minute and a line whenever
something changes. Choose explicitly with `--output tui|summary|ping`.

Ask about the past from any other shell, while the collector keeps running:

```
pingr stats --last 2h
pingr stats example.com --since "2026-10-01 08:00" --until "2026-10-01 18:00"
pingr history example.com --last 10m
pingr targets
pingr status
```

Times are local unless they carry an offset (`2026-10-01T08:00:00Z`); a span
such as `90m` or `7d` means that long ago. `stats`, `history`, `targets`, and
`status` print JSON with `--json`.

Targets and settings can also come from a file, `pingr run --config pingr.toml`:

```toml
targets = ["1.1.1.1", "example.com"]
interval = "1s"
timeout = "2s"
network = "home"    # optional: see Networks below
```

### Networks

pingr notes which network each result was taken on, and `stats` reports each
network on its own row, so a laptop's home Wi-Fi and the café down the road are
never averaged together. A network is recognized by the hardware address of
its default gateway (the router), which works the same over Wi-Fi and Ethernet
and needs no special rights. New networks are labeled `network-1`,
`network-2`, and so on; give them names of your own:

```
pingr networks                          # each network, its gateway, when seen
pingr networks rename network-1 home
pingr stats --network home --last 7d    # one network only
pingr history example.com --network home --last 1h
```

When the machine moves to another network while collecting, pingr ends the
session and starts a new one on the new network. Losing the network altogether
(no default route) is not a move: probes keep going, and their losses count
against the network you were on.

To label results yourself instead, for example when a VPN hides the real
gateway, pass `pingr run --network <label>`: detection is then off, and every
result from that run goes under that label. Naming a label that a detected
network already has adds to that network's results.

## What the numbers mean

- **loss** counts probes that got no answer within the timeout. Time when no
  collector was running is reported separately as "no data" and is never
  counted as loss.
- **p90 / p95 / p99** are nearest-rank percentiles: each is a round-trip time
  that was actually observed.
- **jitter** is the mean difference between one answer and the next.
- **stddev** is the population standard deviation.

In the live table, each target's row is colored by its latest 60 probes:
green when loss is at most 2%, p95 at most 100 ms, and jitter at most 10 ms;
yellow within 5%, 250 ms, and 30 ms; red beyond that, or as soon as three
probes in a row go unanswered. A lone slow or lost probe does not change the
color, and a target that recovers turns green only once its trouble has left
the window.

## Things to know

**Privileges.** pingr uses each system's unprivileged ping mechanism. On Linux
that depends on a kernel setting most distributions enable by default; if
yours does not, pingr says so at startup and names the fix:

```
sudo sysctl -w net.ipv4.ping_group_range="0 2147483647"
```

**Disk.** Every sample is kept until you delete it: about 28 bytes each, so
one target probed every second is roughly 0.9 GB a year. `pingr status` shows
the current size. Reclaim space with:

```
pingr prune --before 90d --dry-run
pingr prune --before 90d
```

**Where the data is.** One SQLite file: `~/.local/share/pingr/pingr.db` on
Linux, `~/Library/Application Support/pingr/pingr.db` on macOS,
`%LOCALAPPDATA%\pingr\pingr.db` on Windows. Override with `--db` or
`PINGR_DB`. Keep it on a local disk; network filesystems are not supported.
Only one collector may use a store at a time.

**Keeping it running.** pingr does not install itself as a service. Run it in
tmux, or under your service manager. A systemd user unit:

```ini
[Unit]
Description=pingr collector

[Service]
ExecStart=%h/.cargo/bin/pingr run --config %h/.config/pingr.toml
Restart=always

[Install]
WantedBy=default.target
```

**Suspend and clock changes.** When the machine sleeps or its clock is changed,
pingr ends the current session and starts a new one, so the pause shows up as
a gap rather than as loss.

**Network changes.** The network is checked every ten seconds, so up to ten
seconds of results after a move are still counted toward the previous network.
When the gateway cannot be read (behind some VPNs, say), results are labeled
`unknown` unless you pass `--network`.

**Names.** A host name is looked up when collection starts and again every
five minutes; each sample records the address it was sent to.

## Not yet

Traceroute and per-hop timings, outage detection, charts, and a graphical
interface are planned and not part of this version.

## Develop

```
mise run test     # includes real pings to 127.0.0.1 and ::1
mise run lint     # fmt, clippy, rustdoc, cargo-deny, cargo-machete, typos
mise run bench    # statistics over a month of one-second samples
```

CI runs the tests on Linux, macOS, and Windows and the lint on Linux, for
pull requests and for `main`.
