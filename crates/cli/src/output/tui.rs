//! A live table of targets, redrawn in place. Read-only: the only input is
//! the key to quit.

use std::collections::VecDeque;
use std::io::{self, Write};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

use pingr_engine::collector::Event;
use pingr_engine::stats::summarize;
use pingr_engine::store::Outcome;
use ratatui::crossterm::event::{self, KeyCode, KeyEventKind, KeyModifiers};
use ratatui::layout::{Constraint, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span, Text};
use ratatui::widgets::{Cell, Row, Table};
use ratatui::{DefaultTerminal, Frame};
use tokio::sync::mpsc;

use super::health::{self, Health, Limits};
use super::{DOWN_AFTER, Display, Target, Totals, notice};
use crate::fmt;

/// How many of the latest probes the "recent" columns and the row's color
/// cover.
const RECENT: usize = 60;

struct TargetLine {
    recent: VecDeque<Outcome>,
    losses_in_a_row: u32,
    totals: Totals,
}

fn color(health: Health) -> Color {
    match health {
        Health::Good => Color::Green,
        Health::Fair => Color::Yellow,
        Health::Poor => Color::Red,
    }
}

/// What the colors mean, e.g. `good ≤ 2% loss, 100 ms p95, 10 ms jitter`.
fn legend() -> Line<'static> {
    let limits = |limits: &Limits| {
        (
            format!("{}%", limits.loss * 100.0),
            format!("{} ms", limits.p95.as_millis()),
            format!("{} ms", limits.jitter.as_millis()),
        )
    };
    let (loss, p95, jitter) = limits(&health::GOOD);
    let good = format!(" ≤ {loss} loss, {p95} p95, {jitter} jitter   ");
    let (loss, p95, jitter) = limits(&health::FAIR);
    let fair = format!(" ≤ {loss}, {p95}, {jitter}   ");
    let grade = |name: &'static str, health| Span::styled(name, Style::new().fg(color(health)));
    Line::from(vec![
        grade("good", Health::Good),
        Span::raw(good),
        grade("fair", Health::Fair),
        Span::raw(fair),
        grade("poor", Health::Poor),
        Span::raw(format!(" worse, or {DOWN_AFTER} lost in a row")),
    ])
}

/// What the table shows: everything the screen is drawn from.
pub struct View {
    heading: String,
    targets: Vec<Target>,
    lines: Vec<TargetLine>,
    /// The latest thing worth telling the user, shown under the table.
    notice: String,
}

impl View {
    pub fn new(heading: String, targets: Vec<Target>) -> Self {
        let lines = targets
            .iter()
            .map(|_| TargetLine {
                recent: VecDeque::with_capacity(RECENT),
                losses_in_a_row: 0,
                totals: Totals::default(),
            })
            .collect();
        Self {
            heading,
            targets,
            lines,
            notice: String::new(),
        }
    }

    /// Takes in one collector event.
    pub fn apply(&mut self, event: &Event) {
        if let Some(line) = notice(event, &mut self.targets) {
            self.notice = line;
        }
        if let Event::Probe {
            target, outcome, ..
        } = event
        {
            let line = &mut self.lines[*target];
            if line.recent.len() == RECENT {
                line.recent.pop_front();
            }
            line.recent.push_back(*outcome);
            line.losses_in_a_row = match outcome {
                Outcome::Lost(_) => line.losses_in_a_row + 1,
                Outcome::Reply { .. } => 0,
            };
            line.totals.add(*outcome);
        }
    }

    pub fn draw(&self, frame: &mut Frame) {
        let dash = || "-".to_owned();
        let rows = self.targets.iter().zip(&self.lines).map(|(target, line)| {
            let recent = summarize(line.recent.iter().copied());
            let timing = |pick: fn(&pingr_engine::stats::RttStats) -> Option<Duration>| {
                recent
                    .rtt
                    .as_ref()
                    .and_then(pick)
                    .map_or_else(dash, fmt::rtt)
            };
            let style = Health::judge(&recent, line.losses_in_a_row)
                .map_or_else(Style::new, |health| Style::new().fg(color(health)));
            Row::new([
                Cell::from(target.name.clone()),
                Cell::from(target.ip.map_or_else(dash, |ip| ip.to_string())),
                Cell::from(
                    line.recent
                        .back()
                        .map_or_else(dash, |last| fmt::outcome(*last)),
                ),
                Cell::from(recent.loss().map_or_else(dash, fmt::percent)),
                Cell::from(timing(|rtt| Some(rtt.mean))),
                Cell::from(timing(|rtt| Some(rtt.p95))),
                Cell::from(timing(|rtt| Some(rtt.max))),
                Cell::from(timing(|rtt| rtt.jitter)),
                Cell::from(line.totals.sent.to_string()),
                Cell::from((line.totals.sent - line.totals.received).to_string()),
            ])
            .style(style)
        });
        let widths = [
            Constraint::Fill(2),
            Constraint::Fill(2),
            Constraint::Length(20),
            Constraint::Length(6),
            Constraint::Length(9),
            Constraint::Length(9),
            Constraint::Length(9),
            Constraint::Length(9),
            Constraint::Length(9),
            Constraint::Length(7),
        ];
        let header = Row::new([
            "target", "address", "last", "loss", "mean", "p95", "max", "jitter", "sent", "lost",
        ])
        .style(Style::new().add_modifier(Modifier::BOLD));

        let [top, body, bottom] = Layout::vertical([
            Constraint::Length(3),
            Constraint::Fill(1),
            Constraint::Length(2),
        ])
        .areas(frame.area());
        frame.render_widget(
            vec![
                Line::from(self.heading.as_str()),
                Line::from(format!(
                    "loss, mean, p95, max, jitter, color: latest {RECENT} probes   \
                     sent, lost: this run"
                ))
                .style(Style::new().add_modifier(Modifier::DIM)),
                legend(),
            ]
            .into_iter()
            .collect::<Text>(),
            top,
        );
        frame.render_widget(Table::new(rows, widths).header(header), body);
        frame.render_widget(
            vec![
                Line::from(self.notice.as_str()),
                Line::from("q to stop").style(Style::new().add_modifier(Modifier::DIM)),
            ]
            .into_iter()
            .collect::<Text>(),
            bottom,
        );
    }
}

/// The terminal view as an output mode: owns the screen until dropped.
pub struct Tui {
    terminal: DefaultTerminal,
    view: View,
    dirty: bool,
    keys_wanted: Arc<AtomicBool>,
}

impl Tui {
    /// Takes over the terminal. The receiver yields once when the user asks
    /// to quit.
    pub fn start(heading: String, targets: Vec<Target>) -> (Self, mpsc::Receiver<()>) {
        let terminal = ratatui::init();
        let keys_wanted = Arc::new(AtomicBool::new(true));
        let (quit, quit_requested) = mpsc::channel(1);
        let wanted = Arc::clone(&keys_wanted);
        thread::spawn(move || {
            while wanted.load(Ordering::Relaxed) {
                // Poll with a timeout so the thread notices when to stop.
                if !event::poll(Duration::from_millis(100)).unwrap_or(false) {
                    continue;
                }
                let Ok(event::Event::Key(key)) = event::read() else {
                    continue;
                };
                // The terminal is in raw mode, so Ctrl-C arrives as a key.
                let interrupt =
                    key.code == KeyCode::Char('c') && key.modifiers.contains(KeyModifiers::CONTROL);
                let quit_key = matches!(key.code, KeyCode::Char('q') | KeyCode::Esc);
                if key.kind == KeyEventKind::Press && (interrupt || quit_key) {
                    let _ = quit.blocking_send(());
                    break;
                }
            }
        });
        let tui = Self {
            terminal,
            view: View::new(heading, targets),
            dirty: true,
            keys_wanted,
        };
        (tui, quit_requested)
    }
}

impl Display for Tui {
    fn event(&mut self, event: &Event, _: &mut dyn Write) -> io::Result<()> {
        self.view.apply(event);
        self.dirty = true;
        Ok(())
    }

    fn report(&mut self, _: &mut dyn Write) -> io::Result<()> {
        Ok(())
    }

    fn redraw(&mut self) -> io::Result<()> {
        if self.dirty {
            self.terminal.draw(|frame| self.view.draw(frame))?;
            self.dirty = false;
        }
        Ok(())
    }
}

impl Drop for Tui {
    fn drop(&mut self) {
        self.keys_wanted.store(false, Ordering::Relaxed);
        ratatui::restore();
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use pingr_engine::store::Loss;
    use pingr_engine::time::Timestamp;
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;

    use super::*;

    fn probe(target: usize, seq: u32, outcome: Outcome) -> Event {
        Event::Probe {
            target,
            sent: Timestamp::from_micros(0),
            seq,
            ip: None,
            outcome,
        }
    }

    fn draw(view: &View) -> Terminal<TestBackend> {
        let mut terminal = Terminal::new(TestBackend::new(120, 10)).expect("test terminal");
        terminal.draw(|frame| view.draw(frame)).expect("draw");
        terminal
    }

    /// The screen as text, one string per row.
    fn screen(view: &View) -> Vec<String> {
        let terminal = draw(view);
        let buffer = terminal.backend().buffer();
        (0..buffer.area.height)
            .map(|y| {
                (0..buffer.area.width)
                    .map(|x| buffer[(x, y)].symbol())
                    .collect()
            })
            .collect()
    }

    #[test]
    fn each_target_row_shows_its_own_latest_result_and_loss() {
        let targets = ["steady.test", "flaky.test"].map(|name| Target {
            name: name.to_owned(),
            ip: None,
        });
        let mut view = View::new("pingr".to_owned(), targets.to_vec());
        let reply = Outcome::Reply {
            rtt: Duration::from_millis(20),
        };
        view.apply(&Event::Resolved {
            target: 0,
            ip: "192.0.2.9".parse().expect("literal address"),
        });
        for seq in 0..4 {
            view.apply(&probe(0, seq, reply));
        }
        view.apply(&probe(1, 0, reply));
        view.apply(&probe(1, 1, Outcome::Lost(Loss::Timeout)));

        let screen = screen(&view);
        let row = |name: &str| {
            let line = screen
                .iter()
                .find(|line| line.starts_with(name))
                .unwrap_or_else(|| panic!("no row for {name}:\n{}", screen.join("\n")));
            line.split_whitespace().collect::<Vec<_>>().join(" ")
        };
        assert_eq!(
            row("steady.test"),
            "steady.test 192.0.2.9 20.0 ms 0.0% 20.0 ms 20.0 ms 20.0 ms 0.00 ms 4 0"
        );
        assert_eq!(
            row("flaky.test"),
            "flaky.test - lost (timeout) 50.0% 20.0 ms 20.0 ms 20.0 ms - 2 1"
        );
        assert!(
            screen
                .iter()
                .any(|line| line.contains("steady.test is 192.0.2.9")),
            "the address change is not announced:\n{}",
            screen.join("\n")
        );
    }

    #[test]
    fn each_row_is_colored_by_its_health_over_the_recent_probes() {
        let targets = ["up.test", "down.test", "waiting.test"].map(|name| Target {
            name: name.to_owned(),
            ip: None,
        });
        let mut view = View::new("pingr".to_owned(), targets.to_vec());
        let reply = Outcome::Reply {
            rtt: Duration::from_millis(20),
        };
        for seq in 0..10 {
            view.apply(&probe(0, seq, reply));
            view.apply(&probe(1, seq, reply));
        }
        for seq in 10..13 {
            view.apply(&probe(1, seq, Outcome::Lost(Loss::Timeout)));
        }

        let terminal = draw(&view);
        let buffer = terminal.backend().buffer();
        let color_of = |name: &str| {
            let y = screen(&view)
                .iter()
                .position(|line| line.starts_with(name))
                .unwrap_or_else(|| panic!("no row for {name}"));
            buffer[(0, u16::try_from(y).expect("a short screen"))].fg
        };
        assert_eq!(color_of("up.test"), Color::Green);
        assert_eq!(color_of("down.test"), Color::Red);
        assert_eq!(color_of("waiting.test"), Color::Reset);
    }
}
