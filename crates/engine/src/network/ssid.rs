//! The Wi-Fi network's name, by asking the tools each system ships with.
//!
//! Best effort throughout: a tool that is missing, slow, refuses (macOS and
//! recent Windows hide the name from apps without location permission), or
//! prints something unexpected just means no name.

use std::io::Read;
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

/// How long a tool may take before it is given up on.
const TOOL_TIMEOUT: Duration = Duration::from_secs(2);

/// The name of the Wi-Fi network `interface` is joined to, if the system
/// says. `friendly` is the interface's display name, where there is one.
pub(super) fn lookup(interface: &str, friendly: Option<&str>) -> Option<String> {
    let name = if cfg!(target_os = "linux") {
        tool(
            "nmcli",
            &[
                "-t",
                "-f",
                "active,ssid",
                "dev",
                "wifi",
                "list",
                "ifname",
                interface,
                "--rescan",
                "no",
            ],
        )
        .and_then(|out| nmcli(&out))
        .or_else(|| tool("iw", &["dev", interface, "link"]).and_then(|out| iw(&out)))
    } else if cfg!(target_os = "macos") {
        tool("ipconfig", &["getsummary", interface])
            .and_then(|out| ipconfig(&out))
            .or_else(|| {
                tool("networksetup", &["-getairportnetwork", interface])
                    .and_then(|out| networksetup(&out))
            })
    } else if cfg!(windows) {
        tool("netsh", &["wlan", "show", "interfaces"]).and_then(|out| netsh(&out, friendly))
    } else {
        None
    };
    name.filter(|name| !name.is_empty())
}

/// Runs `program` and returns what it printed, if it succeeded in time.
fn tool(program: &str, args: &[&str]) -> Option<String> {
    let mut child = Command::new(program)
        .args(args)
        // Keeps the labels these parsers look for in English where the tool
        // honors it.
        .env("LC_ALL", "C")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .ok()?;
    let deadline = Instant::now() + TOOL_TIMEOUT;
    let status = loop {
        if let Some(status) = child.try_wait().ok()? {
            break status;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            return None;
        }
        thread::sleep(Duration::from_millis(20));
    };
    let mut out = Vec::new();
    child.stdout.take()?.read_to_end(&mut out).ok()?;
    status
        .success()
        .then(|| String::from_utf8_lossy(&out).into_owned())
}

/// `nmcli -t -f active,ssid dev wifi list`: `yes:Name` marks the one joined.
/// Terse output escapes `:` and `\` with a backslash.
fn nmcli(out: &str) -> Option<String> {
    out.lines()
        .find_map(|line| line.strip_prefix("yes:"))
        .map(|name| {
            let mut unescaped = String::new();
            let mut chars = name.chars();
            while let Some(c) = chars.next() {
                unescaped.push(if c == '\\' {
                    chars.next().unwrap_or(c)
                } else {
                    c
                });
            }
            unescaped
        })
}

/// `iw dev <interface> link`: a `SSID: Name` line, with bytes outside
/// printable ASCII written as `\xNN`.
fn iw(out: &str) -> Option<String> {
    let name = out
        .lines()
        .find_map(|line| line.trim_start().strip_prefix("SSID: "))?;
    let mut bytes = Vec::new();
    let mut rest = name.as_bytes();
    while let Some((&byte, after)) = rest.split_first() {
        let escaped = after
            .strip_prefix(b"x")
            .filter(|_| byte == b'\\')
            .and_then(|hex| hex.get(..2))
            .and_then(|hex| std::str::from_utf8(hex).ok())
            .and_then(|hex| u8::from_str_radix(hex, 16).ok());
        if let Some(decoded) = escaped {
            bytes.push(decoded);
            rest = &after[3..];
        } else {
            bytes.push(byte);
            rest = after;
        }
    }
    Some(String::from_utf8_lossy(&bytes).into_owned())
}

/// `ipconfig getsummary <interface>`: an `SSID : Name` line, which reads
/// `<redacted>` for apps without location permission.
fn ipconfig(out: &str) -> Option<String> {
    out.lines()
        .find_map(|line| line.trim_start().strip_prefix("SSID : "))
        .filter(|name| *name != "<redacted>")
        .map(str::to_owned)
}

/// `networksetup -getairportnetwork <interface>`: `Current Wi-Fi Network: Name`.
fn networksetup(out: &str) -> Option<String> {
    out.lines()
        .find_map(|line| line.strip_prefix("Current Wi-Fi Network: "))
        .map(|name| name.trim_end().to_owned())
}

/// `netsh wlan show interfaces`: a block of `Key : Value` lines per adapter.
/// The keys are translated except `SSID`, so the adapter is picked by its
/// display name appearing as a value, or as the only one with an SSID.
fn netsh(out: &str, friendly: Option<&str>) -> Option<String> {
    let mut blocks: Vec<Vec<(&str, &str)>> = Vec::new();
    for (key, value) in out.lines().filter_map(|line| line.split_once(':')) {
        let (key, value) = (key.trim(), value.trim());
        // Every adapter lists the same keys: a repeated one starts the next.
        match blocks.last_mut() {
            Some(block) if !block.iter().any(|(seen, _)| *seen == key) => {
                block.push((key, value));
            }
            _ => blocks.push(vec![(key, value)]),
        }
    }
    let ssid = |block: &Vec<(&str, &str)>| {
        block
            .iter()
            .find(|(key, _)| *key == "SSID")
            .map(|(_, value)| (*value).to_owned())
    };
    let with_ssid: Vec<_> = blocks
        .iter()
        .filter(|block| ssid(block).is_some())
        .collect();
    let chosen = match friendly {
        Some(friendly) => with_ssid
            .iter()
            .find(|block| block.iter().any(|(_, value)| *value == friendly))
            .or_else(|| with_ssid.first().filter(|_| with_ssid.len() == 1)),
        None => with_ssid.first().filter(|_| with_ssid.len() == 1),
    }?;
    ssid(chosen)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nmcli_names_the_joined_network_and_unescapes_it() {
        let out = "no:Neighbours\nyes:M\u{fc}ller\\: upstairs\nno:\n";
        assert_eq!(nmcli(out).as_deref(), Some("M\u{fc}ller: upstairs"));
        assert_eq!(nmcli("no:Neighbours\n"), None);
    }

    #[test]
    fn iw_decodes_escaped_bytes() {
        let out = "Connected to 8a:2d:ec:c5:f4:5f (on wlp2s0)\n\
                   \tSSID: M\\xc3\\xbcller Wi-Fi\n\
                   \tfreq: 5180\n";
        assert_eq!(iw(out).as_deref(), Some("M\u{fc}ller Wi-Fi"));
        assert_eq!(iw("Not connected.\n"), None);
    }

    #[test]
    fn ipconfig_gives_no_name_when_macos_redacts_it() {
        let named = "<dictionary> {\n  InterfaceType : WiFi\n  SSID : Home Net\n}\n";
        assert_eq!(ipconfig(named).as_deref(), Some("Home Net"));
        let redacted = "<dictionary> {\n  SSID : <redacted>\n}\n";
        assert_eq!(ipconfig(redacted), None);
    }

    #[test]
    fn networksetup_reads_the_current_network() {
        assert_eq!(
            networksetup("Current Wi-Fi Network: Home Net\n").as_deref(),
            Some("Home Net")
        );
        assert_eq!(
            networksetup("You are not associated with an AirPort network.\n"),
            None
        );
    }

    /// Two adapters, keys in German: only `SSID` can be relied on.
    const NETSH: &str = "
Es gibt 2 Schnittstellen im System:

    Name                   : WLAN
    Beschreibung           : Intel(R) Wi-Fi 6 AX201 160MHz
    Status                 : Verbunden
    SSID                   : Home: 5G
    BSSID                  : 8a:2d:ec:c5:f4:5f

    Name                   : WLAN 2
    Beschreibung           : USB Wi-Fi
    Status                 : Verbunden
    SSID                   : Office
    BSSID                  : 02:00:00:00:00:02
";

    #[test]
    fn netsh_picks_the_adapter_by_its_display_name() {
        assert_eq!(netsh(NETSH, Some("WLAN")).as_deref(), Some("Home: 5G"));
        assert_eq!(netsh(NETSH, Some("WLAN 2")).as_deref(), Some("Office"));
        // Two candidates and no way to tell: better no name than a wrong one.
        assert_eq!(netsh(NETSH, None), None);
    }

    #[test]
    fn netsh_with_one_connected_adapter_needs_no_display_name() {
        let one = "\n    Name                   : Wi-Fi\n    SSID                   : Home\n";
        assert_eq!(netsh(one, Some("Ethernet")).as_deref(), Some("Home"));
        // Location permission denied: no SSID at all.
        assert_eq!(
            netsh("Network shell commands need location permission.\n", None),
            None
        );
    }
}
