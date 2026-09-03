use braid_client::Attach;
use braid_client::forward::ForwardSpec;
use braid_client::predict::Prediction;
use std::env;
use std::ffi::{OsStr, OsString};
use std::process::ExitCode;

const USAGE: &str = "usage: brd [--predict=MODE] [-L SPEC] <destination> [-- command ...]
       brd -N -L SPEC [-L SPEC ...] <destination>
       brd new [--predict=MODE] [-L SPEC] <destination> [-- command ...]
       brd attach <destination> <id>
       brd ls <destination>
       brd kill <destination> <id>
       brd rename <destination> <id> <name>
       brd grep <destination> <pattern>
       brd --help | --version

`brd <destination>` resumes the newest session on that host, or starts one.
`brd new` starts one beside whatever is already running there instead.
`brd attach` names an older session by any unambiguous prefix of its id.
`brd rename` labels a session for `brd ls` to print; the label belongs to the session
rather than to this machine, so every client on that host sees it, and an empty name
clears it.
`brd grep` searches every session's scrollback on that host, where it lives.
`--predict` is never, adaptive or always and defaults to adaptive: a keystroke
is drawn before the session echoes it only on a link slow enough for the round
trip to be worth hiding. `BRD_PREDICT` names the same three.
`-L [bind_address:]port:host:hostport` forwards a local port, repeatable, in
ssh's own syntax and binding loopback unless the spec names an address. The
forward is carried by the session, so it survives the reconnects that end an
`ssh -L` tunnel outright.
`-N` carries the forwards and nothing else: no terminal, no shell, and no
controlling terminal to need one, so it backgrounds with `&` and runs under a
unit file. It requires a `-L` and refuses a command.
In a session, Ctrl-] r repaints, Ctrl-] z suspends, Ctrl-] d detaches and
Ctrl-] . quits.
`BRD_LOG` names a file this client appends a line to for each reconnect,
transport change and detach, which is what a bug report needs and a terminal
cannot carry. Unset, nothing is opened and nothing is written.
Transport options belong in ssh_config: brd runs your `ssh`, and your
`~/.ssh/config` is already the complete answer for ports, jump hosts and keys.";

/// Kept apart from 1 so a script can tell misuse from a session that failed.
const MISUSE: u8 = 2;

fn main() -> ExitCode {
    let mut args = env::args_os();
    let _program = args.next();
    let (options, first) = match options(&mut args) {
        Ok(parsed) => parsed,
        Err(code) => return code,
    };
    let prediction = match options.chosen {
        Some(mode) => mode,
        None => match predict_env() {
            Ok(mode) => mode.unwrap_or_default(),
            Err(code) => return code,
        },
    };
    let Some(first) = first else {
        return usage();
    };
    if let Some(refusal) = misapplied(&first, &options) {
        eprintln!("brd: {refusal}");
        return ExitCode::from(MISUSE);
    }
    let Options {
        forwards,
        headless,
        fresh,
        ..
    } = options;
    match first.to_str() {
        Some("--help" | "-h") => {
            println!("{USAGE}");
            ExitCode::SUCCESS
        }
        Some("--version" | "-V") => {
            println!("brd {}", env!("CARGO_PKG_VERSION"));
            ExitCode::SUCCESS
        }
        // Both are invoked over `ssh` by this binary itself, never by a user.
        Some("--server") => report("brd server", braid_server::run_server()),
        Some("--daemon") => report("brd daemon", braid_server::run_daemon()),
        Some("ls") => match destination_and(&mut args, []) {
            Ok((destination, [])) => report("brd", braid_client::manage::list(&destination)),
            Err(code) => code,
        },
        Some("kill") => match destination_and(&mut args, ["session id"]) {
            Ok((destination, [id])) => report("brd", braid_client::manage::kill(&destination, &id)),
            Err(code) => code,
        },
        Some("rename") => match destination_and(&mut args, ["session id", "name"]) {
            Ok((destination, [id, name])) => report(
                "brd",
                braid_client::manage::rename(&destination, &id, &name),
            ),
            Err(code) => code,
        },
        Some("grep") => match destination_and(&mut args, ["pattern"]) {
            Ok((destination, [pattern])) => {
                report("brd", braid_client::manage::grep(&destination, &pattern))
            }
            Err(code) => code,
        },
        Some("attach") => match destination_and(&mut args, ["session id"]) {
            Ok((destination, [id])) => report(
                "brd",
                braid_client::run(
                    &destination,
                    &[],
                    Attach::Prefix(&id),
                    prediction,
                    &forwards,
                ),
            ),
            Err(code) => code,
        },
        _ => match (destination(Some(first)), command(args)) {
            (Ok(destination), Ok(command)) => {
                if headless {
                    if let Some(refusal) = headless_refusal(&command, &forwards) {
                        eprintln!("brd: {refusal}");
                        return ExitCode::from(MISUSE);
                    }
                    return report("brd", braid_client::forward_only(&destination, &forwards));
                }
                report(
                    "brd",
                    braid_client::run(
                        &destination,
                        &command,
                        if fresh { Attach::Fresh } else { Attach::Newest },
                        prediction,
                        &forwards,
                    ),
                )
            }
            (Err(code), _) | (_, Err(code)) => code,
        },
    }
}

#[derive(Default)]
struct Options {
    chosen: Option<Prediction>,
    forwards: Vec<ForwardSpec>,
    headless: bool,
    /// `new`. A word rather than a flag, but it selects what to attach to and stands
    /// where an option stands, so it reads the same on either side of them.
    fresh: bool,
}

/// The options, and the first word that is not one. `new` is among them: it selects what
/// to attach to rather than where, and a word that stands where an option stands reads
/// the same on either side of the rest. Nothing after the destination is examined: `--`
/// already separates the command.
fn options<A: Iterator<Item = OsString>>(
    args: &mut A,
) -> Result<(Options, Option<OsString>), ExitCode> {
    let mut options = Options::default();
    let mut first = args.next();
    while let Some(argument) = first.as_ref().and_then(|argument| argument.to_str()) {
        if let Some(name) = argument.strip_prefix("--predict=") {
            let Some(mode) = Prediction::parse(name) else {
                eprintln!("brd: --predict takes never, adaptive or always");
                return Err(ExitCode::from(MISUSE));
            };
            options.chosen = Some(mode);
            first = args.next();
            continue;
        }
        if let Some(glued) = argument.strip_prefix("-L") {
            // Glued or apart, because `ssh` takes both.
            let spec = if glued.is_empty() {
                let Some(spec) = text(args.next(), "-L")? else {
                    eprintln!("brd: -L takes [bind_address:]port:host:hostport");
                    return Err(ExitCode::from(MISUSE));
                };
                spec
            } else {
                glued.to_owned()
            };
            match ForwardSpec::parse(&spec) {
                Ok(forward) => options.forwards.push(forward),
                Err(error) => {
                    eprintln!("brd: -L {spec}: {error}");
                    return Err(ExitCode::from(MISUSE));
                }
            }
            first = args.next();
            continue;
        }
        if argument == "-N" {
            options.headless = true;
            first = args.next();
            continue;
        }
        if argument == "new" {
            options.fresh = true;
            first = args.next();
            continue;
        }
        break;
    }
    Ok((options, first))
}

/// The option that does not apply to the subcommand named, if this is such a pair.
fn misapplied(first: &OsStr, options: &Options) -> Option<String> {
    // `-N` already starts a session of its own every time: there is nothing for it to
    // resume and nothing for `new` to add.
    if options.headless && options.fresh {
        return Some("-N does not apply to `new`".to_owned());
    }
    let word = first.to_str()?;
    // These four open a management connection carrying no session for a
    // forward to ride on.
    if !options.forwards.is_empty() && matches!(word, "ls" | "kill" | "grep" | "rename") {
        return Some(format!("-L does not apply to `{word}`"));
    }
    if options.headless && matches!(word, "ls" | "kill" | "grep" | "rename" | "attach") {
        return Some(format!("-N does not apply to `{word}`"));
    }
    // `new` starts a session; every word here reaches one that is already running.
    if options.fresh && matches!(word, "ls" | "kill" | "grep" | "rename" | "attach") {
        return Some(format!("`new` does not apply to `{word}`"));
    }
    None
}

fn headless_refusal(command: &[String], forwards: &[ForwardSpec]) -> Option<&'static str> {
    if !command.is_empty() {
        return Some("-N carries forwards and nothing else: it runs no command");
    }
    if forwards.is_empty() {
        return Some("-N needs at least one -L: without one it forwards nothing");
    }
    None
}

/// An unrecognised value is refused rather than ignored, so an exported
/// `BRD_PREDICT=off` does not silently do nothing.
fn predict_env() -> Result<Option<Prediction>, ExitCode> {
    let Some(value) = env::var_os("BRD_PREDICT") else {
        return Ok(None);
    };
    let Some(mode) = value.to_str().and_then(Prediction::parse) else {
        eprintln!("brd: BRD_PREDICT takes never, adaptive or always");
        return Err(ExitCode::from(MISUSE));
    };
    Ok(Some(mode))
}

/// `ssh` reads a leading `-` as an option, and `-oProxyCommand=...` is a
/// command it runs locally.
fn destination(argument: Option<OsString>) -> Result<String, ExitCode> {
    let Some(destination) = text(argument, "destination")? else {
        return Err(usage());
    };
    if destination.starts_with('-') {
        eprintln!("brd: destination must not begin with '-'");
        return Err(ExitCode::from(MISUSE));
    }
    Ok(destination)
}

/// A destination and the `N` words after it, each named by what it is for. Every word is
/// examined even when an earlier one is wrong, so a command line with two mistakes says
/// so about both, and only one usage is printed however many are missing.
fn destination_and<const N: usize>(
    rest: &mut impl Iterator<Item = OsString>,
    what: [&str; N],
) -> Result<(String, [String; N]), ExitCode> {
    let destination = destination(rest.next());
    let words = what.map(|what| text(rest.next(), what));
    // A name that needed quoting arrives as two words, and renaming a session to the
    // first half of what was typed is worse than refusing the line.
    let extra = rest.next().is_some();
    let mut refusal = destination.as_ref().err().copied();
    let mut given = [const { String::new() }; N];
    let mut short = false;
    for (slot, word) in given.iter_mut().zip(words) {
        match word {
            Ok(Some(word)) => *slot = word,
            Ok(None) => short = true,
            Err(code) => refusal = refusal.or(Some(code)),
        }
    }
    if let Some(code) = refusal {
        return Err(code);
    }
    if short {
        return Err(usage());
    }
    if extra {
        let last = what.last().copied().unwrap_or("destination");
        eprintln!("brd: unexpected argument after the {last}");
        return Err(ExitCode::from(MISUSE));
    }
    Ok((destination?, given))
}

/// argv to run instead of the login shell. A bare word before the `--` is a
/// typo, and taking it as part of the command would run it.
fn command(mut rest: impl Iterator<Item = OsString>) -> Result<Vec<String>, ExitCode> {
    let Some(separator) = rest.next() else {
        return Ok(Vec::new());
    };
    if separator != "--" {
        eprintln!("brd: unexpected argument; a command must follow '--'");
        return Err(ExitCode::from(MISUSE));
    }
    let mut command = Vec::new();
    for word in rest {
        let Some(word) = word.to_str() else {
            eprintln!("brd: command is not valid UTF-8");
            return Err(ExitCode::from(MISUSE));
        };
        command.push(word.to_owned());
    }
    if command.is_empty() {
        eprintln!("brd: '--' must be followed by a command");
        return Err(ExitCode::from(MISUSE));
    }
    Ok(command)
}

fn text(argument: Option<OsString>, what: &str) -> Result<Option<String>, ExitCode> {
    argument
        .map(|argument| {
            argument.into_string().map_err(|_| {
                eprintln!("brd: {what} is not valid UTF-8");
                ExitCode::from(MISUSE)
            })
        })
        .transpose()
}

fn report<E: std::fmt::Display>(role: &str, outcome: Result<(), E>) -> ExitCode {
    match outcome {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{role}: {error}");
            ExitCode::from(1)
        }
    }
}

fn usage() -> ExitCode {
    eprintln!("{USAGE}");
    ExitCode::from(MISUSE)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_headless_session_refuses_what_it_cannot_carry() {
        let forwards = [ForwardSpec::parse("8080:localhost:80").expect("a well-formed spec")];
        let command = ["top".to_owned()];
        let cases: [(&[String], &[ForwardSpec], Option<&str>); 3] = [
            (&[], &[], Some("-L")),
            (&command, &forwards, Some("no command")),
            (&[], &forwards, None),
        ];
        for (command, forwards, expected) in cases {
            let refusal = headless_refusal(command, forwards);
            match expected {
                Some(named) => assert!(
                    refusal.is_some_and(|refusal| refusal.contains(named)),
                    "{refusal:?} does not name {named}"
                ),
                None => assert!(refusal.is_none(), "refused a forward it can carry"),
            }
        }
    }

    fn words(line: &[&str]) -> std::vec::IntoIter<OsString> {
        line.iter()
            .map(OsString::from)
            .collect::<Vec<_>>()
            .into_iter()
    }

    /// The usage line advertises `brd new --predict=never host`, and every other
    /// subcommand takes its options before the word.
    #[test]
    fn new_reads_on_either_side_of_the_options_it_carries() {
        for line in [
            ["new", "--predict=never", "-L", "8080:localhost:80", "host"],
            ["--predict=never", "-L", "8080:localhost:80", "new", "host"],
        ] {
            let mut args = words(&line);
            let (parsed, first) = options(&mut args).expect("a well-formed command line");
            assert!(
                parsed.fresh,
                "{line:?} did not ask for a session of its own"
            );
            assert_eq!(parsed.chosen, Some(Prediction::Never), "{line:?}");
            assert_eq!(parsed.forwards.len(), 1, "{line:?}");
            assert_eq!(first.as_deref(), Some(OsStr::new("host")), "{line:?}");
        }
    }

    /// An unquoted two-word name arrives as two words, and renaming a session to the
    /// first of them is a remote change nobody typed.
    #[test]
    fn a_word_past_the_last_one_asked_for_is_refused() {
        let mut exact = words(&["host", "3f9c", "deploy"]);
        let (destination, [id, name]) =
            destination_and(&mut exact, ["session id", "name"]).expect("three words for three");
        assert_eq!(
            (destination.as_str(), id.as_str(), name.as_str()),
            ("host", "3f9c", "deploy")
        );
        let mut extra = words(&["host", "3f9c", "my", "deploy"]);
        assert!(
            destination_and(&mut extra, ["session id", "name"]).is_err(),
            "a fourth word renamed the session to `my`"
        );
        let mut listed = words(&["host", "3f9c"]);
        assert!(
            destination_and(&mut listed, []).is_err(),
            "`brd ls` takes a destination and nothing else"
        );
    }

    /// A forward rides on a session, `-N` is already a session of its own, and `new`
    /// starts one where the management words reach one that is already running.
    #[test]
    fn an_option_that_cannot_apply_to_a_subcommand_names_both() {
        let forwarded = |on: bool| {
            on.then(|| ForwardSpec::parse("8080:localhost:80").expect("a well-formed spec"))
                .into_iter()
                .collect::<Vec<_>>()
        };
        let cases: [(&str, bool, bool, bool, Option<&str>); 7] = [
            ("rename", true, false, false, Some("-L")),
            ("ls", true, false, false, Some("-L")),
            ("rename", false, true, false, Some("-N")),
            ("ls", false, false, true, Some("new")),
            ("attach", false, false, true, Some("new")),
            ("host", false, true, true, Some("-N")),
            ("host", true, false, true, None),
        ];
        for (word, forwards, headless, fresh, expected) in cases {
            let options = Options {
                chosen: None,
                forwards: forwarded(forwards),
                headless,
                fresh,
            };
            let refusal = misapplied(OsStr::new(word), &options);
            match expected {
                Some(option) => assert!(
                    refusal
                        .as_deref()
                        .is_some_and(|refusal| refusal.contains(option)),
                    "{refusal:?} does not name {option} for `{word}`"
                ),
                None => assert!(refusal.is_none(), "`{word}` refused {refusal:?}"),
            }
        }
    }
}
