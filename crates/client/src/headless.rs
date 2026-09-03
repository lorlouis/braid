#![forbid(unsafe_code)]

//! `-N`: forwards and nothing else. No terminal, because `guarded_raw_mode`
//! fails on anything that is not one and this has to start from a unit file.

use crate::forward::{ForwardSpec, Forwards, Listeners};
use crate::inbound::{
    Deadline, DeadlineReader, Inbound, LINK_TIMEOUT, RESUME_TIMEOUT, next_forward_frame,
    silence_deadline,
};
use crate::log::log;
use crate::outbound::{ClientWriter, Link};
use crate::state::{CLIENT_ID, ReconnectState};
use crate::transport::{Auth, Outbox, SshTransport, TransportError};
use crate::{ClientError, Indicator, Reconnected, Reopen, Upgraded, reconnect, upgrade};
use braid_proto::{
    ByteOff, ClientMessage, CmdSeq, DatagramOffer, DetachReason, MAX_FRAME, RejectReason,
    ServerMessage, Version, VersionRange, read_frame, write_message,
};
use std::cell::{Cell, RefCell};
use std::io::{self, Read, Write};
use std::process::{ChildStdin, ChildStdout};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::thread;
use std::time::{Duration, Instant};

/// On a link that is already dead the read deadline leaves Ctrl-C looking hung.
const CLOSE_GRACE: Duration = Duration::from_millis(500);

/// Long enough for an accept thread to notice [`Forwards`]'s teardown.
const REBIND_GRACE: Duration = Duration::from_secs(2);
const REBIND_PAUSE: Duration = Duration::from_millis(20);

const SIGNAL_THREAD: &str = "brd-forward-signals";

/// Sealing onto the datagram path builds a deflate compressor on the stack, and
/// an overflow is a SIGSEGV no panic hook contains.
const SIGNAL_STACK: usize = 1024 * 1024;

const _: () = assert!(SIGNAL_STACK > braid_proto::wire::CODEC_STACK + 64 * 1024);

/// Carry `forwards` on `destination` until something asks this process to stop.
///
/// # Errors
///
/// A port that cannot be bound, an `ssh` that cannot start, or a refused session.
pub fn forward_only(destination: &str, forwards: &[ForwardSpec]) -> Result<(), ClientError> {
    // A port that did not come up is the whole of what this process is for.
    let mut listeners = Listeners::bind(forwards)?;
    let indicator = Quiet::new(io::stderr());
    let opened = open_forward(destination)?;
    let deadline = Deadline::new(LINK_TIMEOUT);
    // From the first command: nothing was resumed for this one to continue.
    let input = Arc::new(ClientWriter::new(
        Link::Ssh(Outbox::new(opened.input)),
        CmdSeq::first(),
        opened.version,
    ));
    signals(&input)?;
    let mut session = Session {
        state: opened.state,
        inbound: Inbound::ssh(opened.output, deadline.clone())?,
        transport: Some(opened.transport),
        version: opened.version,
        offers_wanted: true,
    };
    let mut offer = opened.offer;
    loop {
        // So a client that migrates never reconciles two transports mid-stream.
        match upgrade(offer, &session.state, &deadline, &input)? {
            Upgraded::Migrated(datagrams, spoken) => {
                session.inbound = datagrams;
                session.version = spoken;
                session.transport = None;
            }
            Upgraded::Stayed => {}
        }
        // Ports held since before the handshake, so an early connection waits
        // in the backlog rather than being refused.
        let serving = Forwards::start(listeners, &input)?;
        match serve(
            destination,
            &mut session,
            &deadline,
            &input,
            &serving,
            &indicator,
        )? {
            Ended::Done => return Ok(()),
            Ended::Gone => {}
        }
        eprintln!("[brd] session expired; opening another for the same forwards");
        // A fresh session answers a frame naming a stream it never opened with
        // silence, so kept forwards would retransmit into the void.
        drop(serving);
        drop(session.transport.take());
        listeners = rebind(forwards)?;
        let resumed = match reconnect(destination, &Reopen::Forwards, &input, &indicator)? {
            Reconnected::Link(resumed) => resumed,
            Reconnected::Closed => return Ok(()),
            // `HelloForward` asks for a session rather than naming one.
            Reconnected::Gone => return Err(ClientError::Remote(RejectReason::UnknownSession)),
        };
        // A replacement transport has measured nothing yet.
        deadline.set(LINK_TIMEOUT);
        session = Session {
            state: resumed.opened,
            inbound: Inbound::ssh(resumed.output, deadline.clone())?,
            transport: Some(resumed.transport),
            version: resumed.version,
            // A fresh session, so no resume of this client's has landed on it.
            offers_wanted: true,
        };
        input.reopen(Link::Ssh(Outbox::new(resumed.input)), resumed.version)?;
        offer = resumed.offer;
    }
}

struct Session {
    state: ReconnectState,
    inbound: Inbound,
    /// Every frame on `inbound` is read at this.
    version: Version,
    /// Dropping it reaps the `ssh` process; a datagram upgrade makes it `None`.
    transport: Option<SshTransport>,
    /// Cleared by a `Detached` naming one of this client's own resumes: that
    /// frame is proof the resume landed with nothing coming back, so a second
    /// offer would cost the attachment this one's relink is about to take.
    offers_wanted: bool,
}

enum Ended {
    Done,
    /// The `-L` arguments still describe something worth serving.
    Gone,
}

struct Opened {
    transport: SshTransport,
    input: ChildStdin,
    output: ChildStdout,
    state: ReconnectState,
    offer: Option<DatagramOffer>,
    version: Version,
}

/// `Auth::Interactive`, unlike every reopen after it: this runs from the command
/// the user just typed, and by the next one there may be no terminal at all.
fn open_forward(destination: &str) -> Result<Opened, ClientError> {
    let mut transport = SshTransport::connect(destination, Auth::Interactive)?;
    let (mut input, output) = transport
        .take_io()
        .ok_or(ClientError::Transport(TransportError::MissingPipe))?;
    // `ConnectTimeout` covers the SYN alone; an sshd that accepts and stalls
    // would park a process with no keyboard to interrupt it.
    let mut output = DeadlineReader::new(output, Deadline::new(RESUME_TIMEOUT))?;
    match forward_hello(&mut input, &mut output) {
        Ok((state, offer, version)) => Ok(Opened {
            transport,
            input,
            output: output.into_inner(),
            state,
            offer,
            version,
        }),
        // `ssh`'s diagnostics went to a pipe, not to this process's stderr.
        Err(ClientError::Protocol(error)) if error.is_transport_loss() => {
            let diagnostics = transport.diagnostics();
            Err(if diagnostics.is_empty() {
                ClientError::Protocol(error)
            } else {
                ClientError::Ssh(diagnostics)
            })
        }
        Err(error) => Err(error),
    }
}

fn forward_hello<I: Write, O: Read>(
    input: &mut I,
    output: &mut O,
) -> Result<(ReconnectState, Option<DatagramOffer>, Version), ClientError> {
    write_message(
        input,
        &ClientMessage::HelloForward {
            versions: VersionRange::LOCAL,
            client: *CLIENT_ID,
        }
        .encode(Version::LOCAL)?,
    )?;
    match ServerMessage::decode(&read_frame(output, MAX_FRAME)?, Version::LOCAL)? {
        ServerMessage::HelloForward {
            version,
            session_id,
            capability,
            offer,
        } => Ok((ReconnectState::new(session_id, capability), offer, version)),
        ServerMessage::Reject { reason } => Err(ClientError::Remote(reason)),
        // Including a `Hello`: it describes a grid nothing here would ever read.
        _ => Err(ClientError::Protocol(
            braid_proto::DecodeError::InvalidField,
        )),
    }
}

/// Answer frames until the session ends or the daemon forgets it.
#[expect(
    clippy::too_many_lines,
    reason = "one arm per message a forward-only session answers, in one match: a message with no arm is a compile error"
)]
fn serve(
    destination: &str,
    session: &mut Session,
    deadline: &Deadline,
    input: &Arc<ClientWriter<Link>>,
    forwards: &Forwards<Link>,
    indicator: &dyn Indicator,
) -> Result<Ended, ClientError> {
    // Otherwise a frame per forwarded chunk costs a fresh zeroed allocation.
    let mut payload = Vec::new();
    // One re-establishment path, however the link was lost: a read that failed,
    // or an attachment this client learned was already replaced. A `Detached`
    // can arrive on a datagram link, where there is no read to fail and waiting
    // for one costs the silence deadline.
    let mut relink = false;
    loop {
        if std::mem::take(&mut relink) {
            // Every forward now holds bytes against a sink that refuses
            // them, which is what the offset-keyed repair is for.
            input.disconnect()?;
            drop(session.transport.take());
            match reconnect(
                destination,
                &Reopen::Session(&session.state),
                input,
                indicator,
            )? {
                Reconnected::Link(resumed) => {
                    deadline.set(LINK_TIMEOUT);
                    session.inbound = Inbound::ssh(resumed.output, deadline.clone())?;
                    session.transport = Some(resumed.transport);
                    session.version = resumed.version;
                    input.reconnect(Link::Ssh(Outbox::new(resumed.input)), resumed.version)?;
                    let offer = resumed.offer.filter(|_| session.offers_wanted);
                    match upgrade(offer, &session.state, deadline, input)? {
                        Upgraded::Migrated(datagrams, spoken) => {
                            session.inbound = datagrams;
                            session.version = spoken;
                            session.transport = None;
                        }
                        Upgraded::Stayed => {}
                    }
                    // The pump thread cannot see this; without it a tunnel
                    // waits for its next local byte.
                    forwards.link_restored();
                    eprintln!("[brd] link restored");
                }
                Reconnected::Closed => return Ok(Ended::Done),
                Reconnected::Gone => return Ok(Ended::Gone),
            }
            continue;
        }
        let frame = match next_forward_frame(&mut session.inbound, &mut payload) {
            Ok(frame) => frame,
            Err(error) if error.is_transport_loss() => {
                // Being asked to stop is not a failure, whatever became of the
                // transport.
                if input.close_requested.load(Ordering::Acquire) {
                    return Ok(Ended::Done);
                }
                relink = true;
                continue;
            }
            Err(error) => return Err(ClientError::Protocol(error)),
        };
        match ServerMessage::decode(frame, session.version)? {
            ServerMessage::Ping {
                token, interval_ms, ..
            } => {
                // Wide open: this session produces no output to be behind on.
                input.pong(token, ByteOff::zero())?;
                // The server paces its probes, so only this number knows what
                // silence means on this link.
                deadline.set(silence_deadline(interval_ms));
            }
            ServerMessage::CommandAck { highest } => {
                // `ForwardOpen` is the one sequenced message this client sends.
                if let Some(highest) = highest {
                    input.acknowledge(highest)?;
                }
            }
            // Ignored rather than refused: the daemon's own reset settles a
            // stream that ended here.
            ServerMessage::ForwardData {
                stream,
                off,
                fin,
                bytes,
            } => forwards.on_data(stream, off, fin, &bytes),
            ServerMessage::ForwardAck {
                stream,
                off,
                window,
                held,
            } => forwards.on_ack(stream, off, window, &held),
            ServerMessage::ForwardReset { stream, .. } => forwards.on_reset(stream),
            // A forward-only session has no output stream to fall behind on, so
            // the daemon never sends this. Ignored rather than refused: losing a
            // working tunnel over a diagnostic would be the worse trade.
            ServerMessage::OutputSkipped { .. } => {}
            ServerMessage::Detached { reason } => match reason {
                DetachReason::Requested => {
                    drop(session.transport.take());
                    eprintln!("[brd] detached");
                    return Ok(Ended::Done);
                }
                // Never another client: the id a resume carries is drawn per
                // process, so this names an attachment one of this client's own
                // resumes replaced. The forwards outlive it, and ending here
                // strands them on a link the daemon has already dropped.
                DetachReason::Replaced => {
                    log!("attachment replaced by this client's own resume; relinking");
                    // Proof that the resume behind it arrived with nothing
                    // coming back, so a further offer would cost the
                    // attachment this relink is about to take.
                    session.offers_wanted = false;
                    relink = true;
                }
            },
            ServerMessage::Exit { code } => {
                // The daemon holds its half of the attachment socket open while
                // the reader thread parks, so waiting here would never return.
                drop(session.transport.take());
                return if input.close_requested.load(Ordering::Acquire) || code == 0 {
                    Ok(Ended::Done)
                } else {
                    Err(ClientError::RemoteExit(code))
                };
            }
            ServerMessage::Reject { reason } => {
                // A forgotten session is not the end of a client whose listeners
                // are still bound; every other terminal refusal is.
                if reason == RejectReason::UnknownSession {
                    return Ok(Ended::Gone);
                }
                if reason.is_terminal() {
                    return Err(ClientError::Remote(reason));
                }
            }
            // Nothing here has a terminal, and each of the rest belongs to a
            // connection of its own.
            ServerMessage::Output { .. }
            | ServerMessage::Screen { .. }
            | ServerMessage::Hello { .. }
            | ServerMessage::HelloForward { .. }
            | ServerMessage::SessionList { .. }
            | ServerMessage::SearchResults { .. }
            | ServerMessage::SessionNames { .. } => {
                return Err(ClientError::Protocol(
                    braid_proto::DecodeError::InvalidField,
                ));
            }
        }
    }
}

fn rebind(forwards: &[ForwardSpec]) -> Result<Listeners, ClientError> {
    let deadline = Instant::now() + REBIND_GRACE;
    loop {
        match Listeners::bind(forwards) {
            Ok(listeners) => return Ok(listeners),
            Err(error) if Instant::now() >= deadline => return Err(error),
            Err(_) => thread::sleep(REBIND_PAUSE),
        }
    }
}

/// No SIGWINCH, SIGTSTP or SIGCONT: there is no terminal here.
fn signals(input: &Arc<ClientWriter<Link>>) -> Result<(), ClientError> {
    let closing = Arc::clone(input);
    thread::Builder::new()
        .name(SIGNAL_THREAD.into())
        .stack_size(SIGNAL_STACK)
        .spawn(move || {
            let Ok(mut signals) = signal_hook::iterator::Signals::new([
                signal_hook::consts::SIGINT,
                signal_hook::consts::SIGTERM,
                signal_hook::consts::SIGHUP,
            ]) else {
                return;
            };
            if signals.forever().next().is_none() {
                return;
            }
            let _ = closing.close();
            // Leaving through `main` gives the ports back through the forwards'
            // own teardown; this bounds the wait for that tidier exit.
            thread::sleep(CLOSE_GRACE);
            std::process::exit(0);
        })
        .map_err(|_| io::Error::other("signal thread failed"))?;
    Ok(())
}

/// One line per state change, never one per [`crate::STATUS_TICK`]: this process
/// outlives suspends measured in hours.
struct Quiet<W> {
    out: RefCell<W>,
    said: Cell<Option<bool>>,
}

impl<W: Write> Quiet<W> {
    fn new(out: W) -> Self {
        Self {
            out: RefCell::new(out),
            said: Cell::new(None),
        }
    }
}

impl<W: Write> Indicator for Quiet<W> {
    fn waiting(&self, since: Duration, dropped: bool) -> Result<(), ClientError> {
        if self.said.get() == Some(dropped) {
            return Ok(());
        }
        self.said.set(Some(dropped));
        let lost = if dropped {
            ", and input was dropped"
        } else {
            ""
        };
        let seconds = since.as_secs();
        writeln!(
            self.out.borrow_mut(),
            "[brd] link down {seconds}s ago; reconnecting{lost}"
        )?;
        Ok(())
    }

    fn clear(&self) -> Result<(), ClientError> {
        // Silent: only the caller knows whether this is a recovery or an exit.
        // It restores the right to report the *next* disconnect.
        self.said.set(None);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use braid_proto::{CAPABILITY_BYTES, Capability, ForwardTarget, GridSize, SessionId};
    use std::net::{IpAddr, Ipv4Addr};
    use std::process::{Child, Stdio};

    /// `cat` for the reason [`crate::tests::relay`] gives: this client is typed
    /// by the descriptors `ssh` hands over.
    fn daemon() -> (Child, ChildStdin, ChildStdout) {
        let mut child = std::process::Command::new("cat")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .expect("cat stands in for the transport");
        let into = child.stdin.take().expect("the relay takes frames");
        let from = child.stdout.take().expect("the relay gives them back");
        (child, into, from)
    }

    fn capability(byte: u8) -> Capability {
        Capability::from_bytes([byte; CAPABILITY_BYTES])
    }

    /// One message out, one back, and no grid in either.
    #[test]
    fn a_forward_only_handshake_takes_the_session_the_daemon_names() {
        let (mut child, mut into, mut from) = daemon();
        into.write_all(
            &ServerMessage::HelloForward {
                version: Version::LOCAL,
                session_id: SessionId::from_bytes([5; 16]),
                capability: capability(6),
                offer: None,
            }
            .encode(Version::LOCAL)
            .expect("the answer encodes"),
        )
        .expect("the answer is carried");
        into.flush().expect("the answer is carried");

        let mut sent = Vec::new();
        let (state, offer, _) = forward_hello(&mut sent, &mut from).expect("the daemon answered");
        assert_eq!(state.session_id, SessionId::from_bytes([5; 16]));
        assert_eq!(state.capability, capability(6));
        assert!(offer.is_none());
        assert_eq!(
            ClientMessage::decode(
                &read_frame(&mut &sent[..], MAX_FRAME).expect("one frame"),
                Version::LOCAL
            )
            .expect("the request decodes"),
            ClientMessage::HelloForward {
                versions: VersionRange::LOCAL,
                client: *CLIENT_ID,
            },
            "the request names this process and nothing about a terminal"
        );

        let _ = child.kill();
        let _ = child.wait();
    }

    /// A `Hello` describes a grid nothing here would ever read.
    #[test]
    fn a_terminal_session_is_not_what_a_forward_only_handshake_accepts() {
        let (mut child, mut into, mut from) = daemon();
        for answer in [
            ServerMessage::Hello {
                version: Version::LOCAL,
                session_id: SessionId::from_bytes([1; 16]),
                capability: capability(2),
                size: GridSize { cols: 80, rows: 24 },
                offer: None,
            },
            ServerMessage::Reject {
                reason: RejectReason::TooManySessions,
            },
        ] {
            into.write_all(&answer.encode(Version::LOCAL).expect("the answer encodes"))
                .expect("the answer is carried");
        }
        into.flush().expect("the answers are carried");

        let Err(terminal) = forward_hello(&mut Vec::new(), &mut from) else {
            panic!("a session with a terminal in it was accepted");
        };
        assert!(
            matches!(terminal, ClientError::Protocol(_)),
            "a grid where none was asked for: {terminal}"
        );
        let Err(refused) = forward_hello(&mut Vec::new(), &mut from) else {
            panic!("a refusal was taken for a session");
        };
        assert!(
            matches!(refused, ClientError::Remote(RejectReason::TooManySessions)),
            "the daemon's own reason, not this client's guess: {refused}"
        );

        let _ = child.kill();
        let _ = child.wait();
    }

    /// A suspended laptop is hours of [`crate::STATUS_TICK`], and a line each
    /// would bury whatever the tunnel was started to serve.
    #[test]
    fn the_quiet_indicator_reports_a_state_change_and_not_a_tick() {
        let said = |quiet: &Quiet<Vec<u8>>| {
            String::from_utf8(quiet.out.borrow().clone()).expect("the lines are text")
        };
        let quiet = Quiet::new(Vec::new());
        for tick in 0..30 {
            quiet
                .waiting(Duration::from_secs(tick), false)
                .expect("the line is written");
        }
        let one_disconnect = said(&quiet);
        assert_eq!(
            one_disconnect.lines().count(),
            1,
            "thirty ticks of one disconnect: {one_disconnect}"
        );

        quiet.clear().expect("nothing to clear");
        quiet
            .waiting(Duration::from_secs(1), false)
            .expect("the line is written");
        assert_eq!(said(&quiet).lines().count(), 2);

        // The flag the line carries is part of the state it reports.
        quiet
            .waiting(Duration::from_secs(2), true)
            .expect("the line is written");
        let dropped = said(&quiet);
        assert_eq!(dropped.lines().count(), 3);
        assert!(
            dropped
                .lines()
                .next_back()
                .is_some_and(|line| line.contains("input was dropped")),
            "the last line says what changed: {dropped}"
        );
    }

    /// The `Pong` proves this client answers as an attachment rather than
    /// merely holding a socket open.
    #[test]
    fn a_forward_only_session_answers_a_probe_and_ends_on_exit() {
        let (inbound_child, mut daemon_says, from) = daemon();
        let (outbound_child, client_writes, mut client_said) = daemon();

        for message in [
            ServerMessage::Ping {
                token: 0x51,
                echo_ack: None,
                interval_ms: 500,
            },
            ServerMessage::Exit { code: 0 },
        ] {
            daemon_says
                .write_all(&message.encode(Version::LOCAL).expect("the frame encodes"))
                .expect("the frames are carried");
        }
        daemon_says.flush().expect("the frames are carried");

        let deadline = Deadline::new(Duration::from_secs(5));
        let input = Arc::new(ClientWriter::new(
            Link::Ssh(Outbox::new(client_writes)),
            CmdSeq::first(),
            Version::LOCAL,
        ));
        let listeners = Listeners::bind(&[ForwardSpec {
            bind: IpAddr::V4(Ipv4Addr::LOCALHOST),
            port: 0,
            target: ForwardTarget {
                host: "127.0.0.1".into(),
                port: 9,
            },
        }])
        .expect("a loopback port");
        let forwards = Forwards::start(listeners, &input).expect("the forwards start");
        let mut session = Session {
            version: Version::LOCAL,
            state: ReconnectState::new(SessionId::from_bytes([1; 16]), capability(2)),
            inbound: Inbound::ssh(from, deadline.clone()).expect("a pipe takes O_NONBLOCK"),
            transport: None,
            offers_wanted: true,
        };

        let ended = serve(
            "nowhere.invalid",
            &mut session,
            &deadline,
            &input,
            &forwards,
            &Quiet::new(Vec::new()),
        )
        .expect("the session ends on the daemon's own `Exit`");
        assert!(matches!(ended, Ended::Done));

        let answered = ClientMessage::decode(
            &read_frame(&mut client_said, MAX_FRAME).expect("the client answered"),
            Version::LOCAL,
        )
        .expect("the answer decodes");
        assert_eq!(
            answered,
            ClientMessage::Pong {
                token: 0x51,
                consumed: ByteOff::zero(),
            },
            "a session with no output has consumed the whole of it"
        );

        drop(forwards);
        for mut child in [inbound_child, outbound_child] {
            let _ = child.kill();
            let _ = child.wait();
        }
    }

    /// `Replaced` names an attachment one of this client's *own* resumes
    /// displaced - no other client can, because the id a resume carries is
    /// drawn per process. The session behind it is still running, so a client
    /// that ends here strands a user's forwards on a link the daemon already
    /// dropped: the frame is a reason to relink, not to stop.
    ///
    /// The close is requested up front so the relink settles on `reconnect`'s
    /// own first check, which spawns no `ssh` at a name that does not resolve.
    /// What proves the relink ran is the link epoch: tearing the writer down is
    /// its first act, and no early return reaches it.
    #[test]
    fn an_attachment_this_client_replaced_relinks_rather_than_ending() {
        let (inbound_child, mut daemon_says, from) = daemon();
        let (outbound_child, client_writes, _client_said) = daemon();

        let detached = ServerMessage::Detached {
            reason: DetachReason::Replaced,
        };
        daemon_says
            .write_all(&detached.encode(Version::LOCAL).expect("the frame encodes"))
            .expect("the frame is carried");
        daemon_says.flush().expect("the frame is carried");

        let deadline = Deadline::new(Duration::from_secs(5));
        let input = Arc::new(ClientWriter::new(
            Link::Ssh(Outbox::new(client_writes)),
            CmdSeq::first(),
            Version::LOCAL,
        ));
        input.close_requested.store(true, Ordering::Release);
        let epoch = input.epoch();
        let listeners = Listeners::bind(&[ForwardSpec {
            bind: IpAddr::V4(Ipv4Addr::LOCALHOST),
            port: 0,
            target: ForwardTarget {
                host: "127.0.0.1".into(),
                port: 9,
            },
        }])
        .expect("a loopback port");
        let forwards = Forwards::start(listeners, &input).expect("the forwards start");
        let mut session = Session {
            version: Version::LOCAL,
            state: ReconnectState::new(SessionId::from_bytes([1; 16]), capability(2)),
            inbound: Inbound::ssh(from, deadline.clone()).expect("a pipe takes O_NONBLOCK"),
            transport: None,
            offers_wanted: true,
        };

        let ended = serve(
            "nowhere.invalid",
            &mut session,
            &deadline,
            &input,
            &forwards,
            &Quiet::new(Vec::new()),
        )
        .expect("a replaced attachment is not a failure");
        assert!(
            matches!(ended, Ended::Done),
            "the relink ended on the close this client had already asked for"
        );
        assert_ne!(
            input.epoch(),
            epoch,
            "the session ended on the replaced attachment instead of relinking"
        );
        assert!(
            !session.offers_wanted,
            "a resume known to have landed must not be followed by another offer"
        );

        drop(forwards);
        for mut child in [inbound_child, outbound_child] {
            let _ = child.kill();
            let _ = child.wait();
        }
    }

    /// A resume that goes out unanswered proves nothing on its own: if it never
    /// reached the daemon, the `ssh` attachment was never touched and the next
    /// attachment's offer is worth exactly as much as this one's. Only the
    /// `Detached` that a resume which *did* land produces is proof, and that
    /// arrives on the link rather than out of this call — so nothing here may
    /// report a spent offer as anything the caller can key a downgrade off.
    ///
    /// Reading it as one is what pins a roaming client to `ssh` for the rest of
    /// the session because one datagram was dropped at attach time.
    #[test]
    fn a_resume_nothing_answers_is_not_a_reason_to_stop_offering() {
        let (mut child, into, _from) = daemon();
        // Bound and then dropped, which is what a firewalled datagram path looks like.
        let port = std::net::UdpSocket::bind((Ipv4Addr::LOCALHOST, 0))
            .and_then(|socket| socket.local_addr())
            .expect("a bound port")
            .port();
        let mut ip = [0; 16];
        ip[10] = 0xff;
        ip[11] = 0xff;
        ip[12..].copy_from_slice(&[127, 0, 0, 1]);
        let offer = DatagramOffer {
            ip,
            port,
            cid: [3; 8],
            secret: [4; 32],
        };

        let input = Arc::new(ClientWriter::new(
            Link::Ssh(Outbox::new(into)),
            CmdSeq::first(),
            Version::LOCAL,
        ));
        let state = ReconnectState::new(SessionId::from_bytes([1; 16]), capability(2));
        let upgraded = upgrade(Some(offer), &state, &Deadline::new(LINK_TIMEOUT), &input)
            .expect("an offer nothing answers is not a failure");
        assert!(
            matches!(upgraded, Upgraded::Stayed),
            "an unanswered resume left the caller something to downgrade on"
        );

        let _ = child.kill();
        let _ = child.wait();
    }
}
