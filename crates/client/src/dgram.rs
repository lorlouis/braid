#![deny(unsafe_code)]

//! The datagram half of a session, once the offer has been taken up. One
//! `braid` frame per datagram and nothing fragments: both ends size messages
//! to [`MIN_DATAGRAM_FRAME`], and [`pack`] swaps the stream's length prefix
//! for a codec tag. Reliability belongs to the journal above, never here.

use crate::inbound::Deadline;
use crate::log::log;
use crate::outbound::{Carriage, FrameSink};
use crate::terminal::{Stake, staked};
use braid_dgram::packet::{HEADER_BYTES, TAG_BYTES};
use braid_dgram::{
    BASE_DATAGRAM, ConnectionId, Endpoint, Fragmentation, MAX_DATAGRAM, MAX_PAYLOAD, Received,
    RootSecret, SendError,
};
use braid_proto::wire::{PACK_TAG, pack, unpack};
use braid_proto::{DatagramOffer, DecodeError, MIN_DATAGRAM_FRAME};
use rustix::net::RecvFlags;
use std::io;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, UdpSocket};
use std::ops::Range;
use std::sync::{Arc, Condvar, Mutex, PoisonError};
use std::thread;
use std::time::{Duration, Instant};

/// Stream framing only: it comes off before packing, and the builders one
/// layer up still count it.
const LENGTH_PREFIX: usize = 4;

/// A frame at the protocol's datagram bound must still be one datagram on the
/// path every IPv6 network is obliged to carry.
const _: () = assert!(
    MIN_DATAGRAM_FRAME - LENGTH_PREFIX + PACK_TAG <= BASE_DATAGRAM - HEADER_BYTES - TAG_BYTES
);

/// A ceiling, not a period: the thread sleeps on [`Endpoint::poll_deadline`].
/// The ceiling exists because path challenges and keep-alives leave only
/// through [`Endpoint::poll_transmit`], so without it a client that changed
/// networks and then stopped typing is never found again.
const TRANSMIT_TICK: Duration = Duration::from_millis(250);

/// A deadline the window refuses to clear — a probe the pacer will not admit
/// names one and [`Endpoint::poll_transmit`] then emits nothing — is one
/// already past on the next pass, so without a floor the thread spins on it.
/// [`Clock::advance`] still reaches under this: it names the instant directly.
const TICK_FLOOR: Duration = Duration::from_millis(1);

/// The session ends the moment a `Close` or `Detach` is handed over, so one
/// still waiting on a congestion window is a close never transmitted. Failing
/// puts the session back on `ssh`, which does deliver it.
const RESERVED_WAIT: Duration = Duration::from_millis(50);

/// [`Endpoint::ready`] answers `None` when it is the window rather than the
/// pacer refusing, and a window opens on an ack this side cannot wait on.
const RESERVED_RETRY: Duration = Duration::from_millis(5);

pub(crate) struct DatagramLink {
    socket: Arc<UdpSocket>,
    shared: Arc<Mutex<Endpoint>>,
}

/// Comfortably above the server's outstanding-byte window: a client can sit
/// inside one blocking terminal `write` for seconds while everything the
/// daemon sends lands here, and a smaller buffer is one the kernel silently
/// overruns — losing `Detached`, `Exit` and `Reject`, which nothing repairs.
const RECEIVE_BUFFER: usize = 4 * 1024 * 1024;

/// Best effort: the transport is correct with the default buffer, merely lossier.
fn widen_receive_buffer(socket: &UdpSocket) {
    let _ = rustix::net::sockopt::set_socket_recv_buffer_size(socket, RECEIVE_BUFFER);
}

/// Either family left fragmenting is enough: this socket reaches one peer.
#[cfg(any(target_os = "linux", target_vendor = "apple"))]
fn fragmenting() -> Fragmentation {
    log!(
        "this kernel would not stop fragmenting datagrams: this session stays at \
         {BASE_DATAGRAM} bytes rather than settle above it on a path that only carries the \
         probe in pieces"
    );
    Fragmentation::Permitted
}

/// Make the MTU search's answers mean something, by refusing to fragment.
///
/// Otherwise the kernel answers for the path: the 4000- and 8900-byte probes
/// are fragmented, all fragments arrive, and the connection settles on a width
/// the path never carried. An 8900-byte datagram is seven fragments on a
/// 1500-byte path, so one percent packet loss costs 6.8% of those datagrams.
/// Reported rather than silently best-effort: the search reads the answer.
#[cfg(target_os = "linux")]
fn refuse_fragmentation(socket: &UdpSocket) -> Fragmentation {
    use rustix::net::sockopt::{Ipv4PathMtuDiscovery, Ipv6PathMtuDiscovery};
    // The family the socket really has rather than the one the offer named:
    // `IPV6_MTU_DISCOVER` is `ENOPROTOOPT` on a v4 socket, and this one
    // reaches exactly one peer.
    let set = if socket.local_addr().is_ok_and(|local| local.is_ipv6()) {
        rustix::net::sockopt::set_ipv6_mtu_discover(socket, Ipv6PathMtuDiscovery::DO)
    } else {
        rustix::net::sockopt::set_ip_mtu_discover(socket, Ipv4PathMtuDiscovery::DO)
    };
    if set.is_ok() {
        return Fragmentation::Refused;
    }
    fragmenting()
}

/// The same on Darwin, where rustix exposes neither option. An oversize
/// `sendto` then fails with `EMSGSIZE`, which [`path_refused`] already reads as
/// a path that narrowed rather than as a socket that broke.
#[cfg(target_vendor = "apple")]
#[allow(
    unsafe_code,
    reason = "IP_DONTFRAG and IPV6_DONTFRAG have no safe binding"
)]
fn refuse_fragmentation(socket: &UdpSocket) -> Fragmentation {
    use std::os::fd::AsRawFd;

    /// Darwin's `netinet6/in6.h`; `libc` declares the v4 option only.
    const IPV6_DONTFRAG: libc::c_int = 62;
    /// Both options take one `c_int`, which is the length `setsockopt` is told.
    const _: () = assert!(size_of::<libc::c_int>() == 4);

    let (level, option) = if socket.local_addr().is_ok_and(|local| local.is_ipv6()) {
        (libc::IPPROTO_IPV6, IPV6_DONTFRAG)
    } else {
        (libc::IPPROTO_IP, libc::IP_DONTFRAG)
    };
    let on: libc::c_int = 1;
    // SAFETY: `socket` owns the descriptor for the whole call, and `on` is
    // live, correctly typed, and named at exactly its own length.
    let set = unsafe {
        libc::setsockopt(
            socket.as_raw_fd(),
            level,
            option,
            std::ptr::from_ref(&on).cast(),
            4,
        )
    };
    if set == 0 {
        return Fragmentation::Refused;
    }
    fragmenting()
}

/// rustix exposes neither `IP_DONTFRAG` nor `IPV6_DONTFRAG`.
#[cfg(not(any(target_os = "linux", target_vendor = "apple")))]
fn refuse_fragmentation(_: &UdpSocket) -> Fragmentation {
    log!(
        "datagram sockets here cannot be told to stop fragmenting, so this session stays at \
         {BASE_DATAGRAM} bytes rather than settle above it on a path that only carries the \
         probe in pieces"
    );
    Fragmentation::Permitted
}

/// A datagram above the cached path MTU returns `EMSGSIZE` on this
/// no-fragment socket. That is a loss, not a broken link: the packet number is
/// spent, the loss detector reports it, and the MTU search narrows the path.
fn path_refused(error: &io::Error) -> bool {
    error.raw_os_error() == Some(rustix::io::Errno::MSGSIZE.raw_os_error())
}

/// Tell the connection what the kernel says this path carries, rather than let
/// the search spend three losses and two spaced probes — seconds of output down
/// a black hole — rediscovering the width the ICMP behind this refusal already
/// named. Only this side's own `sendto` reaches here and no arriving datagram
/// does, so unlike a loss a forged packet cannot drive it.
#[cfg(target_os = "linux")]
fn narrowed(shared: &Mutex<Endpoint>, to: SocketAddr, now: Instant) {
    let Some(carries) = path_mtu(to) else {
        return;
    };
    if let Ok(mut endpoint) = shared.lock() {
        endpoint.path_refused(carries, now);
    }
}

/// What the route says a datagram to this peer may carry. The number is the
/// route's rather than the socket's, and `IP_MTU` on the deliberately
/// unconnected socket this link sends from is `ENOTCONN`, so a throwaway
/// connected socket is what reads the very value the refused `sendto` was
/// measured against: four syscalls, on a path taken once per narrowing.
#[cfg(target_os = "linux")]
fn path_mtu(to: SocketAddr) -> Option<usize> {
    /// What the kernel's MTU counts and a datagram's own width does not.
    const V4_HEADERS: usize = 20 + 8;
    const V6_HEADERS: usize = 40 + 8;

    let probe = match to {
        SocketAddr::V4(_) => UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0)),
        SocketAddr::V6(_) => UdpSocket::bind((Ipv6Addr::UNSPECIFIED, 0)),
    }
    .ok()?;
    probe.connect(to).ok()?;
    let mtu = match to {
        SocketAddr::V4(_) => rustix::net::sockopt::ip_mtu(&probe),
        SocketAddr::V6(_) => rustix::net::sockopt::ipv6_mtu(&probe),
    }
    .ok()?;
    // The headers a v4-mapped peer's datagrams really leave with, not the ones
    // the family of the socket they were handed to would suggest.
    let headers = match to.ip() {
        IpAddr::V4(_) => V4_HEADERS,
        IpAddr::V6(v6) if v6.to_ipv4_mapped().is_some() => V4_HEADERS,
        IpAddr::V6(_) => V6_HEADERS,
    };
    usize::try_from(mtu).ok()?.checked_sub(headers)
}

/// The loss-driven search is the only route here: `IP_DONTFRAG` refuses an
/// oversize datagram without naming a width, and no route MTU can be read back.
#[cfg(not(target_os = "linux"))]
fn narrowed(_: &Mutex<Endpoint>, _: SocketAddr, _: Instant) {}

/// When the tick thread must next be awake. Every path that creates a sooner
/// deadline calls [`Clock::advance`] rather than waiting for the tick to land:
/// an acknowledgement owed in 25 ms and held for the tick's quarter second runs
/// the far side's probe timer out and counts this client silent.
#[derive(Default)]
struct Clock {
    next: Mutex<Option<Instant>>,
    wake: Condvar,
}

impl Clock {
    /// Forget whatever the last pass named, so a deadline already past cannot
    /// carry into the next sleep. Called before the deadline is recomputed, and
    /// only by the thread that sleeps: everything after it is an `advance`.
    fn rearm(&self) {
        *self.next.lock().unwrap_or_else(PoisonError::into_inner) = None;
    }

    /// Sleep until `deadline`, or until something names an earlier one.
    ///
    /// Merged rather than assigned: the sleeper computes its deadline with the
    /// endpoint lock released, and an [`advance`](Self::advance) landing in that
    /// window would otherwise be overwritten by the deadline it raced — which is
    /// exactly the 25 ms acknowledgement this clock exists to deliver.
    fn sleep_until(&self, deadline: Instant) {
        let mut next = self.next.lock().unwrap_or_else(PoisonError::into_inner);
        *next = Some(next.map_or(deadline, |named| named.min(deadline)));
        loop {
            let Some(target) = *next else { return };
            let Some(left) = target.checked_duration_since(Instant::now()) else {
                return;
            };
            let (guard, _) = self
                .wake
                .wait_timeout(next, left)
                .unwrap_or_else(PoisonError::into_inner);
            next = guard;
        }
    }

    /// Bring the next wake forward, if `deadline` is sooner than it.
    fn advance(&self, deadline: Instant) {
        let mut next = self.next.lock().unwrap_or_else(PoisonError::into_inner);
        if next.is_none_or(|target| deadline < target) {
            *next = Some(deadline);
            self.wake.notify_one();
        }
    }
}

impl DatagramLink {
    /// A zero address is what the relay under `sshd` sends when it could not
    /// tell which of its host's addresses the client reached.
    pub(crate) fn open(offer: &DatagramOffer) -> Option<Self> {
        let peer = peer_address(offer)?;
        // The offer's own family: a v4 peer reached from a v6 socket needs
        // dual-stack behaviour no platform is obliged to give.
        let local: SocketAddr = if peer.is_ipv4() {
            (Ipv4Addr::UNSPECIFIED, 0).into()
        } else {
            (Ipv6Addr::UNSPECIFIED, 0).into()
        };
        let socket = UdpSocket::bind(local).ok()?;
        // Deliberately never connected. On a wildcard-bound UDP socket
        // `connect` pins the *local* address too, so a client that changes
        // network is sending from an address that no longer exists: measured,
        // every later `send`/`send_to`/`connect` returns `ENETUNREACH` for the
        // rest of the session. It also filters out the daemon's replies, which
        // come from whichever of its host's addresses the route picks.
        widen_receive_buffer(&socket);
        let fragmentation = refuse_fragmentation(&socket);
        let endpoint = Endpoint::connect(
            ConnectionId::from_bytes(offer.cid),
            RootSecret::new(offer.secret),
            peer,
            fragmentation,
        );
        Some(Self {
            socket: Arc::new(socket),
            shared: Arc::new(Mutex::new(endpoint)),
        })
    }

    /// `None` when the tick thread will not start: without it a connection
    /// works while typed into and then loses the peer on the first network
    /// change. `ssh` is still there and needs no thread of its own.
    pub(crate) fn split(self, timeout: Deadline) -> Option<(DatagramSink, DatagramReader)> {
        let Self { socket, shared } = self;
        let clock = Arc::new(Clock::default());
        // Weak on purpose: the tick stops when both halves are gone, with no
        // flag to set.
        let ticking_socket = Arc::downgrade(&socket);
        let ticking_shared = Arc::downgrade(&shared);
        let ticking_clock = Arc::clone(&clock);
        thread::Builder::new()
            .name("brd-dgram".into())
            .spawn(staked(Stake::Session, move || {
                let mut sealed = Vec::with_capacity(BASE_DATAGRAM);
                loop {
                    let deadline = {
                        let (Some(socket), Some(shared)) =
                            (ticking_socket.upgrade(), ticking_shared.upgrade())
                        else {
                            return;
                        };
                        // Before anything can name a new one, and after the last
                        // sleep consumed the old one.
                        ticking_clock.rearm();
                        transmit(&socket, &shared, &mut sealed);
                        let owed = shared
                            .lock()
                            .ok()
                            .and_then(|endpoint| endpoint.poll_deadline());
                        // Both halves are dropped before the sleep, or this
                        // thread keeps the socket open past the session.
                        let now = Instant::now();
                        owed.map_or(now + TRANSMIT_TICK, |at| {
                            at.clamp(now + TICK_FLOOR, now + TRANSMIT_TICK)
                        })
                    };
                    ticking_clock.sleep_until(deadline);
                }
            }))
            .ok()?;
        Some((
            DatagramSink {
                socket: Arc::clone(&socket),
                shared: Arc::clone(&shared),
                clock: Arc::clone(&clock),
                outbound: Mutex::default(),
            },
            DatagramReader {
                socket,
                shared,
                clock,
                timeout,
                armed: None,
                datagram: vec![0; MAX_DATAGRAM],
                sealed: Vec::with_capacity(BASE_DATAGRAM),
            },
        ))
    }
}

fn peer_address(offer: &DatagramOffer) -> Option<SocketAddr> {
    if offer.ip == [0; 16] || offer.port == 0 {
        return None;
    }
    let ip = Ipv6Addr::from(offer.ip);
    // IPv4 travels mapped, and a v4 peer needs a v4 socket to reach it.
    let ip = ip.to_ipv4_mapped().map_or(IpAddr::V6(ip), IpAddr::V4);
    Some(SocketAddr::new(ip, offer.port))
}

/// One clock for everything this call produces: two datagrams a tick apart are
/// two round-trip samples that disagree. The lock is released before the
/// syscall — it is the one lock a keystroke waits on.
fn transmit(socket: &UdpSocket, shared: &Mutex<Endpoint>, sealed: &mut Vec<u8>) {
    let now = Instant::now();
    loop {
        let owed = {
            let Ok(mut endpoint) = shared.lock() else {
                return;
            };
            sealed.clear();
            endpoint.poll_transmit(now, sealed)
        };
        let Some((to, len)) = owed else { return };
        let Some(datagram) = sealed.get(..len) else {
            return;
        };
        // A probe the kernel refused is otherwise indistinguishable from one
        // the wire lost, and reading it as loss costs the search a candidate
        // chance and the minute before that width is offered again.
        if let Err(error) = socket.send_to(datagram, to)
            && len > BASE_DATAGRAM
            && path_refused(&error)
        {
            narrowed(shared, to, now);
        }
    }
}

/// What one frame's turn at the wire achieved.
enum Sealed {
    Sent,
    /// The window or the pacer refuses this frame now, and when it would not.
    /// `None` is a window only an acknowledgement or a timer reopens.
    Blocked(Option<Instant>),
    /// The link cannot carry this frame at all.
    Failed,
}

/// Behind their own lock rather than the endpoint's: compressing a paste under
/// the mutex the reader holds per datagram is a keystroke waiting on a
/// congestion controller.
#[derive(Default)]
struct Outbound {
    packed: Vec<u8>,
    sealed: Vec<u8>,
}

/// The sending half: one frame, one datagram.
pub(crate) struct DatagramSink {
    socket: Arc<UdpSocket>,
    /// The connection, and the only lock a keystroke can genuinely wait on.
    ///
    /// One lock rather than two: the congestion window is spent by this half
    /// and returned by the reader's acks, and the loss detector is written by
    /// both. What is split is the *work* — the compressor runs before this is
    /// taken and the socket is written after, leaving one AEAD seal inside.
    shared: Arc<Mutex<Endpoint>>,
    /// Shared with the tick thread: a datagram this half sends may leave an
    /// acknowledgement or a loss timer owed sooner than the tick would look.
    clock: Arc<Clock>,
    outbound: Mutex<Outbound>,
}

impl DatagramSink {
    fn seal(&self, frame: &[u8], now: Instant) -> Sealed {
        // The prefix is the stream's framing and stops here; a datagram's own
        // length is what the peer reads instead.
        let Some(payload) = frame.get(LENGTH_PREFIX..) else {
            return Sealed::Failed;
        };
        let Ok(mut outbound) = self.outbound.lock() else {
            return Sealed::Failed;
        };
        let Outbound { packed, sealed } = &mut *outbound;
        pack(payload, packed);
        let to = {
            let Ok(mut endpoint) = self.shared.lock() else {
                return Sealed::Failed;
            };
            sealed.clear();
            let to = match endpoint.send(packed, now, sealed) {
                Ok(to) => to,
                Err(SendError::Blocked) => {
                    return Sealed::Blocked(endpoint.ready(now, packed.len()));
                }
                // A frame too large to seal is one this client sized wrong.
                Err(_) => return Sealed::Failed,
            };
            if let Some(at) = endpoint.poll_deadline() {
                self.clock.advance(at);
            }
            to
        };
        match self.socket.send_to(sealed, to) {
            Ok(_) => Sealed::Sent,
            // Sealed and spent, and the loss detector reports it: a path that
            // narrowed is the MTU search's business, not a reason to leave.
            // The kernel already knows the width, so it is asked rather than
            // rediscovered three losses and two spaced probes later.
            Err(error) if path_refused(&error) => {
                if sealed.len() > BASE_DATAGRAM {
                    narrowed(&self.shared, to, now);
                }
                Sealed::Sent
            }
            Err(_) => Sealed::Failed,
        }
    }
}

impl FrameSink for DatagramSink {
    fn carriage(&self) -> Carriage {
        Carriage::Datagram
    }

    /// A congestion window is not a link that stopped draining: the frame is
    /// dropped as the network would have, and the journal's resend deadline is
    /// what puts it back on the wire.
    fn send(&self, frame: Vec<u8>) -> bool {
        !matches!(self.seal(&frame, Instant::now()), Sealed::Failed)
    }

    /// The last message of a session waits for the window rather than joining
    /// the frames a journal replays: there is no later attachment to replay to.
    fn send_reserved(&self, frame: Vec<u8>) -> bool {
        let deadline = Instant::now() + RESERVED_WAIT;
        loop {
            let now = Instant::now();
            match self.seal(&frame, now) {
                Sealed::Sent => return true,
                Sealed::Failed => return false,
                Sealed::Blocked(ready) => {
                    if now >= deadline {
                        return false;
                    }
                    let wake = ready.unwrap_or(now + RESERVED_RETRY).min(deadline);
                    thread::sleep(wake.saturating_duration_since(now));
                }
            }
        }
    }
}

pub(crate) struct DatagramReader {
    socket: Arc<UdpSocket>,
    shared: Arc<Mutex<Endpoint>>,
    /// Shared with the tick thread: the acknowledgement an arriving datagram
    /// leaves owed is due in milliseconds, not at the tick's own ceiling.
    clock: Arc<Clock>,
    timeout: Deadline,
    /// Re-arming is a `setsockopt` and the deadline shrinks a few microseconds
    /// per pass, so arming per datagram would double this path's syscall count
    /// to restate an unchanged number. The cost is silence noticed a quantum
    /// late, against a deadline measured in seconds.
    armed: Option<Duration>,
    /// [`Endpoint::recv`] opens it in place. Sized to the MTU search's ceiling:
    /// a larger datagram is truncated, and a truncated one fails to
    /// authenticate rather than arrive short.
    datagram: Vec<u8>,
    /// Kept so the receive path allocates nothing per datagram.
    sealed: Vec<u8>,
}

/// How much the remaining deadline must move before the socket is re-armed.
const ARM_QUANTUM: Duration = Duration::from_millis(100);

impl DatagramReader {
    /// The next frame the session sent, or a transport loss.
    ///
    /// A datagram that does not authenticate is not an error: a session anyone
    /// could end with sixteen bytes is not a session. Only silence past the
    /// deadline is a loss, reported as [`DecodeError::is_transport_loss`].
    ///
    /// `park` runs immediately before this call sleeps and at no other time: a
    /// readable socket is not a frame, since acks, challenges, keep-alives and
    /// forgeries are all consumed here and read past.
    pub(crate) fn read_frame<'a>(
        &'a mut self,
        scratch: &'a mut Vec<u8>,
        park: &mut dyn FnMut(),
    ) -> Result<&'a [u8], DecodeError> {
        let frame = self.next_frame(park)?;
        // The bound is what the body may *inflate* to, and this path's own
        // budget bounds it: `MAX_FRAME` is the stream path's, a hundred
        // times wider than any datagram carries.
        unpack(&self.datagram[frame], MAX_PAYLOAD, scratch)
    }

    /// Where in `self.datagram` the next frame landed.
    ///
    /// Split from [`read_frame`](Self::read_frame) because a borrow of
    /// `self.datagram` cannot be returned out of a loop that also takes `self`
    /// mutably to read the next datagram into it.
    fn next_frame(&mut self, park: &mut dyn FnMut()) -> Result<Range<usize>, DecodeError> {
        let deadline = Instant::now() + self.timeout.get();
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(DecodeError::Io(io::Error::from(io::ErrorKind::TimedOut)));
            }
            let Some((len, from)) = self.receive(remaining, park)? else {
                continue;
            };
            let now = Instant::now();
            let opened = match self.shared.lock() {
                Ok(mut endpoint) => {
                    let opened = endpoint.recv(from, now, &mut self.datagram[..len]);
                    // The acknowledgement this datagram owes is due inside the
                    // far side's probe timer, which a quarter-second tick would
                    // run out: it counts the packet abandoned, this side
                    // silent, and two of those collapse its window.
                    if let Some(at) = endpoint.poll_deadline() {
                        self.clock.advance(at);
                    }
                    opened
                }
                Err(_) => return Err(DecodeError::Io(io::Error::other("datagram lock poisoned"))),
            };
            let frame = match opened {
                Ok(Received::Frame(frame)) => Some(frame),
                // A challenge, answer or keep-alive did its work inside the
                // endpoint; anything unplaceable is dropped and the read goes on.
                Ok(Received::Nothing) | Err(_) => None,
                // The one event a user reporting "it froze after I moved"
                // needs to see.
                Ok(Received::Migrated(to)) => {
                    log!("datagram path migrated to {to}");
                    None
                }
                Ok(Received::Closed) => return Err(DecodeError::Truncated),
            };
            // An ack sent here rather than on the tick is what the server's
            // window reopens on and what its round-trip estimate measures.
            transmit(&self.socket, &self.shared, &mut self.sealed);
            if let Some(frame) = frame {
                return Ok(frame);
            }
        }
    }

    /// `None` is a read worth trying again rather than something that arrived.
    ///
    /// The non-blocking read first is what separates a datagram already queued
    /// from a call about to sleep, and only the second is worth parking for.
    /// `MSG_DONTWAIT` rather than `O_NONBLOCK` because the socket is shared: a
    /// non-blocking `send_to` refusing a full buffer would read as a dead link.
    fn receive(
        &mut self,
        remaining: Duration,
        park: &mut dyn FnMut(),
    ) -> Result<Option<(usize, SocketAddr)>, DecodeError> {
        match rustix::net::recvfrom(&self.socket, &mut self.datagram[..], RecvFlags::DONTWAIT) {
            // A source the kernel did not report cannot be validated against a
            // path, so it is dropped like any other unplaceable datagram.
            Ok((len, _, from)) => {
                return Ok(from
                    .and_then(|from| SocketAddr::try_from(from).ok())
                    .map(|from| (len, from)));
            }
            Err(rustix::io::Errno::INTR) => return Ok(None),
            Err(rustix::io::Errno::WOULDBLOCK) => {}
            Err(error) => return Err(DecodeError::Io(error.into())),
        }
        park();
        // Too short is a spurious timeout the session reads as link loss, and
        // more than a quantum too long is silence noticed late.
        let armed = self.armed.is_some_and(|armed| {
            armed >= remaining && armed.saturating_sub(remaining) < ARM_QUANTUM
        });
        if !armed {
            self.socket.set_read_timeout(Some(remaining))?;
            self.armed = Some(remaining);
        }
        match self.socket.recv_from(&mut self.datagram) {
            Ok(received) => Ok(Some(received)),
            Err(error) if error.kind() == io::ErrorKind::Interrupted => Ok(None),
            // A timeout is silence; every other error is this process's own
            // socket failing, never the peer. Linux delivers no ICMP error on
            // an unconnected UDP socket without `IP_RECVERR`, deliberately
            // unset: the session's deadline is what notices a silent peer.
            Err(error) => Err(DecodeError::Io(error)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use braid_proto::{ByteOff, ClientMessage, CmdSeq, MAX_FRAME, Version};

    fn offer(ip: [u8; 16], port: u16) -> DatagramOffer {
        DatagramOffer {
            ip,
            port,
            cid: [7; 8],
            secret: [9; 32],
        }
    }

    /// The offer's address as the daemon wires it: IPv6, IPv4 in the mapped range.
    fn mapped(port: u16) -> DatagramOffer {
        let mut ip = [0; 16];
        ip[10] = 0xff;
        ip[11] = 0xff;
        ip[12..].copy_from_slice(&[127, 0, 0, 1]);
        offer(ip, port)
    }

    /// A daemon that opens one datagram and sends one back.
    fn echo_once(daemon: UdpSocket) -> thread::JoinHandle<()> {
        thread::spawn(move || {
            let mut buffer = vec![0; MAX_DATAGRAM];
            let (len, from) = daemon.recv_from(&mut buffer).expect("a datagram arrives");
            let now = Instant::now();
            let (accepted, received) = Endpoint::accept(
                ConnectionId::from_bytes([7; 8]),
                RootSecret::new([9; 32]),
                from,
                now,
                &mut buffer[..len],
                Fragmentation::Refused,
            )
            .expect("the datagram authenticates");
            let mut endpoint = accepted.commit();
            let Received::Frame(frame) = received else {
                panic!("the first datagram carries the frame");
            };
            let body = buffer[frame].to_vec();
            let mut out = Vec::new();
            let to = endpoint
                .send(&body, now, &mut out)
                .expect("the answer seals");
            daemon.send_to(&out, to).expect("the answer goes back");
        })
    }

    /// Wait until a datagram is actually on `socket`.
    ///
    /// Joining the sender proves its `send_to` returned, not that the kernel
    /// has put the datagram on this socket's receive queue: across loopback
    /// that hand-off is a softirq away, so a test that measures a
    /// non-blocking read in the window between them is measuring the
    /// scheduler. Peeked rather than read, so the call under test still finds
    /// the datagram where it expects it.
    fn until_readable(socket: &UdpSocket) {
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut peeked = [0; 1];
        loop {
            match rustix::net::recvfrom(socket, &mut peeked, RecvFlags::PEEK | RecvFlags::DONTWAIT)
            {
                Ok(_) => return,
                Err(rustix::io::Errno::WOULDBLOCK | rustix::io::Errno::INTR) => {}
                Err(error) => panic!("the link socket failed: {error}"),
            }
            assert!(
                Instant::now() < deadline,
                "the answer never reached the socket"
            );
            thread::sleep(Duration::from_millis(1));
        }
    }

    /// An accepting endpoint may send an unproved address only a small
    /// multiple of what arrived from it, so the challenge goes out first and
    /// the body waits for the answer.
    fn answer_once(daemon: UdpSocket, body: Vec<u8>) -> thread::JoinHandle<()> {
        thread::spawn(move || {
            let mut buffer = vec![0; MAX_DATAGRAM];
            let (len, from) = daemon.recv_from(&mut buffer).expect("a datagram arrives");
            let (accepted, _) = Endpoint::accept(
                ConnectionId::from_bytes([7; 8]),
                RootSecret::new([9; 32]),
                from,
                Instant::now(),
                &mut buffer[..len],
                Fragmentation::Refused,
            )
            .expect("the datagram authenticates");
            let mut endpoint = accepted.commit();
            let mut out = Vec::new();
            let owed = |endpoint: &mut Endpoint, out: &mut Vec<u8>| {
                while let Some((to, len)) = {
                    out.clear();
                    endpoint.poll_transmit(Instant::now(), out)
                } {
                    daemon
                        .send_to(&out[..len], to)
                        .expect("the datagram goes out");
                }
            };
            owed(&mut endpoint, &mut out);
            let (len, from) = daemon.recv_from(&mut buffer).expect("the answer arrives");
            let _ = endpoint.recv(from, Instant::now(), &mut buffer[..len]);
            owed(&mut endpoint, &mut out);
            out.clear();
            let to = endpoint
                .send(&body, Instant::now(), &mut out)
                .expect("the answer seals");
            daemon.send_to(&out, to).expect("the answer goes back");
        })
    }

    /// The relay fills the address in from `$SSH_CONNECTION`; zero is what it
    /// sends when it could not tell.
    #[test]
    fn an_offer_naming_no_address_is_refused() {
        assert!(DatagramLink::open(&offer([0; 16], 4000)).is_none());
        let mut mapped = [0; 16];
        mapped[10] = 0xff;
        mapped[11] = 0xff;
        mapped[15] = 1;
        assert!(DatagramLink::open(&offer(mapped, 0)).is_none());
    }

    /// A v4 peer reached as `::ffff:127.0.0.1` needs a v4 socket to reach it.
    #[test]
    fn a_mapped_address_is_reached_as_the_v4_one_it_is() {
        let peer = peer_address(&mapped(4000)).expect("a mapped address is an address");
        assert_eq!(peer, SocketAddr::from(([127, 0, 0, 1], 4000)));
    }

    /// A kernel that fragments a probe answers "yes" for a path that would
    /// have said no. Read back off the socket rather than trusted to a
    /// `setsockopt` that returned `Ok`.
    #[cfg(target_os = "linux")]
    #[test]
    fn a_link_socket_refuses_to_fragment() {
        use rustix::net::sockopt::{Ipv4PathMtuDiscovery, Ipv6PathMtuDiscovery};

        let v4 = DatagramLink::open(&mapped(4000)).expect("a v4 link");
        assert_eq!(
            rustix::net::sockopt::ip_mtu_discover(&*v4.socket).expect("IP_MTU_DISCOVER reads back"),
            Ipv4PathMtuDiscovery::DO
        );

        let mut loopback = [0; 16];
        loopback[15] = 1;
        let v6 = DatagramLink::open(&offer(loopback, 4000)).expect("a v6 link");
        assert_eq!(
            rustix::net::sockopt::ipv6_mtu_discover(&*v6.socket)
                .expect("IPV6_MTU_DISCOVER reads back"),
            Ipv6PathMtuDiscovery::DO
        );
    }

    /// The same fact on Darwin, where neither setting nor reading the option
    /// has a safe binding: a `setsockopt` that returned `Ok` is not the option
    /// being in force, and without it every session stays at the floor.
    #[cfg(target_vendor = "apple")]
    #[test]
    #[allow(unsafe_code, reason = "IP_DONTFRAG has no safe binding to read back")]
    fn a_link_socket_refuses_to_fragment() {
        use std::os::fd::AsRawFd;

        /// The option `libc` does not declare, as `refuse_fragmentation` has it.
        const IPV6_DONTFRAG: libc::c_int = 62;

        let mut loopback = [0; 16];
        loopback[15] = 1;
        let v4 = DatagramLink::open(&mapped(4000)).expect("a v4 link");
        let v6 = DatagramLink::open(&offer(loopback, 4000)).expect("a v6 link");
        for (link, level, option) in [
            (&v4, libc::IPPROTO_IP, libc::IP_DONTFRAG),
            (&v6, libc::IPPROTO_IPV6, IPV6_DONTFRAG),
        ] {
            let mut on: libc::c_int = 0;
            // What the option is, and what the call writes the length back to.
            let mut len: libc::socklen_t = 4;
            // SAFETY: the link owns the descriptor for the whole call, and
            // `on` and `len` are live and correctly typed for this option.
            let read = unsafe {
                libc::getsockopt(
                    link.socket.as_raw_fd(),
                    level,
                    option,
                    std::ptr::from_mut(&mut on).cast(),
                    std::ptr::from_mut(&mut len),
                )
            };
            assert_eq!(read, 0, "the option does not read back");
            assert_ne!(on, 0, "the kernel would still fragment this link's probes");
        }
    }

    /// The deadline that matters appears *after* the sleep was computed: a lone
    /// server frame owes an acknowledgement in 25 ms, and held for the tick's
    /// quarter second the far side runs the packet to its probe timeout, counts
    /// this client silent, and collapses its window on the second one.
    #[test]
    fn a_deadline_named_after_the_sleep_began_still_wakes_the_tick() {
        let clock = Arc::new(Clock::default());
        let ticking = Arc::clone(&clock);
        let started = Instant::now();
        let tick = thread::spawn(move || ticking.sleep_until(Instant::now() + TRANSMIT_TICK * 20));
        // The sleep has to be under way, or this names a deadline before it.
        thread::sleep(Duration::from_millis(20));
        clock.advance(Instant::now() + Duration::from_millis(5));
        tick.join().expect("the tick thread wakes");
        assert!(
            started.elapsed() < TRANSMIT_TICK,
            "the tick slept out a deadline something had brought forward"
        );
    }

    /// The other half of the same race, and the harder one: the sleeper computes
    /// its deadline with the endpoint lock released, so an acknowledgement owed
    /// in that window is named before the sleep begins and would be lost to a
    /// clock that assigned over it rather than taking the sooner of the two.
    #[test]
    fn a_deadline_named_before_the_sleep_began_still_shortens_it() {
        let clock = Clock::default();
        clock.rearm();
        clock.advance(Instant::now() + Duration::from_millis(5));
        let started = Instant::now();
        clock.sleep_until(Instant::now() + TRANSMIT_TICK * 20);
        assert!(
            started.elapsed() < TRANSMIT_TICK,
            "the sleep overwrote a deadline that landed while it was being computed"
        );
    }

    /// And the deadline a pass has already slept out must not shorten the next
    /// one, or the tick spins.
    #[test]
    fn rearming_forgets_a_deadline_the_last_pass_consumed() {
        let clock = Clock::default();
        clock.advance(Instant::now());
        clock.rearm();
        let started = Instant::now();
        clock.sleep_until(Instant::now() + Duration::from_millis(30));
        assert!(
            started.elapsed() >= Duration::from_millis(25),
            "a spent deadline carried into the next sleep"
        );
    }

    /// `EMSGSIZE` is the ordinary outcome of a path that narrowed. Provoked
    /// with a datagram past the UDP maximum, which every kernel refuses.
    #[test]
    fn a_datagram_the_path_refuses_is_not_a_dead_link() {
        let socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).expect("a socket");
        let peer = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).expect("a peer");
        let error = socket
            .send_to(&vec![0; 70_000], peer.local_addr().expect("a bound port"))
            .expect_err("a datagram past the UDP maximum goes nowhere");
        assert!(path_refused(&error));
        assert!(!path_refused(&io::Error::from(
            io::ErrorKind::ConnectionRefused
        )));
    }

    /// A connection whose tick never started works while typed into and then
    /// loses the peer for good on the first network change, so the offer is
    /// declined and `ssh` keeps carrying the session.
    ///
    /// Run in a child process because `RLIMIT_NPROC` is per user; the child
    /// lowers its own limit because `ulimit` has no portable spelling for this
    /// resource — bash calls it `-u`, dash `-p` and errors on `-u`.
    #[cfg(target_os = "linux")]
    #[test]
    fn a_link_whose_tick_cannot_start_is_no_link() {
        use rustix::process::{Resource, Rlimit, getrlimit, setrlimit};

        const IN_CHILD: &str = "BRD_TEST_NO_THREADS";
        const NAME: &str = "dgram::tests::a_link_whose_tick_cannot_start_is_no_link";
        if std::env::var_os(IN_CHILD).is_some() {
            let nproc = getrlimit(Resource::Nproc);
            setrlimit(
                Resource::Nproc,
                Rlimit {
                    current: Some(1),
                    maximum: nproc.maximum,
                },
            )
            .expect("a process may lower its own limit");
            // The kernel exempts root and `CAP_SYS_RESOURCE`, so a probe that
            // still starts cannot produce the condition under test.
            if let Ok(probe) = thread::Builder::new().spawn(|| {}) {
                probe.join().expect("the probe finishes");
                return;
            }
            let link = DatagramLink::open(&mapped(4000)).expect("a link");
            assert!(link.split(Deadline::new(Duration::from_secs(5))).is_none());
            return;
        }
        let exe = std::env::current_exe().expect("the test binary");
        let child = std::process::Command::new(exe)
            .args(["--exact", NAME, "--test-threads", "1"])
            .env(IN_CHILD, "1")
            .output()
            .expect("the child runs");
        assert!(
            child.status.success(),
            "{}{}",
            String::from_utf8_lossy(&child.stdout),
            String::from_utf8_lossy(&child.stderr)
        );
    }

    /// The whole path over a real socket: packed, sealed, opened on the far
    /// side and unpacked back through the codec in both directions. The paste
    /// is larger than a datagram carries uncompressed.
    #[test]
    fn a_frame_survives_the_socket_it_was_sealed_into() {
        let paste = ClientMessage::Input {
            seq: CmdSeq::first(),
            bytes: b"the same line over and over ".repeat(300),
        };
        assert!(
            paste.encode(Version::LOCAL).expect("a paste encodes").len() > MIN_DATAGRAM_FRAME,
            "the test frame is meant to need the compressor"
        );
        for message in [
            ClientMessage::Pong {
                token: 42,
                consumed: ByteOff::zero(),
            },
            paste,
        ] {
            let daemon = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).expect("a daemon socket");
            let offer = mapped(daemon.local_addr().expect("a bound port").port());
            let echoed = echo_once(daemon);

            let (sink, mut reader) = DatagramLink::open(&offer)
                .expect("a link")
                .split(Deadline::new(Duration::from_secs(5)))
                .expect("the tick thread starts");
            assert!(sink.send(message.encode(Version::LOCAL).expect("the message encodes")));
            let mut scratch = Vec::new();
            let answer = reader
                .read_frame(&mut scratch, &mut || {})
                .expect("the answer comes back");
            assert_eq!(
                ClientMessage::decode(answer, Version::LOCAL).expect("the answer decodes"),
                message
            );
            echoed.join().expect("the daemon half finishes");
        }
    }

    /// A readable socket is not a frame, so parking per datagram would put a
    /// terminal write back on each one; parking on none would hold a
    /// keystroke's echo until the next datagram happened to arrive.
    #[test]
    fn a_read_parks_only_when_it_is_about_to_wait() {
        let daemon = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).expect("a daemon socket");
        let offer = mapped(daemon.local_addr().expect("a bound port").port());
        let echoed = echo_once(daemon);

        let (sink, mut reader) = DatagramLink::open(&offer)
            .expect("a link")
            .split(Deadline::new(Duration::from_secs(5)))
            .expect("the tick thread starts");
        let pong = ClientMessage::Pong {
            token: 42,
            consumed: ByteOff::zero(),
        };
        assert!(sink.send(pong.encode(Version::LOCAL).expect("a pong encodes")));
        echoed.join().expect("the daemon half finishes");
        // Waited for rather than assumed: the assertion below is about the
        // non-blocking read this path tries first, and it only says that when
        // the answer is genuinely on the socket to be found.
        until_readable(&reader.socket);
        let mut parks = 0;
        let mut payload = Vec::new();
        reader
            .read_frame(&mut payload, &mut || parks += 1)
            .expect("the answer comes back");
        assert_eq!(parks, 0, "a datagram already queued was parked for");

        // Nothing is coming now, so the read must say so before it waits.
        let mut parks = 0;
        let mut reader = DatagramReader {
            timeout: Deadline::new(Duration::from_millis(50)),
            ..reader
        };
        let waited = reader
            .read_frame(&mut payload, &mut || parks += 1)
            .expect_err("nothing else is coming");
        assert!(waited.is_transport_loss(), "silence is a transport loss");
        assert!(parks > 0, "a read that slept never said it was about to");
    }

    /// Nothing is acknowledged here, so the window shuts and stays shut — and
    /// the session must stay on this transport, because the journal is what
    /// replays what the window refused.
    #[test]
    fn a_full_window_is_not_reported_as_a_dead_link() {
        let daemon = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).expect("a daemon socket");
        let offer = mapped(daemon.local_addr().expect("a bound port").port());
        let (sink, _reader) = DatagramLink::open(&offer)
            .expect("a link")
            .split(Deadline::new(Duration::from_secs(5)))
            .expect("the tick thread starts");
        // Incompressible, so the window sees every byte the paste is worth.
        let mut noise = vec![0u8; 1000];
        getrandom::fill(&mut noise).expect("entropy");

        let mut blocked = false;
        for _ in 0..64 {
            let frame = ClientMessage::Input {
                seq: CmdSeq::first(),
                bytes: noise.clone(),
            }
            .encode(Version::LOCAL)
            .expect("an input frame");
            assert!(sink.send(frame), "backpressure was reported as link loss");
            blocked |= matches!(
                sink.seal(&[0, 0, 0, 1, 0], Instant::now()),
                Sealed::Blocked(_)
            );
        }
        assert!(blocked, "the window never shut, so nothing was proved");
    }

    /// The one message a full window must not simply swallow: there is no
    /// later attachment to replay a farewell to, so it waits.
    #[test]
    fn a_farewell_waits_out_a_full_window_before_giving_up() {
        let daemon = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).expect("a daemon socket");
        let offer = mapped(daemon.local_addr().expect("a bound port").port());
        let (sink, _reader) = DatagramLink::open(&offer)
            .expect("a link")
            .split(Deadline::new(Duration::from_secs(5)))
            .expect("the tick thread starts");
        let close = ClientMessage::Close {
            seq: CmdSeq::first(),
        }
        .encode(Version::LOCAL)
        .expect("a close encodes");
        // Filled with the farewell frame itself: a larger filler would leave
        // room this one still fits into.
        loop {
            match sink.seal(&close, Instant::now()) {
                Sealed::Sent => {}
                Sealed::Blocked(_) => break,
                Sealed::Failed => panic!("the socket refused a frame the window admitted"),
            }
        }

        let started = Instant::now();
        let sent = sink.send_reserved(close);

        assert!(!sent, "a farewell nothing carried was reported as sent");
        assert!(
            started.elapsed() >= RESERVED_WAIT,
            "the farewell gave up without waiting for the window"
        );
    }

    /// A datagram carries no length prefix to cross-check the body against, so
    /// `unpack` is the whole check and must refuse rather than index-panic.
    #[test]
    fn a_datagram_body_that_is_not_a_frame_is_refused() {
        let mut scratch = Vec::new();
        assert!(unpack(&[], MAX_PAYLOAD, &mut scratch).is_err());
        assert!(unpack(&[0xEE, 1, 2], MAX_PAYLOAD, &mut scratch).is_err());
        // Stored, so the answer is the datagram's own bytes and the scratch is untouched.
        assert_eq!(
            unpack(&[0, 1, 2], MAX_PAYLOAD, &mut scratch).expect("a stored body"),
            &[1, 2]
        );
    }

    /// The ceiling this path reads against is its own: `MAX_FRAME` would be a
    /// mebibyte of growing and zeroing per datagram on a path whose frames are
    /// cut to fit one, and nothing legitimate is refused by the narrower bound.
    #[test]
    fn the_widest_frame_a_datagram_carries_is_admitted_and_nothing_wider_is() {
        assert!(
            MAX_PAYLOAD < MAX_FRAME as usize / 100,
            "the client's ceiling is still the stream path's"
        );
        let mut packed = Vec::new();
        let mut payload = Vec::new();

        // Compressible, so the bound has to be applied to what comes out.
        let widest = b"the same line over and over "
            .iter()
            .copied()
            .cycle()
            .take(MAX_PAYLOAD)
            .collect::<Vec<_>>();
        pack(&widest, &mut packed);
        assert!(
            packed.len() < MAX_PAYLOAD / 10,
            "the test body has to be one this side would actually receive"
        );
        assert!(
            unpack(&packed, MAX_PAYLOAD, &mut payload).is_ok(),
            "a frame filling the widest datagram this path carries was refused"
        );
        assert_eq!(payload, widest);

        let bomb = vec![0_u8; 16 * MAX_PAYLOAD];
        pack(&bomb, &mut packed);
        assert!(unpack(&packed, MAX_PAYLOAD, &mut payload).is_err());
    }

    /// And the reader is what has to apply it, per arriving datagram.
    #[test]
    fn a_frame_wider_than_any_datagram_carries_is_refused_by_the_reader() {
        let daemon = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).expect("a daemon socket");
        let offer = mapped(daemon.local_addr().expect("a bound port").port());
        let mut bomb = Vec::new();
        pack(&vec![0_u8; 16 * MAX_PAYLOAD], &mut bomb);
        assert!(
            bomb.len() < BASE_DATAGRAM / 2,
            "the body has to be one a single datagram carries"
        );
        let answered = answer_once(daemon, bomb);

        let (sink, mut reader) = DatagramLink::open(&offer)
            .expect("a link")
            .split(Deadline::new(Duration::from_secs(5)))
            .expect("the tick thread starts");
        assert!(
            sink.send(
                ClientMessage::Pong {
                    token: 1,
                    consumed: ByteOff::zero(),
                }
                .encode(Version::LOCAL)
                .expect("a pong encodes")
            )
        );

        let mut payload = Vec::new();
        let refused = reader
            .read_frame(&mut payload, &mut || {})
            .expect_err("a body no datagram could have carried was admitted");
        assert!(
            !refused.is_transport_loss(),
            "the answer never arrived, so nothing was proved: {refused:?}"
        );
        // The length, not the capacity: `room_for` never offers the decompressor
        // more than the ceiling, and what a `Vec` rounds its allocation up to is
        // the allocator's business.
        assert!(
            payload.len() <= MAX_PAYLOAD,
            "the bomb was inflated anyway: {} bytes against a {MAX_PAYLOAD}-byte ceiling",
            payload.len()
        );
        answered.join().expect("the daemon half finishes");
    }
}
