#![forbid(unsafe_code)]

//! The half that owes a socket. The advertised window is what keeps a peer
//! sending into a socket that stopped reading from becoming this process's
//! memory.

use crate::{
    Ack, CHUNK_WINDOW, Contradiction, FORWARD_WINDOW, MAX_ACK_RUNS, MAX_FORWARD_CHUNK,
    MAX_HELD_SEGMENTS, SackRun, SackRuns, as_len, as_off,
};
use std::collections::{BTreeMap, VecDeque};
use std::num::NonZeroU32;

/// Buffers kept for segments held past a gap, and the largest one kept. The
/// held set reaches 256 segments of 32 KiB, so pooling every one would idle
/// 8 MiB beside a 256 KiB window; four at a quarter-segment idle 32 KiB, which
/// is one segment. The size bound is what holds it there: the gap a lost
/// datagram opens is filled by segments cut to the datagram budget - 1138
/// bytes on the link `tests/link.rs` drives - so a buffer grown to a whole
/// stream-sized segment goes back rather than sit on 32 KiB awaiting a
/// kilobyte.
const SPARE_HELD: usize = 4;
const SPARE_HELD_BYTES: usize = MAX_FORWARD_CHUNK / 4;

pub(crate) struct Receiver {
    /// Once the peer's end has been placed this sits one past the offset that
    /// end occupies.
    next: u64,
    ready: VecDeque<u8>,
    /// Segments past the gap at `next`, by offset.
    held: BTreeMap<u64, Vec<u8>>,
    held_bytes: usize,
    /// Buffers of segments already placed, for the next segment held.
    spare: Vec<Vec<u8>>,
    fin: Option<u64>,
    finished: bool,
    ack_owed: bool,
    /// The runs of the last acknowledgement, kept for the next one: an
    /// acknowledgement is owed once per arrival including a duplicate.
    /// `SackRuns::from_slice` copies out of this, so what the retention saves is
    /// not that copy but the accumulator's growth from zero — and the copy is
    /// sized by the merged run count rather than by the held segments the merge
    /// folded away.
    runs: Vec<SackRun>,
}

impl Receiver {
    pub(crate) fn new() -> Self {
        Self {
            next: 0,
            ready: VecDeque::new(),
            held: BTreeMap::new(),
            held_bytes: 0,
            spare: Vec::new(),
            fin: None,
            finished: false,
            ack_owed: false,
            runs: Vec::new(),
        }
    }

    pub(crate) fn readable(&self) -> (&[u8], &[u8]) {
        self.ready.as_slices()
    }

    pub(crate) fn readable_len(&self) -> usize {
        self.ready.len()
    }

    pub(crate) fn finished(&self) -> bool {
        self.finished
    }

    /// Room past `next`: what the peer may have in flight.
    fn window(&self) -> u32 {
        let used = self.ready.len().saturating_add(self.held_bytes);
        u32::try_from(FORWARD_WINDOW.saturating_sub(used)).unwrap_or(u32::MAX)
    }

    /// Whether `len` more bytes fit the buffer the local socket reads. Asked of
    /// a zero-window probe too, or a probe per timeout is unbounded. Held bytes
    /// are charged to the advertised window but not to this bound: counting
    /// them would let a buffer filled past a gap refuse the segment that
    /// empties it.
    fn admits(&self, len: usize) -> bool {
        self.ready.len() + len <= FORWARD_WINDOW
    }

    pub(crate) fn consume(&mut self, n: usize) {
        let before = self.window();
        let taken = n.min(self.ready.len());
        self.ready.drain(..taken);
        // Only when a whole segment fits again: a window reopened a byte at a
        // time is the silly-window collapse.
        if before < CHUNK_WINDOW && self.window() >= CHUNK_WINDOW {
            self.ack_owed = true;
        }
    }

    pub(crate) fn poll_ack(&mut self) -> Option<Ack> {
        self.ack_owed.then(|| {
            self.ack_owed = false;
            Ack {
                off: self.next,
                window: self.window(),
                held: self.held_runs(),
            }
        })
    }

    /// Runs above the gap, measured from it outwards. Overlapping or abutting
    /// segments are merged, or a sender is told to leave a hole it has filled;
    /// the lowest runs win the cut, since they are repaired first.
    fn held_runs(&mut self) -> SackRuns {
        // Split so the walk over `held` and the refill of `runs` borrow apart.
        let Self {
            next, held, runs, ..
        } = self;
        runs.clear();
        let mut cursor = *next;
        let mut open: Option<(u64, u64)> = None;
        for (&off, held) in &*held {
            let end = off.saturating_add(as_off(held.len()));
            match open {
                Some((start, seen)) if off <= seen => open = Some((start, seen.max(end))),
                Some(run) => {
                    push_run(runs, &mut cursor, run);
                    open = Some((off, end));
                }
                None => open = Some((off, end)),
            }
            if runs.len() == MAX_ACK_RUNS {
                break;
            }
        }
        if let Some(run) = open
            && runs.len() < MAX_ACK_RUNS
        {
            push_run(runs, &mut cursor, run);
        }
        SackRuns::from_slice(runs).expect("the loop stops at MAX_ACK_RUNS")
    }

    pub(crate) fn on_data(
        &mut self,
        off: u64,
        fin: bool,
        bytes: &[u8],
    ) -> Result<(), Contradiction> {
        let end = off.saturating_add(as_off(bytes.len()));
        if let Some(known) = self.fin {
            // A second end at another offset, or payload past the one declared.
            if end > known || (fin && end != known) {
                return Err(Contradiction);
            }
        } else if fin {
            if self.next > end || self.held_past(end) {
                return Err(Contradiction);
            }
            self.fin = Some(end);
        }
        // Owed on every arrival, including a duplicate: a duplicate is the peer
        // repairing what it believes was lost.
        self.ack_owed = true;
        if end > self.next {
            if off > self.next {
                self.hold(off, bytes);
            } else {
                let fresh = &bytes[as_len(self.next - off)..];
                // Moving `next` past bytes this side dropped would tell the
                // peer they arrived.
                if self.admits(fresh.len()) {
                    self.ready.extend(fresh);
                    self.next = end;
                    self.drain_held();
                }
            }
        }
        self.settle_end();
        Ok(())
    }

    fn held_past(&self, end: u64) -> bool {
        self.held
            .iter()
            .any(|(off, held)| off.saturating_add(as_off(held.len())) > end)
    }

    fn hold(&mut self, off: u64, bytes: &[u8]) {
        // Refused rather than truncated: a segment stored short is one whose
        // offsets no longer describe its bytes.
        if bytes.is_empty()
            || self.held.len() >= MAX_HELD_SEGMENTS
            || off >= self.next.saturating_add(as_off(FORWARD_WINDOW))
            || self.ready.len() + self.held_bytes + bytes.len() > FORWARD_WINDOW
        {
            return;
        }
        let spare = &mut self.spare;
        let entry = self
            .held
            .entry(off)
            .or_insert_with(|| spare.pop().unwrap_or_default());
        if entry.len() < bytes.len() {
            self.held_bytes = self.held_bytes + bytes.len() - entry.len();
            entry.clear();
            entry.extend_from_slice(bytes);
        }
    }

    fn drain_held(&mut self) {
        while let Some((&off, _)) = self.held.first_key_value() {
            if off > self.next {
                break;
            }
            let Some(held) = self.held.remove(&off) else {
                break;
            };
            self.held_bytes -= held.len();
            let end = off.saturating_add(as_off(held.len()));
            if end > self.next {
                let skip = as_len(self.next - off);
                self.ready.extend(&held[skip..]);
                self.next = end;
            }
            self.recycle(held);
        }
    }

    /// Cleared on the way in: a buffer handed back with its bytes still in it
    /// would be read as a held segment already longer than what replaces it.
    fn recycle(&mut self, mut buffer: Vec<u8>) {
        if self.spare.len() < SPARE_HELD && buffer.capacity() <= SPARE_HELD_BYTES {
            buffer.clear();
            self.spare.push(buffer);
        }
    }

    /// The end occupies its own offset, so `next` moves past it and an
    /// acknowledgement of the end differs from one of the last byte.
    fn settle_end(&mut self) {
        if !self.finished && self.fin == Some(self.next) {
            self.next += 1;
            self.finished = true;
        }
    }
}

/// One merged run, as the gap from where the last one ended and its length. A
/// run that will not fit the relative form is dropped rather than clamped: a
/// clamped run tells the sender to skip bytes that never arrived.
fn push_run(runs: &mut Vec<SackRun>, cursor: &mut u64, (start, end): (u64, u64)) {
    let Ok(gap) = u32::try_from(start.saturating_sub(*cursor)) else {
        return;
    };
    let Ok(len) = u32::try_from(end.saturating_sub(start)) else {
        return;
    };
    let (Some(gap), Some(len)) = (NonZeroU32::new(gap), NonZeroU32::new(len)) else {
        return;
    };
    runs.push(SackRun { gap, len });
    *cursor = end;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn drained(receiver: &Receiver) -> Vec<u8> {
        let (front, back) = receiver.readable();
        [front, back].concat()
    }

    /// Fill the window with in-order segments, as a socket that has stopped
    /// reading leaves it. Returns the offset the peer is now stuck at.
    fn shut_window(recv: &mut Receiver) -> u64 {
        let bulk = vec![b'x'; MAX_FORWARD_CHUNK];
        let mut off = 0;
        for _ in 0..(FORWARD_WINDOW / MAX_FORWARD_CHUNK) {
            recv.on_data(off, false, &bulk).expect("in window");
            off += as_off(bulk.len());
        }
        off
    }

    #[test]
    fn a_gap_is_held_until_it_fills() {
        let mut recv = Receiver::new();
        recv.on_data(3, false, b"def").expect("held");
        assert_eq!(recv.next, 0);
        assert!(drained(&recv).is_empty());
        recv.on_data(0, false, b"abc").expect("the gap");
        assert_eq!(drained(&recv), b"abcdef");
        assert_eq!(recv.next, 6);
    }

    #[test]
    fn an_overlapping_segment_contributes_only_its_tail() {
        let mut recv = Receiver::new();
        recv.on_data(0, false, b"abc").expect("first");
        recv.on_data(1, false, b"bcde").expect("overlapping");
        assert_eq!(drained(&recv), b"abcde");
    }

    #[test]
    fn an_acknowledgement_is_owed_once_per_arrival_including_a_duplicate() {
        let mut recv = Receiver::new();
        recv.on_data(0, false, b"abc").expect("first");
        assert!(recv.poll_ack().is_some());
        assert!(recv.poll_ack().is_none(), "nothing new to say");
        recv.on_data(0, false, b"abc").expect("a repeat");
        assert_eq!(
            recv.poll_ack().map(|ack| ack.off),
            Some(3),
            "a repair is answered or the peer repairs forever"
        );
    }

    #[test]
    fn a_window_reopens_only_for_a_whole_segment() {
        let mut recv = Receiver::new();
        shut_window(&mut recv);
        recv.poll_ack().expect("the shut window");
        recv.consume(1);
        assert!(recv.poll_ack().is_none(), "one byte is not room");
        recv.consume(MAX_FORWARD_CHUNK);
        assert!(recv.poll_ack().is_some(), "a whole segment is");
    }

    #[test]
    fn in_order_bytes_past_a_shut_window_are_refused() {
        let mut recv = Receiver::new();
        let stuck = shut_window(&mut recv);
        let ack = recv.poll_ack().expect("an ack");
        assert_eq!((ack.off, ack.window), (stuck, 0));
        // A peer that ignores the window: in order, at the offset this side
        // asked for, for as long as it likes.
        let bulk = vec![b'y'; MAX_FORWARD_CHUNK];
        for _ in 0..64 {
            recv.on_data(stuck, false, &bulk)
                .expect("legal but unusable");
        }
        assert_eq!(
            recv.readable_len(),
            FORWARD_WINDOW,
            "the advertised window is the bound"
        );
        assert_eq!(
            recv.next, stuck,
            "refused bytes are not acknowledged, or the peer never sends them again"
        );
    }

    #[test]
    fn a_zero_window_probe_is_refused_and_still_answered() {
        let mut recv = Receiver::new();
        let stuck = shut_window(&mut recv);
        recv.poll_ack().expect("the shut window");
        recv.on_data(stuck, false, b"p").expect("a probe");
        assert_eq!(
            recv.readable_len(),
            FORWARD_WINDOW,
            "one byte is one byte too many"
        );
        let ack = recv.poll_ack().expect("an answer");
        assert_eq!(
            (ack.off, ack.window),
            (stuck, 0),
            "a probe refused is a probe still answered"
        );
    }

    #[test]
    fn a_gap_is_filled_even_when_what_sits_past_it_shut_the_window() {
        let mut recv = Receiver::new();
        let bulk = vec![b'x'; MAX_FORWARD_CHUNK];
        // Repairs cut at different boundaries overlap once they arrive out of
        // order, and the bytes held then outweigh the span they cover.
        for slot in 1..=8 {
            recv.on_data(slot * 10, false, &bulk).expect("past the gap");
        }
        assert_eq!(recv.held_bytes, FORWARD_WINDOW);
        let ack = recv.poll_ack().expect("an ack");
        assert_eq!((ack.off, ack.window), (0, 0));
        recv.on_data(0, false, b"0123456789").expect("the gap");
        assert_eq!(recv.next, 80 + as_off(MAX_FORWARD_CHUNK));
        assert!(
            recv.held.is_empty(),
            "held bytes may not refuse the segment that drains them"
        );
        assert!(recv.readable_len() <= FORWARD_WINDOW);
    }

    #[test]
    fn a_segment_past_the_window_is_refused_rather_than_held() {
        let mut recv = Receiver::new();
        recv.on_data(as_off(FORWARD_WINDOW) + 1, false, b"far away")
            .expect("legal but unusable");
        assert_eq!(recv.held_bytes, 0);
    }

    #[test]
    fn an_end_declared_behind_held_data_is_a_contradiction() {
        let mut recv = Receiver::new();
        recv.on_data(10, false, b"held").expect("held");
        assert_eq!(recv.on_data(0, true, b"abc"), Err(Contradiction));
    }

    #[test]
    fn a_reused_buffer_carries_none_of_the_segment_it_held() {
        let mut recv = Receiver::new();
        recv.on_data(3, false, b"defgh").expect("held");
        recv.on_data(0, false, b"abc").expect("the gap");
        assert_eq!(drained(&recv), b"abcdefgh");
        assert_eq!(recv.spare.len(), 1, "a datagram-sized buffer is kept");
        // Shorter than what the pooled buffer last held, which is the case a
        // buffer handed back uncleared would silently refuse to store.
        recv.on_data(9, false, b"j").expect("a second gap");
        recv.on_data(8, false, b"i").expect("the second gap");
        assert_eq!(drained(&recv), b"abcdefghij");
        assert_eq!(recv.held_bytes, 0);
    }

    #[test]
    fn a_whole_segment_buffer_is_dropped_rather_than_pooled() {
        let mut recv = Receiver::new();
        let bulk = vec![b'x'; MAX_FORWARD_CHUNK];
        recv.on_data(1, false, &bulk).expect("past the gap");
        recv.on_data(0, false, b"a").expect("the gap");
        assert!(recv.held.is_empty());
        assert!(
            recv.spare.is_empty(),
            "32 KiB idle against the next kilobyte costs more than the malloc"
        );
    }

    #[test]
    fn the_pool_of_spare_buffers_is_bounded() {
        let mut recv = Receiver::new();
        for slot in 1..=as_off(SPARE_HELD + 4) {
            recv.on_data(slot, false, b"x").expect("past the gap");
        }
        recv.on_data(0, false, b"x").expect("the gap");
        assert!(recv.held.is_empty());
        assert_eq!(recv.spare.len(), SPARE_HELD, "idle capacity has a ceiling");
    }

    #[test]
    fn the_run_buffer_stops_growing_once_an_acknowledgement_has_warmed_it() {
        let mut recv = Receiver::new();
        // Disjoint, so each one is a run of its own rather than a merge.
        for slot in 1..=8 {
            recv.on_data(slot * 10, false, b"held")
                .expect("past the gap");
        }
        let ack = recv.poll_ack().expect("the first ack");
        assert_eq!(ack.held.as_slice().len(), 8);
        let warm = recv.runs.capacity();
        for slot in 1..=8 {
            recv.on_data(slot * 10, false, b"held").expect("a repeat");
            let ack = recv.poll_ack().expect("owed once per arrival");
            assert_eq!(
                ack.held.as_slice().len(),
                8,
                "a refill that did not clear would stack the runs it already sent"
            );
            assert_eq!(
                recv.runs.capacity(),
                warm,
                "one allocation, not one per ack"
            );
        }
    }
}
