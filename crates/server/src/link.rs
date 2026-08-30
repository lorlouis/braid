#![forbid(unsafe_code)]

//! A smoothed round-trip estimate, and the probe still in flight: the repaint
//! rate and the deadline that calls a peer dead both derive from it rather than
//! from a constant or from TCP.

use crate::{
    PING_CEILING, PING_DEAD_FLOOR, PING_DEAD_INTERVALS, PING_FLOOR, REPAINT_CEILING, REPAINT_FLOOR,
    RTT_SHIFT,
};
use std::time::{Duration, Instant};

/// How far the probe interval may back off while the session carries nothing.
/// [`PING_CEILING`] is eight times [`PING_FLOOR`], so three doublings reach it
/// from anywhere. A doubling rather than a longer step because the client waits
/// three intervals of whatever the last `Ping` named: at twice, the `Ping` that
/// announces the longer interval always lands inside the patience its
/// predecessor bought.
const IDLE_BACKOFF_STEPS: u32 = 3;

/// A smoothed round-trip estimate, and the probe still in flight. Every pacing
/// interval on this attachment is derived from `srtt`.
pub(crate) struct LinkTiming {
    pub(crate) srtt: Option<Duration>,
    /// The probe token last sent, and when.
    pub(crate) outstanding: Option<(u64, Instant)>,
    next_token: u64,
    pub(crate) last_probe: Instant,
    /// When the client last answered anything.
    last_answer: Instant,
    /// Doublings the probe interval has taken for want of anything to measure.
    idle_probes: u32,
    /// Whether the session has carried nothing since the last probe left.
    quiet: bool,
}

impl LinkTiming {
    pub(crate) fn new() -> Self {
        let now = Instant::now();
        Self {
            srtt: None,
            outstanding: None,
            next_token: 1,
            last_probe: now,
            last_answer: now,
            idle_probes: 0,
            quiet: true,
        }
    }

    /// The next probe token, if one is due.
    pub(crate) fn probe(&mut self, now: Instant) -> Option<u64> {
        if now.saturating_duration_since(self.last_probe) < self.ping_interval() {
            return None;
        }
        let token = self.next_token;
        self.next_token = self.next_token.wrapping_add(1);
        self.last_probe = now;
        // Only the newest probe is timed: an answer to an older one measures
        // the queue it waited in rather than the link.
        self.outstanding = Some((token, now));
        self.quiet = true;
        Some(token)
    }

    pub(crate) fn answered(&mut self, token: u64) {
        self.last_answer = Instant::now();
        let Some((sent_token, sent_at)) = self.outstanding else {
            return;
        };
        if sent_token != token {
            return;
        }
        self.outstanding = None;
        // A probe answered with nothing to show for it between: the estimate is
        // only load-bearing while there is traffic to pace and predict, and the
        // first keystroke gives a fresh sample before anything reads it again.
        if self.quiet {
            self.idle_probes = (self.idle_probes + 1).min(IDLE_BACKOFF_STEPS);
        }
        let sample = sent_at.elapsed();
        // One eighth of the error, in whichever direction it falls: `Duration`
        // has no signed form, so the two halves are taken separately.
        self.srtt = Some(match self.srtt {
            None => sample,
            Some(srtt) => (srtt + sample.saturating_sub(srtt) / (1 << RTT_SHIFT))
                .saturating_sub(srtt.saturating_sub(sample) / (1 << RTT_SHIFT)),
        });
    }

    /// The session carried a byte, in either direction: pace the link by the
    /// link again, from the next probe on.
    pub(crate) fn stirred(&mut self) {
        self.quiet = false;
        self.idle_probes = 0;
    }

    /// How long a screen must wait before the next one is worth sending.
    pub(crate) fn repaint_interval(&self) -> Duration {
        self.srtt
            .map_or(REPAINT_CEILING.min(Duration::from_millis(33)), |srtt| {
                (srtt / 2).clamp(REPAINT_FLOOR, REPAINT_CEILING)
            })
    }

    /// How long an unanswered screen holds the next one back.
    pub(crate) fn ack_timeout(&self) -> Duration {
        self.srtt
            .map_or(REPAINT_CEILING, |srtt| srtt * 2)
            .clamp(REPAINT_CEILING, Duration::from_secs(2))
    }

    /// How often this attachment is probed, and how long the client is told it
    /// may wait for the next one. Backed off toward the ceiling while the
    /// session carries nothing: at the floor an attached session with nobody
    /// typing exchanges four probe pairs a second for ever, waking the actor,
    /// the sink and the client's reader each time, on the battery of the machine
    /// holding the terminal. mosh idles at about one.
    pub(crate) fn ping_interval(&self) -> Duration {
        let paced = self.srtt.map_or(PING_CEILING, |srtt| {
            (srtt * 2).clamp(PING_FLOOR, PING_CEILING)
        });
        (paced * (1 << self.idle_probes)).min(PING_CEILING)
    }

    /// The interval as the wire carries it. [`PING_CEILING`] is two seconds, so
    /// the saturation here is unreachable rather than a decision.
    pub(crate) fn interval_ms(&self) -> u16 {
        u16::try_from(self.ping_interval().as_millis()).unwrap_or(u16::MAX)
    }

    /// Five intervals, so a link that is merely slow is not called dead. At the
    /// idle ceiling that is ten seconds rather than five, which is what the
    /// backoff above costs: a session with nothing to say has nobody waiting on
    /// the notice, and one with something to say is back at the floor by then.
    pub(crate) fn is_dead(&self, now: Instant) -> bool {
        let deadline = (self.ping_interval() * PING_DEAD_INTERVALS).max(PING_DEAD_FLOOR);
        now.saturating_duration_since(self.last_answer) > deadline
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    /// The client's patience is derived from a number this side ships, so the
    /// pacing has to track the link and stay inside what the field can carry.
    #[test]
    fn the_probe_interval_is_paced_by_the_round_trip() {
        let mut link = LinkTiming::new();
        // Nothing measured yet: the most patient interval.
        assert_eq!(link.ping_interval(), PING_CEILING);

        link.srtt = Some(Duration::from_millis(10));
        assert_eq!(link.ping_interval(), PING_FLOOR);
        link.srtt = Some(Duration::from_millis(400));
        assert_eq!(link.ping_interval(), Duration::from_millis(800));
        assert_eq!(link.interval_ms(), 800);
        link.srtt = Some(Duration::from_secs(5));
        assert_eq!(link.ping_interval(), PING_CEILING);

        // Five intervals or the floor, whichever is longer: a LAN must not
        // call a scheduling hiccup a death, and it must not wait out twenty
        // seconds of a session that is already gone either.
        let now = Instant::now();
        link.srtt = Some(Duration::from_millis(10));
        link.last_answer = now
            .checked_sub(PING_DEAD_FLOOR / 2)
            .expect("the test clock is not at the epoch");
        assert!(!link.is_dead(now));
        link.last_answer = now
            .checked_sub(PING_DEAD_FLOOR * 2)
            .expect("the test clock is not at the epoch");
        assert!(link.is_dead(now));
    }

    /// An attached session with nobody typing is the highest-frequency periodic
    /// thing in this system at rest, and every probe of it wakes four threads.
    #[test]
    fn a_probe_answered_with_no_traffic_backs_the_next_one_off() {
        let mut link = LinkTiming::new();
        link.srtt = Some(Duration::from_millis(10));
        assert_eq!(link.ping_interval(), PING_FLOOR);

        let mut now = Instant::now();
        for expected in [
            PING_FLOOR * 2,
            PING_FLOOR * 4,
            PING_CEILING,
            // Three doublings reach the ceiling, and nothing goes past it.
            PING_CEILING,
        ] {
            let announced = link.ping_interval();
            now += announced;
            let token = link.probe(now).expect("a probe is due at its own interval");
            link.answered(token);
            assert_eq!(link.ping_interval(), expected);
            assert!(
                link.ping_interval() < announced * 3,
                "the client waits three of the interval the last `Ping` named, and the \
                 one announcing this step would arrive after that ran out"
            );
        }

        // The trade: at the ceiling an attachment that stopped answering is
        // noticed after ten seconds rather than five.
        link.last_answer = now
            .checked_sub(Duration::from_secs(9))
            .expect("the test clock is not at the epoch");
        assert!(!link.is_dead(now));
        link.last_answer = now
            .checked_sub(Duration::from_secs(11))
            .expect("the test clock is not at the epoch");
        assert!(link.is_dead(now));

        // A byte in either direction pays for the estimate again.
        link.stirred();
        assert_eq!(link.ping_interval(), PING_FLOOR);
    }
}
