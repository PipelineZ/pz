//! Remote statistics a connector reports for the write session it is serving: request count, wire
//! bytes, time spent waiting on the remote system, throttling, billed cost and a few connector-specific
//! extras. [`stats`] returns the handle for the session the calling task runs in; outside one it is a
//! no-op, so connector code can report unconditionally.
//!
//! Values are validated rather than trusted: an invalid one is dropped (never a panic, never an error
//! to the connector), and the rule that fired is remembered so tests can see it. The rules are the
//! host's own: values finite and non-negative; a cost unit matching `[a-z][a-z0-9_]{0,23}` with the
//! first unit reported winning; extra names matching `[a-z][a-z0-9_]*\.[a-z][a-z0-9_.]*` up to 40
//! characters, at most 8 distinct names (names already present keep summing).

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::atomic::{AtomicI64, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use crate::pb;

const MAX_EXTRAS: usize = 8;
const MAX_EXTRA_NAME_LEN: usize = 40;
const MAX_COST_UNIT_LEN: usize = 24;

/// Spans a session may open for remote calls; further calls still count as requests.
pub(crate) const SPAN_BUDGET: u32 = 200;

pub(crate) const DROP_NEGATIVE_OR_NON_FINITE: u32 = 1;
pub(crate) const DROP_INVALID_COST_UNIT: u32 = 2;
pub(crate) const DROP_SECOND_COST_UNIT: u32 = 4;
pub(crate) const DROP_INVALID_EXTRA_NAME: u32 = 8;
pub(crate) const DROP_TOO_MANY_EXTRAS: u32 = 16;

tokio::task_local! {
    static STATS: Stats;
}

#[derive(Default)]
struct Locked {
    cost_units: f64,
    cost_unit: Option<String>,
    extras: BTreeMap<String, f64>,
}

#[derive(Default)]
struct Inner {
    requests: AtomicI64,
    wire_bytes: AtomicI64,
    remote_ms: AtomicI64,
    throttled: AtomicI64,
    spans_dropped: AtomicI64,
    spans_opened: AtomicU32,
    drops: AtomicU32,
    locked: Mutex<Locked>,
}

/// A handle on one write session's remote statistics. Cloning shares the same counters.
#[derive(Clone)]
pub struct Stats {
    inner: Option<Arc<Inner>>,
}

/// The statistics handle for the write session the calling task runs in, or a no-op handle outside
/// one. Reporting through it needs no setup and never fails.
#[must_use]
pub fn stats() -> Stats {
    STATS
        .try_with(Stats::clone)
        .unwrap_or(Stats { inner: None })
}

/// Runs `f` with `stats` as the ambient handle [`stats`] returns, across every `.await` inside it.
pub(crate) fn scope<F: Future>(stats: Stats, f: F) -> impl Future<Output = F::Output> {
    STATS.scope(stats, f)
}

impl Stats {
    /// A fresh, empty, live handle (the SDK makes one per write session).
    pub(crate) fn new() -> Self {
        Stats {
            inner: Some(Arc::new(Inner::default())),
        }
    }

    /// Adds requests made to the remote system.
    pub fn add_requests(&self, n: i64) {
        self.add_counter(|i| &i.requests, n);
    }

    /// Adds bytes sent or received on the wire.
    pub fn add_wire_bytes(&self, n: i64) {
        self.add_counter(|i| &i.wire_bytes, n);
    }

    /// Adds milliseconds spent waiting on the remote system.
    pub fn add_remote_ms(&self, ms: i64) {
        self.add_counter(|i| &i.remote_ms, ms);
    }

    /// Adds requests the remote system throttled.
    pub fn add_throttled(&self, n: i64) {
        self.add_counter(|i| &i.throttled, n);
    }

    pub(crate) fn add_spans_dropped(&self, n: i64) {
        self.add_counter(|i| &i.spans_dropped, n);
    }

    /// Adds billed cost in `unit` (lowercase, at most 24 characters, such as `ru`). The first unit
    /// reported wins; amounts in another unit are dropped.
    pub fn add_cost(&self, units: f64, unit: &str) {
        let Some(inner) = &self.inner else { return };
        if !units.is_finite() || units < 0.0 {
            inner.drop_rule(DROP_NEGATIVE_OR_NON_FINITE);
            return;
        }
        if !is_valid_cost_unit(unit) {
            inner.drop_rule(DROP_INVALID_COST_UNIT);
            return;
        }
        let mut locked = inner.locked.lock().unwrap_or_else(|e| e.into_inner());
        match &locked.cost_unit {
            Some(existing) if existing != unit => inner.drop_rule(DROP_SECOND_COST_UNIT),
            _ => {
                locked.cost_unit = Some(unit.to_string());
                locked.cost_units += units;
            }
        }
    }

    /// Adds a connector-specific number named `vendor.metric` (lowercase, dotted, at most 40
    /// characters). At most 8 distinct names are kept.
    pub fn add_extra(&self, name: &str, value: f64) {
        let Some(inner) = &self.inner else { return };
        if !value.is_finite() || value < 0.0 {
            inner.drop_rule(DROP_NEGATIVE_OR_NON_FINITE);
            return;
        }
        if !is_valid_extra_name(name) {
            inner.drop_rule(DROP_INVALID_EXTRA_NAME);
            return;
        }
        let mut locked = inner.locked.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(current) = locked.extras.get_mut(name) {
            *current += value;
        } else if locked.extras.len() >= MAX_EXTRAS {
            inner.drop_rule(DROP_TOO_MANY_EXTRAS);
        } else {
            locked.extras.insert(name.to_string(), value);
        }
    }

    /// Takes one span from the session's budget; false once it is spent.
    pub(crate) fn try_take_span(&self) -> bool {
        let Some(inner) = &self.inner else {
            return true;
        };
        inner
            .spans_opened
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                (n < SPAN_BUDGET).then_some(n + 1)
            })
            .is_ok()
    }

    /// The wire message for what was reported, or `None` when nothing was.
    pub(crate) fn to_proto(&self) -> Option<pb::ConnectorStats> {
        let inner = self.inner.as_ref()?;
        let locked = inner.locked.lock().unwrap_or_else(|e| e.into_inner());
        let proto = pb::ConnectorStats {
            requests: inner.requests.load(Ordering::Relaxed),
            wire_bytes: inner.wire_bytes.load(Ordering::Relaxed),
            remote_ms: inner.remote_ms.load(Ordering::Relaxed),
            throttled: inner.throttled.load(Ordering::Relaxed),
            cost_units: if locked.cost_unit.is_some() {
                locked.cost_units
            } else {
                0.0
            },
            cost_unit: locked.cost_unit.clone().unwrap_or_default(),
            extras: locked.extras.iter().map(|(k, v)| (k.clone(), *v)).collect(),
            spans_dropped: inner.spans_dropped.load(Ordering::Relaxed),
        };
        let empty = proto.requests == 0
            && proto.wire_bytes == 0
            && proto.remote_ms == 0
            && proto.throttled == 0
            && proto.cost_unit.is_empty()
            && proto.extras.is_empty()
            && proto.spans_dropped == 0;
        (!empty).then_some(proto)
    }

    #[cfg(test)]
    pub(crate) fn drops(&self) -> u32 {
        self.inner
            .as_ref()
            .map_or(0, |i| i.drops.load(Ordering::Relaxed))
    }

    fn add_counter(&self, field: fn(&Inner) -> &AtomicI64, n: i64) {
        let Some(inner) = &self.inner else { return };
        if n < 0 {
            inner.drop_rule(DROP_NEGATIVE_OR_NON_FINITE);
            return;
        }
        field(inner).fetch_add(n, Ordering::Relaxed);
    }
}

impl Inner {
    fn drop_rule(&self, rule: u32) {
        self.drops.fetch_or(rule, Ordering::Relaxed);
    }
}

fn is_lower(c: char) -> bool {
    c.is_ascii_lowercase()
}

fn is_word(c: char) -> bool {
    c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_'
}

/// `^[a-z][a-z0-9_]*$`, 1 to 24 characters.
fn is_valid_cost_unit(unit: &str) -> bool {
    let mut chars = unit.chars();
    unit.len() <= MAX_COST_UNIT_LEN && chars.next().is_some_and(is_lower) && chars.all(is_word)
}

/// `^[a-z][a-z0-9_]*\.[a-z][a-z0-9_.]*$`, at most 40 characters.
fn is_valid_extra_name(name: &str) -> bool {
    if name.len() > MAX_EXTRA_NAME_LEN || !name.starts_with(is_lower) {
        return false;
    }
    let Some((head, tail)) = name.split_once('.') else {
        return false;
    };
    head.chars().all(is_word)
        && tail.starts_with(is_lower)
        && tail.chars().all(|c| is_word(c) || c == '.')
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snapshot(s: &Stats) -> pb::ConnectorStats {
        s.to_proto().expect("non-empty")
    }

    #[test]
    fn empty_stats_have_no_proto() {
        assert!(Stats::new().to_proto().is_none());
    }

    #[test]
    fn counters_sum_and_negatives_are_dropped() {
        let s = Stats::new();
        s.add_requests(2);
        s.add_requests(3);
        s.add_wire_bytes(10);
        s.add_remote_ms(7);
        s.add_throttled(1);
        s.add_requests(-1);
        let p = snapshot(&s);
        assert_eq!(
            (p.requests, p.wire_bytes, p.remote_ms, p.throttled),
            (5, 10, 7, 1)
        );
        assert_eq!(s.drops(), DROP_NEGATIVE_OR_NON_FINITE);
    }

    #[test]
    fn non_finite_or_negative_values_are_dropped() {
        let s = Stats::new();
        s.add_cost(f64::NAN, "ru");
        s.add_cost(-1.0, "ru");
        s.add_extra("a.b", f64::INFINITY);
        s.add_extra("a.b", -0.5);
        assert!(s.to_proto().is_none());
        assert_eq!(s.drops(), DROP_NEGATIVE_OR_NON_FINITE);
    }

    #[test]
    fn invalid_cost_units_are_dropped() {
        for bad in ["", "RU", "1ru", "r-u", "r u", &"a".repeat(25)] {
            let s = Stats::new();
            s.add_cost(1.0, bad);
            assert!(s.to_proto().is_none(), "{bad}");
            assert_eq!(s.drops(), DROP_INVALID_COST_UNIT, "{bad}");
        }
        let s = Stats::new();
        s.add_cost(1.0, &"a".repeat(24));
        assert!(s.to_proto().is_some());
    }

    #[test]
    fn first_cost_unit_wins() {
        let s = Stats::new();
        s.add_cost(1.5, "ru");
        s.add_cost(2.0, "ru");
        s.add_cost(9.0, "bytes_billed");
        let p = snapshot(&s);
        assert_eq!((p.cost_units, p.cost_unit.as_str()), (3.5, "ru"));
        assert_eq!(s.drops(), DROP_SECOND_COST_UNIT);
    }

    #[test]
    fn zero_cost_still_reports_its_unit() {
        let s = Stats::new();
        s.add_cost(0.0, "ru");
        assert_eq!(snapshot(&s).cost_unit, "ru");
    }

    #[test]
    fn invalid_extra_names_are_dropped() {
        let long = format!("a.{}", "b".repeat(39));
        for bad in [
            "", "nodot", ".a", "a.", "A.b", "a.B", "1a.b", "a-b.c", "a.b-c", "a..b", &long,
        ] {
            let s = Stats::new();
            s.add_extra(bad, 1.0);
            assert!(s.to_proto().is_none(), "{bad}");
            assert_eq!(s.drops(), DROP_INVALID_EXTRA_NAME, "{bad}");
        }
        for good in ["a.b", "vendor.metric_1", "a.b.c", "a_1.b.c_2"] {
            let s = Stats::new();
            s.add_extra(good, 1.0);
            assert!(s.to_proto().is_some(), "{good}");
            assert_eq!(s.drops(), 0, "{good}");
        }
        let exactly_forty = format!("a.{}", "b".repeat(38));
        let s = Stats::new();
        s.add_extra(&exactly_forty, 1.0);
        assert!(s.to_proto().is_some());
    }

    #[test]
    fn nine_extras_keep_eight_and_existing_names_keep_summing() {
        let s = Stats::new();
        for i in 0..9 {
            s.add_extra(&format!("v.m{i}"), 1.0);
        }
        s.add_extra("v.m0", 2.0);
        let p = snapshot(&s);
        assert_eq!(p.extras.len(), 8);
        assert_eq!(p.extras["v.m0"], 3.0);
        assert!(!p.extras.contains_key("v.m8"));
        assert_eq!(s.drops(), DROP_TOO_MANY_EXTRAS);
    }

    #[test]
    fn spans_dropped_is_reported() {
        let s = Stats::new();
        s.add_spans_dropped(3);
        assert_eq!(snapshot(&s).spans_dropped, 3);
    }

    #[test]
    fn outside_a_scope_the_handle_is_a_no_op() {
        let s = stats();
        s.add_requests(5);
        s.add_cost(1.0, "ru");
        assert!(s.to_proto().is_none());
    }

    #[tokio::test]
    async fn the_scope_flows_across_await_points() {
        let s = Stats::new();
        scope(s.clone(), async {
            stats().add_requests(1);
            tokio::task::yield_now().await;
            stats().add_requests(1);
        })
        .await;
        assert_eq!(snapshot(&s).requests, 2);
    }
}
