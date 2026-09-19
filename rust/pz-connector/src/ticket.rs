use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Fixed v1 protocol constant, mirrored from `Pz.Connectors.Protocol.ProtocolConstants.TicketLength`.
pub(crate) const TICKET_LENGTH: usize = 16;

/// What a minted data-plane ticket authorizes. v1 of this crate is sink-first, so the only entry a
/// connector author's data ever reaches through is a write session; a `ReadTicket` twin lands when
/// source support does.
#[derive(Clone)]
pub(crate) enum TicketEntry {
    Write(Arc<crate::server::SessionState>),
}

/// Mints and burns the single-use data-plane tickets: 16 cryptographically random bytes, valid for
/// exactly one connection. The registry removes an entry the moment it is presented, so an unknown
/// ticket and a replayed one are indistinguishable to a caller -- both mean "nothing to serve here",
/// which is what makes a single `TryBurn` check enough for the data plane to reject both cases the same
/// way (close the connection without writing a byte).
#[derive(Default)]
pub(crate) struct TicketRegistry {
    entries: Mutex<HashMap<[u8; TICKET_LENGTH], TicketEntry>>,
}

impl TicketRegistry {
    /// 16 cryptographically random bytes, not yet registered anywhere. Split out from
    /// [`insert`](Self::insert) so a caller that needs the ticket value embedded in the entry it is
    /// about to register (`SessionState` records its own ticket so an abort can revoke it) can
    /// generate it first and build that entry around it.
    pub(crate) fn generate() -> [u8; TICKET_LENGTH] {
        let mut ticket = [0u8; TICKET_LENGTH];
        getrandom::getrandom(&mut ticket).expect("system randomness source unavailable");
        ticket
    }

    pub(crate) fn insert(&self, ticket: [u8; TICKET_LENGTH], entry: TicketEntry) {
        self.entries.lock().unwrap().insert(ticket, entry);
    }

    #[cfg(test)]
    pub(crate) fn mint(&self, entry: TicketEntry) -> [u8; TICKET_LENGTH] {
        let ticket = Self::generate();
        self.insert(ticket, entry);
        ticket
    }

    /// Resolves a presented ticket and removes it in the same critical section, so two connections
    /// racing on one ticket can never both be served.
    pub(crate) fn burn(&self, ticket: &[u8]) -> Option<TicketEntry> {
        let ticket: &[u8; TICKET_LENGTH] = ticket.try_into().ok()?;
        self.entries.lock().unwrap().remove(ticket)
    }

    /// Removes a ticket whether or not it was ever presented -- the counterpart to a session being
    /// aborted through the control plane before its data connection burned the ticket itself. Without
    /// this an aborted session whose data connection never opened leaves its ticket live forever: a
    /// later connection presenting it would resolve the very `SessionState` that was already taken
    /// apart. A no-op when the ticket was already burned.
    ///
    /// Only an abort may do this. A commit has to leave the ticket alone: the data connection it is
    /// waiting for may not have been accepted yet, and turning that connection away leaves the commit
    /// waiting for a drain nothing can signal any more.
    pub(crate) fn revoke(&self, ticket: &[u8; TICKET_LENGTH]) {
        self.entries.lock().unwrap().remove(ticket);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::SessionState;

    fn dummy_entry() -> TicketEntry {
        TicketEntry::Write(SessionState::new_for_test())
    }

    #[test]
    fn a_minted_ticket_is_sixteen_random_bytes() {
        let registry = TicketRegistry::default();
        let a = registry.mint(dummy_entry());
        let b = registry.mint(dummy_entry());
        assert_eq!(a.len(), TICKET_LENGTH);
        assert_ne!(a, b, "two mints must not collide in a single test run");
    }

    #[test]
    fn a_ticket_is_single_use() {
        let registry = TicketRegistry::default();
        let ticket = registry.mint(dummy_entry());

        assert!(
            registry.burn(&ticket).is_some(),
            "first burn resolves the ticket"
        );
        assert!(
            registry.burn(&ticket).is_none(),
            "a burned ticket is never resolved again"
        );
    }

    #[test]
    fn an_unminted_ticket_is_rejected() {
        let registry = TicketRegistry::default();
        let unknown = [7u8; TICKET_LENGTH];
        assert!(registry.burn(&unknown).is_none());
    }

    #[test]
    fn a_malformed_length_ticket_is_rejected() {
        let registry = TicketRegistry::default();
        assert!(registry.burn(&[1, 2, 3]).is_none());
    }

    /// The mechanism `abort_write` uses to close the "session aborted, ticket never presented" window:
    /// a ticket revoked before its data connection ever arrived must never resolve afterward, exactly
    /// as if it had been burned.
    #[test]
    fn a_revoked_ticket_is_refused_even_if_never_burned() {
        let registry = TicketRegistry::default();
        let ticket = TicketRegistry::generate();
        registry.insert(ticket, dummy_entry());

        registry.revoke(&ticket);

        assert!(
            registry.burn(&ticket).is_none(),
            "a revoked ticket must never resolve, the same as an already-burned one"
        );
    }

    #[test]
    fn revoking_an_already_burned_ticket_is_a_harmless_no_op() {
        let registry = TicketRegistry::default();
        let ticket = registry.mint(dummy_entry());
        assert!(registry.burn(&ticket).is_some());

        // Does not panic; the common case is that the data connection already burned the ticket
        // before commit/abort ever runs.
        registry.revoke(&ticket);
    }
}
