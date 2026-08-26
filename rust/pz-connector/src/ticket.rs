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
    pub(crate) fn mint(&self, entry: TicketEntry) -> [u8; TICKET_LENGTH] {
        let mut ticket = [0u8; TICKET_LENGTH];
        getrandom::getrandom(&mut ticket).expect("system randomness source unavailable");
        self.entries.lock().unwrap().insert(ticket, entry);
        ticket
    }

    /// Resolves a presented ticket and removes it in the same critical section, so two connections
    /// racing on one ticket can never both be served.
    pub(crate) fn burn(&self, ticket: &[u8]) -> Option<TicketEntry> {
        let ticket: &[u8; TICKET_LENGTH] = ticket.try_into().ok()?;
        self.entries.lock().unwrap().remove(ticket)
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
}
