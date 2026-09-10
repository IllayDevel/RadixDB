use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_CLIENT_REQUEST_SEQUENCE: AtomicU64 = AtomicU64::new(1);

pub(crate) fn next_client_request_id() -> Option<u64> {
    let sequence = NEXT_CLIENT_REQUEST_SEQUENCE
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
            (current < u64::from(u32::MAX)).then_some(current + 1)
        })
        .ok()?;
    Some((u64::from(std::process::id()) << 32) | sequence)
}

#[cfg(test)]
mod tests {
    #[test]
    fn generated_request_ids_are_nonzero_and_process_scoped() {
        let first = super::next_client_request_id().expect("request id");
        let second = super::next_client_request_id().expect("request id");
        assert_ne!(first, 0);
        assert!(second > first);
        assert_eq!(first >> 32, u64::from(std::process::id()));
        assert_eq!(second >> 32, u64::from(std::process::id()));
    }
}
