use super::RecoveryLimits;

#[test]
fn default_limits_use_the_closed_format_ceilings() {
    let limits = RecoveryLimits::default();
    assert_eq!(
        limits.reachability(),
        crate::v6::ReachabilityLimits::default()
    );
    assert_eq!(
        limits.catalog_wal(),
        crate::v6::CatalogWalReplayLimits::hard()
    );
}
