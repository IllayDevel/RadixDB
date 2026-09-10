use crate::protocol::{BuildIdentity, PROTOCOL_VERSION};

use crate::common::version::{BUILD_PROFILE, BUILD_TARGET, CARGO_LOCK_SHA256, GIT_COMMIT, VERSION};

pub fn build_identity() -> BuildIdentity {
    BuildIdentity {
        semantic_version: VERSION.to_string(),
        git_revision: GIT_COMMIT.to_string(),
        protocol_version: PROTOCOL_VERSION,
        build_profile: BUILD_PROFILE.to_string(),
        target: BUILD_TARGET.to_string(),
    }
}

pub fn version_line(process_name: &str) -> String {
    let identity = build_identity();
    format!(
        "{process_name} {} git={} protocol={} profile={} target={} lock={}",
        identity.semantic_version,
        identity.git_revision,
        identity.protocol_version,
        identity.build_profile,
        identity.target,
        CARGO_LOCK_SHA256,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn is_source_identity(value: &str) -> bool {
        let revision = value.strip_suffix("-dirty").unwrap_or(value);
        (revision.len() == 40 && revision.bytes().all(|byte| byte.is_ascii_hexdigit()))
            || revision.starts_with("source-")
    }

    #[test]
    fn identity_uses_full_build_metadata() {
        let identity = build_identity();
        assert_eq!(identity.semantic_version, env!("CARGO_PKG_VERSION"));
        assert_eq!(identity.protocol_version, PROTOCOL_VERSION);
        assert!(!identity.build_profile.is_empty());
        assert!(!identity.target.is_empty());
        assert!(is_source_identity(&identity.git_revision));
        assert_eq!(CARGO_LOCK_SHA256.len(), 64);
        assert!(CARGO_LOCK_SHA256
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit()));
    }
}
