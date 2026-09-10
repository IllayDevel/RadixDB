use std::path::Path;

use super::publication::filesystem::source::scrub_body_identity;
use super::{ArtifactKind, FormatError, FormatResult, PhysicalGenerationSnapshot};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ArtifactScrubReport {
    data_artifacts: u64,
    index_artifacts: u64,
    body_bytes: u64,
}

impl ArtifactScrubReport {
    pub const fn data_artifacts(self) -> u64 {
        self.data_artifacts
    }

    pub const fn index_artifacts(self) -> u64 {
        self.index_artifacts
    }

    pub const fn body_bytes(self) -> u64 {
        self.body_bytes
    }

    fn record(&mut self, kind: ArtifactKind, body_bytes: u64) -> FormatResult<()> {
        let counter = match kind {
            ArtifactKind::Data => &mut self.data_artifacts,
            ArtifactKind::Index => &mut self.index_artifacts,
        };
        *counter = counter
            .checked_add(1)
            .ok_or(FormatError::InvalidPublication {
                detail: "artifact scrub count overflows",
            })?;
        self.body_bytes =
            self.body_bytes
                .checked_add(body_bytes)
                .ok_or(FormatError::InvalidPublication {
                    detail: "artifact scrub byte count overflows",
                })?;
        Ok(())
    }
}

/// Recompute the immutable whole-body identity of every artifact referenced by
/// one already validated physical generation.
///
/// Normal recovery and retained-artifact publication intentionally perform a
/// metadata-only open. Payload block/page checks remain lazy, while this
/// explicit operation provides the bounded-memory full validation path.
pub fn scrub_generation_artifacts(
    root: impl AsRef<Path>,
    generation: &PhysicalGenerationSnapshot,
) -> FormatResult<ArtifactScrubReport> {
    let root = root.as_ref();
    let mut report = ArtifactScrubReport::default();
    for reference in generation.artifact_references() {
        let body_bytes = scrub_body_identity(&root.join(reference.relative_path()), reference)?;
        report.record(reference.kind(), body_bytes)?;
    }
    Ok(report)
}
