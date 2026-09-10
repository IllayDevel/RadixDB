//! Canonical DATA-artifact fixtures shared by storage unit tests.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use radixdb_catalog::{CatalogDataType, ObjectId};
use radixdb_core::{Row, Schema, SchemaBuilder};

use crate::v6::{
    build_data_artifact, ArtifactDataSource, ArtifactId, CatalogGeneration, ColumnBuildPolicy,
    DataArtifactBuildRequest, DataArtifactHeader, DataColumnSpec, DataPhysicalCodec,
    DatabaseGeneration, DatabaseId, FanoutBuildLimits, SegmentId, SegmentKind, SourceRow,
    DEFAULT_SORT_RUN_BYTES, DEFAULT_SORT_RUN_RECORDS,
};

use super::writer::FrozenVolume;

pub(crate) struct ArtifactVolumeFixture {
    pub(crate) volume: Arc<FrozenVolume>,
    pub(crate) relative_path: PathBuf,
    pub(crate) absolute_path: PathBuf,
}

pub(crate) fn build_artifact_volume(
    root: &Path,
    schema: &Schema,
    segment_number: u64,
    rows: &[(i64, Row)],
) -> ArtifactVolumeFixture {
    build_artifact_volume_with_group_rows(root, schema, segment_number, rows, 65_536)
}

pub(crate) fn build_artifact_volume_with_group_rows(
    root: &Path,
    schema: &Schema,
    segment_number: u64,
    rows: &[(i64, Row)],
    row_group_rows: u32,
) -> ArtifactVolumeFixture {
    assert!(
        !rows.is_empty(),
        "a DATA artifact fixture must contain rows"
    );
    let limits = FanoutBuildLimits::new(
        row_group_rows,
        DEFAULT_SORT_RUN_RECORDS,
        DEFAULT_SORT_RUN_BYTES,
        4_096,
    )
    .expect("valid test fanout limits");
    let columns = schema
        .columns
        .iter()
        .map(|column| {
            DataColumnSpec::new(
                ObjectId::new(),
                CatalogDataType::scalar(column.data_type).expect("supported test column type"),
                column.nullable,
            )
        })
        .collect::<Vec<_>>();
    let row_count = u64::try_from(rows.len()).expect("test row count fits u64");
    let row_group_rows = limits
        .planned_row_group_rows(row_count, schema.columns.len() as u32)
        .unwrap();
    let row_group_count = row_count.div_ceil(u64::from(row_group_rows));
    let header = DataArtifactHeader::new(
        ArtifactId::new(),
        DatabaseId::new(),
        ObjectId::new(),
        SegmentId::new(),
        DatabaseGeneration::new(segment_number.max(1)).expect("nonzero generation"),
        CatalogGeneration::new(1).expect("nonzero catalog generation"),
        1,
        segment_number.max(1),
        row_count,
        u32::try_from(columns.len()).expect("test column count fits u32"),
        u32::try_from(row_group_count).expect("test row-group count fits u32"),
        SegmentKind::Rows,
        segment_number,
    )
    .expect("valid test DATA header");
    let request = DataArtifactBuildRequest::new(
        header,
        columns,
        vec![ColumnBuildPolicy::default(); schema.columns.len()],
        DataPhysicalCodec::Lz4,
        limits,
    )
    .expect("valid test DATA request");
    let source_rows = rows.iter().map(|(row_id, row)| {
        Ok(SourceRow::new(
            crate::v6::encode_runtime_row_id(*row_id),
            row.as_slice().to_vec(),
        ))
    });
    let built = build_data_artifact(&request, source_rows).expect("build test DATA artifact");
    let reference = built.data_reference();
    let relative_path = reference.relative_path();
    let absolute_path = root.join(&relative_path);
    std::fs::create_dir_all(
        absolute_path
            .parent()
            .expect("artifact path always has a parent"),
    )
    .expect("create test artifact directory");
    std::fs::write(&absolute_path, built.data_bytes()).expect("write test DATA artifact");
    let source = Arc::new(
        ArtifactDataSource::open(&absolute_path, reference).expect("open test DATA artifact"),
    );
    let volume = Arc::new(
        FrozenVolume::from_artifact_source(schema, source)
            .expect("construct artifact-backed frozen volume"),
    );
    ArtifactVolumeFixture {
        volume,
        relative_path,
        absolute_path,
    }
}

pub(crate) fn persist_eager_volume(
    root: &Path,
    segment_number: u64,
    volume: &FrozenVolume,
) -> ArtifactVolumeFixture {
    persist_eager_volume_with_group_rows(root, segment_number, volume, 65_536)
}

pub(crate) fn persist_eager_volume_with_group_rows(
    root: &Path,
    segment_number: u64,
    volume: &FrozenVolume,
    row_group_rows: u32,
) -> ArtifactVolumeFixture {
    assert!(!volume.is_cold(), "fixture source must be eager");
    let mut schema = SchemaBuilder::new("artifact_fixture");
    for (name, data_type) in volume
        .meta
        .column_names
        .iter()
        .zip(&volume.meta.column_types)
    {
        schema = schema.column(name.as_str(), *data_type, true, false);
    }
    let schema = schema.build();
    let rows = (0..volume.meta.row_count)
        .map(|index| {
            (
                volume.meta.row_ids.get(index).expect("fixture row ID"),
                volume.get_row(index),
            )
        })
        .collect::<Vec<_>>();
    build_artifact_volume_with_group_rows(root, &schema, segment_number, &rows, row_group_rows)
}
