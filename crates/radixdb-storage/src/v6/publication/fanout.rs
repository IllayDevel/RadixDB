use std::io::{Cursor, Read, Seek, Write};

use radixdb_core::Value;

use super::super::data::DataStreamWriter;
use super::super::index::write_index_artifact_stream;
use super::super::{
    DataBlockSpec, DataStatisticsSpec, FormatError, FormatResult, IndexArtifactHeader,
};
use super::diagnostics::{record as record_diagnostic, DiagnosticEvent};
use super::model::{
    invalid_data, invalid_index, ArtifactPairBuildRequest, BuiltArtifactPair, BuiltDataArtifact,
    DataArtifactBuildRequest, SourceRow, WrittenArtifactPair, WrittenDataArtifact,
};
use super::prepare::prepare_accelerators;
use super::resources::{value_payload_bytes, PublicationBuildBudget};
use super::runs::IndexRunBuilder;

/// Consume one authoritative row stream and fan every row directly into the
/// data row-group encoder and every logical accelerator's bounded sort-run
/// builder. The index side never opens or decodes the newly encoded data
/// artifact; its binding uses the layout produced by the data encoder itself.
pub fn build_artifact_pair(
    request: &ArtifactPairBuildRequest,
    rows: impl IntoIterator<Item = FormatResult<SourceRow>>,
) -> FormatResult<BuiltArtifactPair> {
    let mut data_output = Cursor::new(Vec::new());
    let mut index_output = Cursor::new(Vec::new());
    let written = write_artifact_pair(request, rows, &mut data_output, &mut index_output)?;
    let (data_reference, data_layout, index_reference) = written.into_parts();
    Ok(BuiltArtifactPair::new(
        data_output.into_inner(),
        data_reference,
        data_layout,
        index_output.into_inner(),
        index_reference,
    ))
}

/// Build authoritative DATA without manufacturing an INDEX artifact for a
/// table that has no logical accelerator.
pub fn build_data_artifact(
    request: &DataArtifactBuildRequest,
    rows: impl IntoIterator<Item = FormatResult<SourceRow>>,
) -> FormatResult<BuiltDataArtifact> {
    let mut data_output = Cursor::new(Vec::new());
    let written = write_data_artifact(request, rows, &mut data_output)?;
    let (data_reference, data_layout) = written.into_parts();
    Ok(BuiltDataArtifact::new(
        data_output.into_inner(),
        data_reference,
        data_layout,
    ))
}

pub fn write_data_artifact<D>(
    request: &DataArtifactBuildRequest,
    rows: impl IntoIterator<Item = FormatResult<SourceRow>>,
    data_output: &mut D,
) -> FormatResult<WrittenDataArtifact>
where
    D: Read + Write + Seek,
{
    record_diagnostic(DiagnosticEvent::PublicationInvocation, 1);
    let budget = PublicationBuildBudget::acquire(request.limits(), 0)?;
    let (data_reference, data_layout) = write_data_stream(
        DataStreamPlan {
            data_header: request.data_header(),
            columns: request.columns(),
            policies: request.column_policies(),
            row_id_codec: request.row_id_codec(),
            limits: request.limits(),
            budget: &budget,
        },
        rows,
        data_output,
    )?;
    Ok(WrittenDataArtifact::new(data_reference, data_layout))
}

/// Consume one source stream and write both artifacts into caller-owned empty
/// seekable destinations. The data path writes each row-group payload directly
/// to its final offset; index sort runs are independent of that output.
pub fn write_artifact_pair<D, I>(
    request: &ArtifactPairBuildRequest,
    rows: impl IntoIterator<Item = FormatResult<SourceRow>>,
    data_output: &mut D,
    index_output: &mut I,
) -> FormatResult<WrittenArtifactPair>
where
    D: Read + Write + Seek,
    I: Read + Write + Seek,
{
    record_diagnostic(DiagnosticEvent::PublicationInvocation, 1);
    validate_staging_directory(request)?;
    let budget = PublicationBuildBudget::acquire(request.limits(), request.accelerators().len())?;
    let mut index_builders = request
        .accelerators()
        .iter()
        .cloned()
        .enumerate()
        .map(|(ordinal, spec)| {
            IndexRunBuilder::new(
                spec,
                request.columns(),
                request.limits(),
                request.staging_directory(),
                ordinal,
                budget.accelerator_resident_bytes(),
                budget.spill(),
            )
        })
        .collect::<FormatResult<Vec<_>>>()?;

    let (data_reference, data_layout) = write_data_stream_with_observer(
        DataStreamPlan {
            data_header: request.data_header(),
            columns: request.columns(),
            policies: request.column_policies(),
            row_id_codec: request.row_id_codec(),
            limits: request.limits(),
            budget: &budget,
        },
        rows,
        data_output,
        |values, row_ordinal| {
            for builder in &mut index_builders {
                builder.visit(values, row_ordinal)?;
            }
            Ok(())
        },
    )?;

    let source_row_count = request.data_header().row_count();
    let accelerators = prepare_accelerators(index_builders, source_row_count, request.limits())?;
    let index_header = IndexArtifactHeader::for_data(
        request.index_artifact_id(),
        request.data_header().creation_generation(),
        request.data_header().catalog_generation(),
        &data_layout,
    )?;
    let index_reference = write_index_artifact_stream(
        index_output,
        index_header,
        &accelerators,
        budget.index_metadata_bytes(),
    )?;
    Ok(WrittenArtifactPair::new(
        data_reference,
        data_layout,
        index_reference,
    ))
}

#[derive(Clone, Copy)]
struct DataStreamPlan<'a> {
    data_header: super::super::DataArtifactHeader,
    columns: &'a [super::super::DataColumnSpec],
    policies: &'a [super::model::ColumnBuildPolicy],
    row_id_codec: super::super::DataPhysicalCodec,
    limits: super::model::FanoutBuildLimits,
    budget: &'a PublicationBuildBudget,
}

#[derive(Clone, Copy)]
struct RowEncodingPlan<'a> {
    budget: &'a PublicationBuildBudget,
    column_specs: &'a [super::super::DataColumnSpec],
    column_policies: &'a [super::model::ColumnBuildPolicy],
    row_id_codec: super::super::DataPhysicalCodec,
}

fn write_data_stream<D>(
    plan: DataStreamPlan<'_>,
    rows: impl IntoIterator<Item = FormatResult<SourceRow>>,
    data_output: &mut D,
) -> FormatResult<(super::super::ArtifactRef, super::super::DataArtifactLayout)>
where
    D: Read + Write + Seek,
{
    write_data_stream_with_observer(plan, rows, data_output, |_, _| Ok(()))
}

fn write_data_stream_with_observer<D, O>(
    plan: DataStreamPlan<'_>,
    rows: impl IntoIterator<Item = FormatResult<SourceRow>>,
    data_output: &mut D,
    mut observe_row: O,
) -> FormatResult<(super::super::ArtifactRef, super::super::DataArtifactLayout)>
where
    D: Read + Write + Seek,
    O: FnMut(&[Value], u64) -> FormatResult<()>,
{
    let DataStreamPlan {
        data_header,
        columns,
        policies,
        row_id_codec,
        limits,
        budget,
    } = plan;
    let row_encoding = RowEncodingPlan {
        budget,
        column_specs: columns,
        column_policies: policies,
        row_id_codec,
    };
    let group_capacity = usize::try_from(
        limits.planned_row_group_rows(
            data_header.row_count(),
            u32::try_from(columns.len())
                .map_err(|_| invalid_data("fanout column count does not fit u32"))?,
        )?,
    )
    .map_err(|_| invalid_data("fanout row-group capacity does not fit usize"))?;
    let mut group_row_ids = Vec::with_capacity(group_capacity);
    let mut group_values = columns
        .iter()
        .map(|_| Vec::with_capacity(group_capacity))
        .collect::<Vec<Vec<Value>>>();
    let bloom_column_count = policies
        .iter()
        .filter(|policy| policy.bloom().is_some())
        .count();
    let mut data_writer = DataStreamWriter::new(
        data_output,
        data_header,
        columns,
        bloom_column_count,
        budget.data_metadata_bytes(),
    )?;
    let mut source_row_count = 0_u64;
    let mut previous_row_id = None;
    let mut group_ordinal = 0_u32;
    let mut group_payload_bytes = 0_u64;
    record_diagnostic(DiagnosticEvent::SourceStreamPass, 1);

    for row in rows {
        let row = row?;
        record_diagnostic(DiagnosticEvent::SourceRow, 1);
        if source_row_count >= data_header.row_count() {
            return Err(invalid_data(
                "source stream contains more rows than its data header",
            ));
        }
        if row.values().len() != columns.len() {
            return Err(invalid_data(
                "source row width differs from the table columns",
            ));
        }
        if previous_row_id.is_some_and(|previous| previous >= row.row_id()) {
            return Err(invalid_data("source row IDs are not strictly increasing"));
        }
        let row_payload_bytes = value_payload_bytes(row.values())?;
        let next_payload_bytes = group_payload_bytes
            .checked_add(row_payload_bytes)
            .ok_or_else(|| invalid_data("row-group variable resident bytes overflow"))?;
        if next_payload_bytes > budget.variable_group_bytes() {
            return Err(FormatError::DataArtifactLimitExceeded {
                field: "row-group variable resident bytes",
                actual: next_payload_bytes,
                limit: budget.variable_group_bytes(),
            });
        }
        observe_row(row.values(), source_row_count)?;
        let (row_id, values) = row.into_parts();
        group_row_ids.push(row_id);
        for (column, value) in group_values.iter_mut().zip(values) {
            column.push(value);
        }
        previous_row_id = Some(row_id);
        source_row_count += 1;
        group_payload_bytes = next_payload_bytes;

        if group_row_ids.len() == group_capacity {
            write_encoded_row_group(
                &mut data_writer,
                &row_encoding,
                group_ordinal,
                &group_row_ids,
                &group_values,
            )?;
            clear_group(&mut group_row_ids, &mut group_values);
            group_payload_bytes = 0;
            group_ordinal = group_ordinal
                .checked_add(1)
                .ok_or_else(|| invalid_data("row-group ordinal overflows"))?;
        }
    }
    if !group_row_ids.is_empty() {
        write_encoded_row_group(
            &mut data_writer,
            &row_encoding,
            group_ordinal,
            &group_row_ids,
            &group_values,
        )?;
        clear_group(&mut group_row_ids, &mut group_values);
        group_ordinal = group_ordinal
            .checked_add(1)
            .ok_or_else(|| invalid_data("row-group ordinal overflows"))?;
    }
    if source_row_count != data_header.row_count()
        || u64::from(group_ordinal) != u64::from(data_header.row_group_count())
    {
        return Err(invalid_data(
            "source stream row/group count differs from its data header",
        ));
    }

    data_writer.finish()
}

fn write_encoded_row_group<D>(
    writer: &mut DataStreamWriter<'_, D>,
    plan: &RowEncodingPlan<'_>,
    group_ordinal: u32,
    row_ids: &[u64],
    columns: &[Vec<Value>],
) -> FormatResult<()>
where
    D: Read + Write + Seek,
{
    let RowEncodingPlan {
        budget,
        column_specs,
        column_policies,
        row_id_codec,
    } = *plan;
    if row_ids.is_empty() || columns.iter().any(|column| column.len() != row_ids.len()) {
        return Err(invalid_data("fanout row-group buffers are inconsistent"));
    }
    budget.admit_row_id_block(row_ids.len())?;
    for ((column_spec, policy), values) in column_specs
        .iter()
        .copied()
        .zip(column_policies.iter().copied())
        .zip(columns.iter())
    {
        budget.admit_column_block(column_spec, values, policy.value_encoding())?;
        if let Some(config) = policy.bloom() {
            budget.admit_bloom_block(values, config)?;
        }
    }
    let mut statistics = Vec::with_capacity(column_specs.len());
    for (column_ordinal, (column_spec, values)) in
        column_specs.iter().copied().zip(columns.iter()).enumerate()
    {
        statistics.push(DataStatisticsSpec::from_validated_values(
            column_ordinal as u32,
            group_ordinal,
            column_spec,
            values,
            None,
        )?);
    }
    let row_id_blocks =
        std::iter::once_with(|| DataBlockSpec::row_ids(group_ordinal, row_ids, row_id_codec));
    let column_blocks = column_specs
        .iter()
        .copied()
        .zip(column_policies.iter().copied())
        .zip(columns.iter())
        .enumerate()
        .map(|(column_ordinal, ((column_spec, policy), values))| {
            DataBlockSpec::column(
                group_ordinal,
                column_ordinal as u32,
                column_spec,
                values,
                policy.value_encoding(),
                policy.physical_codec(),
            )
        });
    let bloom_blocks = column_specs
        .iter()
        .copied()
        .zip(column_policies.iter().copied())
        .zip(columns.iter())
        .enumerate()
        .filter_map(|(column_ordinal, ((column_spec, policy), values))| {
            policy.bloom().map(|config| {
                DataBlockSpec::bloom(
                    group_ordinal,
                    column_ordinal as u32,
                    column_spec,
                    values,
                    config,
                    policy.physical_codec(),
                )
            })
        });
    writer.write_row_group(
        group_ordinal,
        row_ids,
        row_id_blocks.chain(column_blocks).chain(bloom_blocks),
        statistics,
    )
}

fn clear_group(row_ids: &mut Vec<u64>, columns: &mut [Vec<Value>]) {
    row_ids.clear();
    for column in columns {
        column.clear();
    }
}

fn validate_staging_directory(request: &ArtifactPairBuildRequest) -> FormatResult<()> {
    let metadata = std::fs::metadata(request.staging_directory()).map_err(|error| {
        FormatError::ArtifactIo {
            operation: "inspect fanout staging directory",
            kind: error.kind(),
        }
    })?;
    if !metadata.is_dir() {
        return Err(invalid_index("fanout staging path is not a directory"));
    }
    Ok(())
}
