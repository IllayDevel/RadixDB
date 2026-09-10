#![cfg(unix)]

use std::os::unix::process::CommandExt;
use std::process::Command;

use radixdb_catalog::{CatalogDataType, ObjectId};
use radixdb_core::{DataType, Value};
use radixdb_storage::v6::*;

fn object_id(marker: u8) -> ObjectId {
    ObjectId::from_user_bytes([marker; 16]).unwrap()
}

fn request(staging: &std::path::Path) -> ArtifactPairBuildRequest {
    let column = DataColumnSpec::new(
        object_id(0x21),
        CatalogDataType::scalar(DataType::Integer).unwrap(),
        false,
    );
    let header = DataArtifactHeader::new(
        ArtifactId::from_bytes([0x31; 16]).unwrap(),
        DatabaseId::from_bytes([0x32; 16]).unwrap(),
        object_id(0x33),
        SegmentId::from_bytes([0x34; 16]).unwrap(),
        DatabaseGeneration::new(17).unwrap(),
        CatalogGeneration::new(13).unwrap(),
        501,
        509,
        1_024,
        1,
        1,
        SegmentKind::Rows,
        123_456,
    )
    .unwrap();
    let accelerators = (0_u8..4)
        .map(|ordinal| {
            AcceleratorBuildSpec::exact(
                object_id(0x41 + ordinal),
                false,
                false,
                [0x51 + ordinal; 32],
                vec![IndexKeyColumn::new(
                    column.column_id(),
                    DataType::Integer,
                    IndexSortDirection::Ascending,
                    IndexNullsOrder::Last,
                )],
                IndexPageCodec::Lz4,
                ExactPageBuildLimits::default(),
            )
            .unwrap()
        })
        .collect();
    ArtifactPairBuildRequest::new(
        header,
        vec![column],
        vec![ColumnBuildPolicy::default()],
        DataPhysicalCodec::Lz4,
        ArtifactId::from_bytes([0x35; 16]).unwrap(),
        accelerators,
        FanoutBuildLimits::new(1_024, 8, 1024 * 1024, 4_096)
            .unwrap()
            .with_merge_fan_in(4)
            .unwrap(),
        staging,
    )
    .unwrap()
}

fn wide_request(staging: &std::path::Path) -> ArtifactPairBuildRequest {
    let columns = (0_u32..4_096)
        .map(|ordinal| {
            DataColumnSpec::new(
                ObjectId::from_user_bytes((u128::from(ordinal) + 1).to_le_bytes()).unwrap(),
                CatalogDataType::scalar(DataType::Integer).unwrap(),
                false,
            )
        })
        .collect::<Vec<_>>();
    let header = DataArtifactHeader::new(
        ArtifactId::from_bytes([0x61; 16]).unwrap(),
        DatabaseId::from_bytes([0x62; 16]).unwrap(),
        object_id(0x63),
        SegmentId::from_bytes([0x64; 16]).unwrap(),
        DatabaseGeneration::new(17).unwrap(),
        CatalogGeneration::new(13).unwrap(),
        501,
        509,
        1,
        columns.len() as u32,
        1,
        SegmentKind::Rows,
        123_456,
    )
    .unwrap();
    let accelerator = AcceleratorBuildSpec::exact(
        object_id(0x65),
        true,
        false,
        [0x66; 32],
        vec![IndexKeyColumn::new(
            columns[0].column_id(),
            DataType::Integer,
            IndexSortDirection::Ascending,
            IndexNullsOrder::Last,
        )],
        IndexPageCodec::Lz4,
        ExactPageBuildLimits::default(),
    )
    .unwrap();
    ArtifactPairBuildRequest::new(
        header,
        columns,
        vec![ColumnBuildPolicy::default(); 4_096],
        DataPhysicalCodec::Lz4,
        ArtifactId::from_bytes([0x67; 16]).unwrap(),
        vec![accelerator],
        FanoutBuildLimits::default(),
        staging,
    )
    .unwrap()
}

#[test]
#[ignore = "isolated child entrypoint for the low-file-descriptor review gate"]
fn low_file_descriptor_build_child() {
    assert_eq!(
        std::env::var("RADIXDB_LOW_FD_REVIEW_CHILD").as_deref(),
        Ok("1"),
        "child entrypoint must only run through its parent"
    );
    let workers = (0..2)
        .map(|_| {
            std::thread::spawn(|| {
                let staging = tempfile::tempdir().unwrap();
                let result = build_artifact_pair(
                    &request(staging.path()),
                    (0..1_024)
                        .map(|row| Ok(SourceRow::new(row + 1, vec![Value::integer(row as i64)]))),
                );
                assert!(
                    result.is_ok(),
                    "valid bounded merge exhausted the child descriptor limit: {result:?}"
                );
                assert!(
                    std::fs::read_dir(staging.path()).unwrap().next().is_none(),
                    "bounded merge leaked temporary runs"
                );
            })
        })
        .collect::<Vec<_>>();
    for worker in workers {
        worker.join().unwrap();
    }
}

#[test]
#[ignore = "isolated child entrypoint for the publication memory review gate"]
fn wide_single_row_build_child() {
    assert_eq!(
        std::env::var("RADIXDB_MEMORY_REVIEW_CHILD").as_deref(),
        Ok("1"),
        "child entrypoint must only run through its parent"
    );
    let staging = tempfile::tempdir().unwrap();
    let request = wide_request(staging.path());
    let row = (0_i64..4_096).map(Value::integer).collect::<Vec<_>>();
    let result = build_artifact_pair(&request, [Ok(SourceRow::new(1, row))]);
    assert!(
        result.is_ok(),
        "one legal wide row exceeded the bounded publication corridor: {result:?}"
    );
}

#[test]
fn sorted_run_merge_respects_low_file_descriptor_limit() {
    for descriptor_limit in [64_u64, 128, 1_024] {
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .arg("--exact")
            .arg("low_file_descriptor_build_child")
            .arg("--ignored")
            .arg("--nocapture")
            .env("RADIXDB_LOW_FD_REVIEW_CHILD", "1")
            .env(
                "RADIXDB_DESCRIPTOR_REVIEW_LIMIT",
                descriptor_limit.to_string(),
            );
        // SAFETY: this closure runs in the forked child immediately before
        // exec, touches only its RLIMIT_NOFILE and returns an OS error on
        // failure.
        unsafe {
            command.pre_exec(move || {
                let limit = libc::rlimit {
                    rlim_cur: descriptor_limit,
                    rlim_max: descriptor_limit,
                };
                if libc::setrlimit(libc::RLIMIT_NOFILE, &limit) == 0 {
                    Ok(())
                } else {
                    Err(std::io::Error::last_os_error())
                }
            });
        }
        let output = command.output().unwrap();
        assert!(
            output.status.success(),
            "descriptor-limited child ({descriptor_limit}) failed\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

#[test]
fn wide_single_row_respects_process_memory_limit() {
    let mut command = Command::new(std::env::current_exe().unwrap());
    command
        .arg("--exact")
        .arg("wide_single_row_build_child")
        .arg("--ignored")
        .arg("--nocapture")
        .env("RADIXDB_MEMORY_REVIEW_CHILD", "1");
    // SAFETY: this closure runs in the forked child immediately before exec,
    // touches only its address-space limit and returns an OS error on failure.
    unsafe {
        command.pre_exec(|| {
            let limit = libc::rlimit {
                rlim_cur: 1024 * 1024 * 1024,
                rlim_max: 1024 * 1024 * 1024,
            };
            if libc::setrlimit(libc::RLIMIT_AS, &limit) == 0 {
                Ok(())
            } else {
                Err(std::io::Error::last_os_error())
            }
        });
    }
    let output = command.output().unwrap();
    assert!(
        output.status.success(),
        "memory-limited child failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}
