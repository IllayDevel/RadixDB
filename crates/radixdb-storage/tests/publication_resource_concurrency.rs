use std::sync::{mpsc, Arc, Condvar, Mutex};
use std::time::Duration;

use radixdb_catalog::{CatalogDataType, ObjectId};
use radixdb_core::{DataType, Value};
use radixdb_storage::v6::*;

fn object_id(marker: u8) -> ObjectId {
    ObjectId::from_user_bytes([marker; 16]).unwrap()
}

fn request(staging: &std::path::Path, marker: u8) -> ArtifactPairBuildRequest {
    let column = DataColumnSpec::new(
        object_id(marker),
        CatalogDataType::scalar(DataType::Integer).unwrap(),
        false,
    );
    let accelerator = AcceleratorBuildSpec::exact(
        object_id(marker.wrapping_add(1)),
        false,
        false,
        [marker.wrapping_add(2); 32],
        vec![IndexKeyColumn::new(
            column.column_id(),
            DataType::Integer,
            IndexSortDirection::Ascending,
            IndexNullsOrder::Last,
        )],
        IndexPageCodec::Lz4,
        ExactPageBuildLimits::default(),
    )
    .unwrap();
    ArtifactPairBuildRequest::new(
        DataArtifactHeader::new(
            ArtifactId::from_bytes([marker.wrapping_add(3); 16]).unwrap(),
            DatabaseId::from_bytes([marker.wrapping_add(4); 16]).unwrap(),
            object_id(marker.wrapping_add(5)),
            SegmentId::from_bytes([marker.wrapping_add(6); 16]).unwrap(),
            DatabaseGeneration::new(17).unwrap(),
            CatalogGeneration::new(13).unwrap(),
            501,
            509,
            1,
            1,
            1,
            SegmentKind::Rows,
            123_456,
        )
        .unwrap(),
        vec![column],
        vec![ColumnBuildPolicy::default()],
        DataPhysicalCodec::Lz4,
        ArtifactId::from_bytes([marker.wrapping_add(7); 16]).unwrap(),
        vec![accelerator],
        FanoutBuildLimits::default(),
        staging,
    )
    .unwrap()
}

#[test]
fn concurrent_builds_backpressure_at_the_process_budget() {
    let release = Arc::new((Mutex::new(false), Condvar::new()));
    let (started_tx, started_rx) = mpsc::channel();
    let mut workers = Vec::new();
    for ordinal in 0_u8..5 {
        let release = Arc::clone(&release);
        let started_tx = started_tx.clone();
        workers.push(std::thread::spawn(move || {
            let staging = tempfile::tempdir().unwrap();
            let request = request(staging.path(), 0x20 + ordinal * 16);
            let rows = std::iter::once_with(|| {
                started_tx.send(ordinal).unwrap();
                let (lock, condvar) = &*release;
                let mut released = lock.lock().unwrap();
                while !*released {
                    released = condvar.wait(released).unwrap();
                }
                Ok(SourceRow::new(1, vec![Value::integer(ordinal.into())]))
            });
            build_artifact_pair(&request, rows)
        }));
    }
    drop(started_tx);

    for _ in 0..4 {
        started_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("four default builds fit the one-GiB process corridor");
    }
    assert!(matches!(
        started_rx.recv_timeout(Duration::from_millis(250)),
        Err(mpsc::RecvTimeoutError::Timeout)
    ));

    let (lock, condvar) = &*release;
    *lock.lock().unwrap() = true;
    condvar.notify_all();
    started_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("the fifth build resumes after a resident permit is released");
    for worker in workers {
        worker.join().unwrap().unwrap();
    }
}
