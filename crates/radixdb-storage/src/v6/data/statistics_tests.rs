use std::cell::Cell;
use std::rc::Rc;

use radixdb_catalog::{CatalogDataType, ObjectId};
use radixdb_core::DataType;

use super::*;

struct CountedBlock {
    kind: DataBlockKind,
    group: u32,
    column: u32,
    visits: Rc<Cell<usize>>,
}

impl StatisticsBlock for CountedBlock {
    fn kind(&self) -> DataBlockKind {
        self.visits.set(self.visits.get() + 1);
        self.kind
    }

    fn row_group_ordinal(&self) -> u32 {
        self.group
    }

    fn column_ordinal(&self) -> u32 {
        self.column
    }

    fn validate_bloom_source(&self, _column: DataColumnSpec, count: u64) -> FormatResult<()> {
        assert_eq!(count, 1);
        Ok(())
    }
}

fn shape(
    width: u32,
    group_count: u32,
    blooms: impl Fn(u32, u32) -> bool,
) -> (
    Vec<DataColumnSpec>,
    Vec<DataRowGroup>,
    Vec<CountedBlock>,
    Vec<DataStatisticsSpec>,
) {
    let visits = Rc::new(Cell::new(0));
    let columns = (0..width)
        .map(|ordinal| {
            DataColumnSpec::new(
                ObjectId::from_user_bytes((1000 + u128::from(ordinal)).to_le_bytes()).unwrap(),
                CatalogDataType::scalar(DataType::Integer).unwrap(),
                false,
            )
        })
        .collect::<Vec<_>>();
    let mut blocks = Vec::new();
    let mut groups = Vec::new();
    for group in 0..group_count {
        let first = blocks.len() as u32;
        blocks.push(CountedBlock {
            kind: DataBlockKind::RowIds,
            group,
            column: u32::MAX,
            visits: Rc::clone(&visits),
        });
        for column in 0..width {
            blocks.push(CountedBlock {
                kind: DataBlockKind::Column,
                group,
                column,
                visits: Rc::clone(&visits),
            });
        }
        for column in 0..width {
            if blooms(group, column) {
                blocks.push(CountedBlock {
                    kind: DataBlockKind::Bloom,
                    group,
                    column,
                    visits: Rc::clone(&visits),
                });
            }
        }
        groups.push(
            DataRowGroup::new(
                group,
                1,
                u64::from(group),
                u64::from(group),
                u64::from(group),
                first,
                blocks.len() as u32 - first,
            )
            .unwrap(),
        );
    }
    let specs = columns
        .iter()
        .enumerate()
        .flat_map(|(column, spec)| {
            (0..group_count).map(move |group| {
                DataStatisticsSpec::from_values(
                    column as u32,
                    group,
                    *spec,
                    &[Value::integer(i64::from(group))],
                    None,
                )
                .unwrap()
            })
        })
        .collect();
    (columns, groups, blocks, specs)
}

#[test]
fn bloom_lookup_descriptor_work_is_linear_logarithmic() {
    for mode in 0..3 {
        for (width, count) in [(8, 128), (64, 8), (1, 1024)] {
            let (columns, groups, blocks, specs) = shape(width, count, |group, column| {
                mode == 1 || mode == 2 && (group + column) % 3 == 0
            });
            let count = specs.len();
            let encoded = build_statistics_with_blocks(&columns, &groups, &blocks, specs).unwrap();
            let visits = blocks[0].visits.get();
            let log_bound = blocks.len().ilog2() as usize + 2;
            assert!(
                visits <= 3 * blocks.len() + count * log_bound,
                "visited {visits} descriptors for {} blocks/{count} statistics",
                blocks.len()
            );
            for stat in encoded.entries {
                let expected = mode == 1
                    || mode == 2 && (stat.row_group_ordinal + stat.column_ordinal) % 3 == 0;
                assert_eq!(stat.bloom_block_index.is_some(), expected);
                if let Some(index) = stat.bloom_block_index {
                    let block = &blocks[index as usize];
                    assert_eq!(
                        (block.kind, block.group, block.column),
                        (
                            DataBlockKind::Bloom,
                            stat.row_group_ordinal,
                            stat.column_ordinal
                        )
                    );
                }
            }
        }
    }
}

#[test]
fn bloom_lookup_rejects_unordered_duplicate_and_unowned_blocks() {
    let (columns, groups, mut blocks, specs) = shape(2, 2, |_, _| true);
    blocks.swap(0, 1);
    assert!(build_statistics_with_blocks(&columns, &groups, &blocks, specs.clone()).is_err());
    blocks.swap(0, 1);
    blocks[4].column = blocks[3].column;
    assert!(build_statistics_with_blocks(&columns, &groups, &blocks, specs.clone()).is_err());
    blocks[4].column = 1;
    assert!(build_statistics_with_blocks(&columns, &groups, &blocks, specs[1..].to_vec()).is_err());
}
