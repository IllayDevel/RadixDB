use std::cell::Cell;
use std::rc::Rc;

use super::*;

struct CountedBlock {
    kind: DataBlockKind,
    column: u32,
    visits: Rc<Cell<usize>>,
}

impl ColumnBlockDescriptor for CountedBlock {
    fn kind(&self) -> DataBlockKind {
        self.visits.set(self.visits.get() + 1);
        self.kind
    }

    fn column_ordinal(&self) -> u32 {
        self.visits.set(self.visits.get() + 1);
        self.column
    }
}

#[test]
fn column_block_map_visits_each_descriptor_at_most_twice() {
    let visits = Rc::new(Cell::new(0));
    let columns = 64_u32;
    let groups = 1024_u32;
    let mut blocks = Vec::new();
    for _group in 0..groups {
        blocks.push(CountedBlock {
            kind: DataBlockKind::RowIds,
            column: u32::MAX,
            visits: Rc::clone(&visits),
        });
        for column in 0..columns {
            blocks.push(CountedBlock {
                kind: DataBlockKind::Column,
                column,
                visits: Rc::clone(&visits),
            });
        }
    }

    let (first, counts) = collect_column_block_map(&blocks, columns as usize).unwrap();

    assert!(visits.get() <= blocks.len() * 2);
    for column in 0..columns as usize {
        assert_eq!(first[column], column as u32 + 1);
        assert_eq!(counts[column], groups);
    }
}

#[test]
fn column_block_map_rejects_an_out_of_range_ordinal() {
    let visits = Rc::new(Cell::new(0));
    let blocks = [CountedBlock {
        kind: DataBlockKind::Column,
        column: 2,
        visits,
    }];

    assert!(collect_column_block_map(&blocks, 2).is_err());
}
