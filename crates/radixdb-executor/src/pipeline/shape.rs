//! Explicit physical and public row-shape contract.

use radixdb_core::{CompactArc, Error, Result};
use radixdb_storage::traits::QueryResult;

use crate::result::ProjectedResult;

/// Describes the row crossing relational operators.
///
/// `physical_columns` can include private ORDER BY or DISTINCT ON keys. Only
/// the leading `public_width` columns may cross the SQL result boundary.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RowShape {
    physical_columns: CompactArc<Vec<String>>,
    public_width: usize,
}

impl RowShape {
    pub fn new(physical_columns: CompactArc<Vec<String>>, public_width: usize) -> Result<Self> {
        if public_width > physical_columns.len() {
            return Err(Error::internal(format!(
                "public row width {public_width} exceeds physical width {}",
                physical_columns.len()
            )));
        }
        Ok(Self {
            physical_columns,
            public_width,
        })
    }

    pub fn physical_columns(&self) -> &CompactArc<Vec<String>> {
        &self.physical_columns
    }

    pub fn physical_width(&self) -> usize {
        self.physical_columns.len()
    }

    pub fn public_width(&self) -> usize {
        self.public_width
    }

    pub fn has_private_tail(&self) -> bool {
        self.public_width > 0 && self.physical_width() > self.public_width
    }

    pub fn project_public(&self, result: Box<dyn QueryResult>) -> Box<dyn QueryResult> {
        if self.has_private_tail() {
            Box::new(ProjectedResult::new(result, self.public_width))
        } else {
            result
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn private_tail_is_explicit_and_bounded_by_physical_width() {
        let shape = RowShape::new(
            CompactArc::new(vec!["public".into(), "order-key".into()]),
            1,
        )
        .unwrap();
        assert_eq!(shape.public_width(), 1);
        assert_eq!(shape.physical_width(), 2);
        assert!(shape.has_private_tail());

        assert!(RowShape::new(CompactArc::new(vec!["only".into()]), 2).is_err());
    }
}
