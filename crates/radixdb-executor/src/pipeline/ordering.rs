//! ORDER BY row comparison and public-shape validation.

use std::cmp::Ordering;

use radixdb_core::Row;

pub type OrderSpec = (Option<usize>, bool, Option<bool>);

#[inline]
pub fn compare_rows(a: &Row, b: &Row, specs: &[OrderSpec]) -> Ordering {
    for (column, ascending, nulls_first) in specs {
        let Some(index) = column else {
            continue;
        };
        let left = a.get(*index);
        let right = b.get(*index);
        let left_null = left.is_none_or(|value| value.is_null());
        let right_null = right.is_none_or(|value| value.is_null());
        if left_null || right_null {
            if left_null && right_null {
                continue;
            }
            let nulls_first = nulls_first.unwrap_or(!*ascending);
            return if left_null == nulls_first {
                Ordering::Less
            } else {
                Ordering::Greater
            };
        }
        let ordering = left
            .zip(right)
            .and_then(|(left, right)| left.partial_cmp(right))
            .unwrap_or(Ordering::Equal);
        let ordering = if *ascending {
            ordering
        } else {
            ordering.reverse()
        };
        if ordering != Ordering::Equal {
            return ordering;
        }
    }
    Ordering::Equal
}

#[cfg(test)]
mod tests {
    use super::*;
    use radixdb_core::Value;

    #[test]
    fn applies_default_null_ordering() {
        let null = Row::from_values(vec![Value::null_unknown()]);
        let value = Row::from_values(vec![Value::Integer(1)]);
        assert_eq!(
            compare_rows(&null, &value, &[(Some(0), true, None)]),
            Ordering::Greater
        );
        assert_eq!(
            compare_rows(&null, &value, &[(Some(0), false, None)]),
            Ordering::Less
        );
    }
}
