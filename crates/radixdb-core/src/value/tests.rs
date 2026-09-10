use super::*;
use chrono::{Datelike, Timelike};

// =========================================================================
// Size verification tests
// =========================================================================

#[test]
fn test_value_size() {
    use std::mem::size_of;

    // Value must be exactly 16 bytes for memory efficiency
    assert_eq!(
        size_of::<Value>(),
        16,
        "Value should be 16 bytes, got {}",
        size_of::<Value>()
    );

    // Option<Value> should also be 16 bytes due to niche optimization
    assert_eq!(
        size_of::<Option<Value>>(),
        16,
        "Option<Value> should be 16 bytes (niche optimization), got {}",
        size_of::<Option<Value>>()
    );
}

// =========================================================================
// Constructor tests
// =========================================================================

#[test]
fn test_constructors() {
    assert!(Value::null(DataType::Integer).is_null());
    assert_eq!(Value::integer(42).as_int64(), Some(42));
    assert_eq!(Value::float(3.5).as_float64(), Some(3.5));
    assert_eq!(Value::text("hello").as_str(), Some("hello"));
    assert_eq!(Value::boolean(true).as_boolean(), Some(true));
    assert!(Value::json(r#"{"key": "value"}"#).as_json().is_some());
}

#[test]
fn test_from_implementations() {
    let v: Value = 42i64.into();
    assert_eq!(v.as_int64(), Some(42));

    let v: Value = 3.5f64.into();
    assert_eq!(v.as_float64(), Some(3.5));

    let v: Value = "hello".into();
    assert_eq!(v.as_str(), Some("hello"));

    let v: Value = true.into();
    assert_eq!(v.as_boolean(), Some(true));

    let v: Value = Option::<i64>::None.into();
    assert!(v.is_null());

    let v: Value = Some(42i64).into();
    assert_eq!(v.as_int64(), Some(42));
}

// =========================================================================
// Type accessor tests
// =========================================================================

#[test]
fn test_data_type() {
    assert_eq!(
        Value::null(DataType::Integer).data_type(),
        DataType::Integer
    );
    assert_eq!(Value::integer(42).data_type(), DataType::Integer);
    assert_eq!(Value::float(3.5).data_type(), DataType::Float);
    assert_eq!(Value::text("hello").data_type(), DataType::Text);
    assert_eq!(Value::boolean(true).data_type(), DataType::Boolean);
    assert_eq!(
        Value::Timestamp(Utc::now()).data_type(),
        DataType::Timestamp
    );
    assert_eq!(Value::json("{}").data_type(), DataType::Json);
}

// =========================================================================
// AsXxx conversion tests
// =========================================================================

#[test]
fn test_as_int64() {
    // Direct integer
    assert_eq!(Value::integer(42).as_int64(), Some(42));

    // Float to integer (truncates)
    assert_eq!(Value::float(3.7).as_int64(), Some(3));
    assert_eq!(Value::float(-3.7).as_int64(), Some(-3));

    // String to integer
    assert_eq!(Value::text("42").as_int64(), Some(42));
    assert_eq!(Value::text("-42").as_int64(), Some(-42));
    assert_eq!(Value::text("3.7").as_int64(), Some(3)); // Parse as float, convert

    // Boolean to integer
    assert_eq!(Value::boolean(true).as_int64(), Some(1));
    assert_eq!(Value::boolean(false).as_int64(), Some(0));

    // NULL returns None
    assert_eq!(Value::null(DataType::Integer).as_int64(), None);

    // Invalid string
    assert_eq!(Value::text("not a number").as_int64(), None);
}

#[test]
fn test_as_float64() {
    // Direct float
    assert_eq!(Value::float(3.5).as_float64(), Some(3.5));

    // Integer to float
    assert_eq!(Value::integer(42).as_float64(), Some(42.0));

    // String to float
    assert_eq!(Value::text("3.5").as_float64(), Some(3.5));

    // Boolean to float
    assert_eq!(Value::boolean(true).as_float64(), Some(1.0));
    assert_eq!(Value::boolean(false).as_float64(), Some(0.0));

    // NULL returns None
    assert_eq!(Value::null(DataType::Float).as_float64(), None);
}

#[test]
fn test_as_boolean() {
    // Direct boolean
    assert_eq!(Value::boolean(true).as_boolean(), Some(true));
    assert_eq!(Value::boolean(false).as_boolean(), Some(false));

    // Integer to boolean
    assert_eq!(Value::integer(1).as_boolean(), Some(true));
    assert_eq!(Value::integer(0).as_boolean(), Some(false));
    assert_eq!(Value::integer(-1).as_boolean(), Some(true));

    // Float to boolean
    assert_eq!(Value::float(1.0).as_boolean(), Some(true));
    assert_eq!(Value::float(0.0).as_boolean(), Some(false));

    // String to boolean (various string values)
    assert_eq!(Value::text("true").as_boolean(), Some(true));
    assert_eq!(Value::text("TRUE").as_boolean(), Some(true));
    assert_eq!(Value::text("t").as_boolean(), Some(true));
    assert_eq!(Value::text("yes").as_boolean(), Some(true));
    assert_eq!(Value::text("y").as_boolean(), Some(true));
    assert_eq!(Value::text("1").as_boolean(), Some(true));
    assert_eq!(Value::text("false").as_boolean(), Some(false));
    assert_eq!(Value::text("FALSE").as_boolean(), Some(false));
    assert_eq!(Value::text("f").as_boolean(), Some(false));
    assert_eq!(Value::text("no").as_boolean(), Some(false));
    assert_eq!(Value::text("n").as_boolean(), Some(false));
    assert_eq!(Value::text("0").as_boolean(), Some(false));
    assert_eq!(Value::text("").as_boolean(), Some(false));

    // Numeric strings
    assert_eq!(Value::text("42").as_boolean(), Some(true));
    assert_eq!(Value::text("0.0").as_boolean(), Some(false));
}

#[test]
fn test_as_string() {
    // Direct string
    assert_eq!(Value::text("hello").as_string(), Some("hello".to_string()));

    // Integer to string
    assert_eq!(Value::integer(42).as_string(), Some("42".to_string()));

    // Float to string
    assert_eq!(Value::float(3.5).as_string(), Some("3.5".to_string()));

    // Boolean to string
    assert_eq!(Value::boolean(true).as_string(), Some("true".to_string()));
    assert_eq!(Value::boolean(false).as_string(), Some("false".to_string()));

    // NULL returns None
    assert_eq!(Value::null(DataType::Text).as_string(), None);
}

// =========================================================================
// Equality tests
// =========================================================================

#[test]
fn test_equality() {
    // Same type equality
    assert_eq!(Value::integer(42), Value::integer(42));
    assert_ne!(Value::integer(42), Value::integer(43));

    assert_eq!(Value::float(3.5), Value::float(3.5));
    assert_ne!(Value::float(3.5), Value::float(3.15));

    assert_eq!(Value::text("hello"), Value::text("hello"));
    assert_ne!(Value::text("hello"), Value::text("world"));

    assert_eq!(Value::boolean(true), Value::boolean(true));
    assert_ne!(Value::boolean(true), Value::boolean(false));

    // NULL equality
    assert_eq!(Value::null(DataType::Integer), Value::null(DataType::Float));
    assert_ne!(Value::null(DataType::Integer), Value::integer(0));

    // Cross-type numeric comparison: Integer and Float with same value ARE equal
    // This is important for queries like WHERE id = 5.0 or WHERE price = 100
    assert_eq!(Value::integer(1), Value::float(1.0));
    assert_eq!(Value::integer(5), Value::float(5.0));
    assert_ne!(Value::integer(1), Value::float(1.5)); // Different values are not equal

    // Different non-numeric types are not equal
    assert_ne!(Value::text("1"), Value::integer(1));
}

#[test]
fn test_float_nan_equality() {
    // NaN handling: NaN == NaN in our implementation (for consistency)
    let nan = Value::float(f64::NAN);
    assert_eq!(nan, nan.clone());
}

#[test]
fn test_integer_float_exact_identity_boundaries() {
    let two_to_53 = 1_i64 << 53;
    let largest_float_below_two_to_63 = f64::from_bits((i64::MAX as f64).to_bits() - 1);
    let first_float_below_negative_two_to_63 = f64::from_bits((i64::MIN as f64).to_bits() + 1);

    // Exactly representable integral floats share Integer identity even
    // outside the consecutive-integer range of f64.
    assert_eq!(Value::integer(two_to_53), Value::float(two_to_53 as f64));
    assert_eq!(
        Value::integer(two_to_53 + 2),
        Value::float((two_to_53 + 2) as f64)
    );

    // 2^53 + 1 rounds down to 2^53 as f64 and must keep a distinct key.
    let rounded_large = Value::float((two_to_53 + 1) as f64);
    assert_ne!(Value::integer(two_to_53 + 1), rounded_large);
    assert_eq!(
        Value::integer(two_to_53 + 1).cmp(&rounded_large),
        Ordering::Greater
    );

    // i64::MAX rounds up to 2^63; that Float is outside the i64 domain.
    let two_to_63 = i64::MAX as f64;
    assert_eq!(two_to_63, 9_223_372_036_854_775_808.0);
    assert_ne!(Value::integer(i64::MAX), Value::float(two_to_63));
    assert_eq!(
        Value::integer(i64::MAX).cmp(&Value::float(two_to_63)),
        Ordering::Less
    );
    assert_eq!(
        Value::integer(9_223_372_036_854_774_784),
        Value::float(largest_float_below_two_to_63)
    );

    // -2^63 is exactly representable, while -2^63 + 1 is not.
    let negative_two_to_63 = i64::MIN as f64;
    assert_eq!(Value::integer(i64::MIN), Value::float(negative_two_to_63));
    assert_ne!(
        Value::integer(i64::MIN + 1),
        Value::float(negative_two_to_63)
    );
    assert_eq!(
        Value::integer(i64::MIN + 1).cmp(&Value::float(negative_two_to_63)),
        Ordering::Greater
    );
    assert_eq!(
        Value::integer(i64::MIN).cmp(&Value::float(first_float_below_negative_two_to_63)),
        Ordering::Greater
    );

    assert_eq!(Value::integer(0), Value::float(0.0));
    assert_eq!(Value::integer(0), Value::float(-0.0));
    assert_eq!(
        Value::integer(0).cmp(&Value::float(f64::from_bits(1))),
        Ordering::Less
    );
    assert_eq!(
        Value::integer(0).cmp(&Value::float(f64::from_bits(0x8000_0000_0000_0001))),
        Ordering::Greater
    );
    assert_eq!(Value::integer(5).cmp(&Value::float(5.5)), Ordering::Less);
    assert_eq!(
        Value::integer(-5).cmp(&Value::float(-5.5)),
        Ordering::Greater
    );
    assert_eq!(
        Value::integer(i64::MAX).cmp(&Value::float(f64::INFINITY)),
        Ordering::Less
    );
    assert_eq!(
        Value::integer(i64::MIN).cmp(&Value::float(f64::NEG_INFINITY)),
        Ordering::Greater
    );
    assert_eq!(
        Value::integer(0).cmp(&Value::float(f64::NAN)),
        Ordering::Less
    );
}

#[test]
fn test_integer_float_equality_order_hash_laws() {
    use std::hash::{DefaultHasher, Hash, Hasher};

    fn hash_value(value: &Value) -> u64 {
        let mut hasher = DefaultHasher::new();
        value.hash(&mut hasher);
        hasher.finish()
    }

    let values = [
        Value::float(f64::NEG_INFINITY),
        Value::float(f64::from_bits((i64::MIN as f64).to_bits() + 1)),
        Value::integer(i64::MIN),
        Value::float(i64::MIN as f64),
        Value::integer(i64::MIN + 1),
        Value::integer(-(1_i64 << 53) - 1),
        Value::float((-(1_i64 << 53) - 1) as f64),
        Value::integer(-(1_i64 << 53) - 2),
        Value::float((-(1_i64 << 53) - 2) as f64),
        Value::float(f64::from_bits(0x8000_0000_0000_0001)),
        Value::float(-0.5),
        Value::integer(0),
        Value::float(-0.0),
        Value::float(0.0),
        Value::float(0.5),
        Value::float(f64::from_bits(1)),
        Value::integer((1_i64 << 53) + 1),
        Value::float(((1_i64 << 53) + 1) as f64),
        Value::integer((1_i64 << 53) + 2),
        Value::float(((1_i64 << 53) + 2) as f64),
        Value::float(f64::from_bits((i64::MAX as f64).to_bits() - 1)),
        Value::integer(i64::MAX),
        Value::float(i64::MAX as f64),
        Value::float(f64::INFINITY),
        Value::float(f64::NAN),
        Value::float(f64::from_bits(0x7ff0_0000_0000_0001)),
    ];

    for left in &values {
        for right in &values {
            let ordering = left.cmp(right);
            assert_eq!(
                ordering,
                right.cmp(left).reverse(),
                "numeric ordering must be antisymmetric: {left:?}, {right:?}"
            );
            assert_eq!(
                left == right,
                ordering == Ordering::Equal,
                "Eq and Ord must identify the same numeric keys: {left:?}, {right:?}"
            );
            assert_eq!(
                left.compare(right).unwrap(),
                ordering,
                "SQL numeric comparison and key ordering must agree: {left:?}, {right:?}"
            );
            assert_eq!(
                left.partial_cmp(right),
                Some(ordering),
                "PartialOrd and Ord must agree: {left:?}, {right:?}"
            );
            if left == right {
                assert_eq!(
                    hash_value(left),
                    hash_value(right),
                    "equal numeric keys must hash equally: {left:?}, {right:?}"
                );
            }
        }
    }

    for first in &values {
        for second in &values {
            for third in &values {
                if first <= second && second <= third {
                    assert!(
                        first <= third,
                        "numeric ordering must be transitive: {first:?}, {second:?}, {third:?}"
                    );
                }
                if first == second && second == third {
                    assert_eq!(
                        first, third,
                        "numeric equality must be transitive: {first:?}, {second:?}, {third:?}"
                    );
                }
            }
        }
    }
}

#[test]
fn test_integer_float_exact_identity_in_hash_and_btree_containers() {
    use rustc_hash::{FxHashMap, FxHashSet};
    use std::collections::{BTreeMap, BTreeSet};

    let rounded = 1_i64 << 53;
    let exact_integer = rounded + 1;
    let rounded_float = Value::float(exact_integer as f64);

    let mut hash_set = FxHashSet::default();
    hash_set.insert(Value::integer(rounded));
    hash_set.insert(rounded_float.clone());
    hash_set.insert(Value::integer(exact_integer));
    assert_eq!(hash_set.len(), 2);
    assert!(hash_set.contains(&Value::integer(rounded)));
    assert!(hash_set.contains(&Value::integer(exact_integer)));

    let mut tree_set = BTreeSet::new();
    tree_set.insert(Value::integer(rounded));
    tree_set.insert(rounded_float.clone());
    tree_set.insert(Value::integer(exact_integer));
    assert_eq!(tree_set.len(), 2);

    let mut hash_map = FxHashMap::default();
    hash_map.insert(Value::integer(i64::MAX), "integer max");
    hash_map.insert(Value::float(i64::MAX as f64), "two to 63");
    assert_eq!(hash_map.len(), 2);
    assert_eq!(
        hash_map.get(&Value::integer(i64::MAX)),
        Some(&"integer max")
    );
    assert_eq!(
        hash_map.get(&Value::float(i64::MAX as f64)),
        Some(&"two to 63")
    );

    let mut tree_map = BTreeMap::new();
    tree_map.insert(Value::integer(0), "integer zero");
    tree_map.insert(Value::float(-0.0), "float zero");
    assert_eq!(tree_map.len(), 1);
    assert_eq!(tree_map.get(&Value::float(0.0)), Some(&"float zero"));
}

// =========================================================================
// Comparison tests
// =========================================================================

#[test]
fn test_compare_integers() {
    assert_eq!(
        Value::integer(1).compare(&Value::integer(2)).unwrap(),
        Ordering::Less
    );
    assert_eq!(
        Value::integer(2).compare(&Value::integer(2)).unwrap(),
        Ordering::Equal
    );
    assert_eq!(
        Value::integer(3).compare(&Value::integer(2)).unwrap(),
        Ordering::Greater
    );
}

#[test]
fn test_compare_floats() {
    assert_eq!(
        Value::float(1.0).compare(&Value::float(2.0)).unwrap(),
        Ordering::Less
    );
    assert_eq!(
        Value::float(2.0).compare(&Value::float(2.0)).unwrap(),
        Ordering::Equal
    );
    assert_eq!(
        Value::float(3.0).compare(&Value::float(2.0)).unwrap(),
        Ordering::Greater
    );
}

#[test]
fn test_compare_cross_type_numeric() {
    // Integer vs Float comparison
    assert_eq!(
        Value::integer(1).compare(&Value::float(2.0)).unwrap(),
        Ordering::Less
    );
    assert_eq!(
        Value::integer(2).compare(&Value::float(2.0)).unwrap(),
        Ordering::Equal
    );
    assert_eq!(
        Value::float(3.0).compare(&Value::integer(2)).unwrap(),
        Ordering::Greater
    );
}

#[test]
fn decimal_coercion_preserves_shortest_float_value() {
    let decimal = Value::float(123.45).coerce_to_type(DataType::Decimal);
    assert_eq!(decimal.as_decimal_parts(), Some((12_345, 5, 2)));
    assert_eq!(decimal.as_string().as_deref(), Some("123.45"));

    let small = Value::float(1e-20).coerce_to_type(DataType::Decimal);
    assert_eq!(small.as_decimal_parts(), Some((1, 20, 20)));
    assert_eq!(small.as_string().as_deref(), Some("0.00000000000000000001"));

    let scale_38 =
        Value::text("0.00000000000000000000000000000000000001").coerce_to_type(DataType::Decimal);
    assert_eq!(scale_38.as_decimal_parts(), Some((1, 38, 38)));

    assert!(Value::float(f64::NAN)
        .coerce_to_type(DataType::Decimal)
        .is_null());
    assert!(Value::float(f64::INFINITY)
        .coerce_to_type(DataType::Decimal)
        .is_null());
}

#[test]
fn decimal_compares_exactly_across_scales_and_numeric_types() {
    let decimal = Value::decimal(12_345, 5, 2);
    let same_with_more_scale = Value::decimal(1_234_500, 7, 4);
    assert_eq!(decimal.compare(&same_with_more_scale), Ok(Ordering::Equal));
    assert_eq!(decimal.compare(&Value::float(123.45)), Ok(Ordering::Equal));
    assert_eq!(decimal.compare(&Value::integer(123)), Ok(Ordering::Greater));
    assert_eq!(Value::integer(124).compare(&decimal), Ok(Ordering::Greater));

    assert_eq!(
        decimal.coerce_to_type(DataType::Text).as_str(),
        Some("123.45")
    );
    assert_eq!(
        decimal.coerce_to_type(DataType::Integer).as_int64(),
        Some(123)
    );
    assert_eq!(
        decimal.coerce_to_type(DataType::Float).as_float64(),
        Some(123.45)
    );
}

#[test]
fn decimal_identity_preserves_payload_but_ignores_scale_and_precision_metadata() {
    use std::hash::{DefaultHasher, Hash, Hasher};

    fn hash_value(value: &Value) -> u64 {
        let mut hasher = DefaultHasher::new();
        value.hash(&mut hasher);
        hasher.finish()
    }

    let one_tenth = Value::decimal(1, 1, 1);
    let one_tenth_padded = Value::decimal(100, 38, 3);

    assert_eq!(one_tenth.as_decimal_parts(), Some((1, 1, 1)));
    assert_eq!(one_tenth_padded.as_decimal_parts(), Some((100, 38, 3)));
    match (&one_tenth, &one_tenth_padded) {
        (Value::Extension(left), Value::Extension(right)) => {
            assert_ne!(left.as_ref(), right.as_ref());
            assert_eq!(left.len(), 19);
            assert_eq!(right.len(), 19);
        }
        _ => panic!("Decimal values must retain their Extension payloads"),
    }
    assert_eq!(one_tenth, one_tenth_padded);
    assert_eq!(one_tenth.cmp(&one_tenth_padded), Ordering::Equal);
    assert_eq!(
        one_tenth.partial_cmp(&one_tenth_padded),
        Some(Ordering::Equal)
    );
    assert_eq!(hash_value(&one_tenth), hash_value(&one_tenth_padded));
}

#[test]
fn decimal_integer_float_equality_order_and_hash_laws() {
    use std::hash::{DefaultHasher, Hash, Hasher};

    fn hash_value(value: &Value) -> u64 {
        let mut hasher = DefaultHasher::new();
        value.hash(&mut hasher);
        hasher.finish()
    }

    let values = [
        Value::float(f64::NEG_INFINITY),
        Value::decimal(-99_999_999_999_999_999_999_999_999_999_999_999_999, 38, 0),
        Value::decimal(-100, 38, 2),
        Value::integer(-1),
        Value::float(-1.0),
        Value::decimal(-1, 38, 38),
        Value::float(-1e-39),
        Value::decimal(0, 38, 38),
        Value::integer(0),
        Value::float(-0.0),
        Value::decimal(1, 38, 38),
        Value::float(1e-39),
        Value::decimal(1, 1, 1),
        Value::float(0.1),
        Value::decimal(100, 3, 3),
        Value::decimal(123_450, 38, 3),
        Value::float(123.45),
        Value::decimal(i64::MAX as i128, 19, 0),
        Value::integer(i64::MAX),
        Value::decimal(10_i128.pow(20), 21, 0),
        Value::float(1e20),
        Value::decimal(99_999_999_999_999_999_999_999_999_999_999_999_999, 38, 0),
        Value::float(1e39),
        Value::float(f64::INFINITY),
        Value::float(f64::NAN),
    ];

    for left in &values {
        for right in &values {
            let ordering = left.cmp(right);
            assert_eq!(
                ordering,
                right.cmp(left).reverse(),
                "Decimal numeric ordering must be antisymmetric: {left:?}, {right:?}"
            );
            assert_eq!(
                left == right,
                ordering == Ordering::Equal,
                "Decimal Eq and Ord must identify the same keys: {left:?}, {right:?}"
            );
            assert_eq!(left.compare(right).unwrap(), ordering);
            assert_eq!(left.partial_cmp(right), Some(ordering));
            if left == right {
                assert_eq!(hash_value(left), hash_value(right));
            }
        }
    }

    for first in &values {
        for second in &values {
            for third in &values {
                if first <= second && second <= third {
                    assert!(
                        first <= third,
                        "Decimal numeric ordering must be transitive: {first:?}, {second:?}, {third:?}"
                    );
                }
            }
        }
    }
}

#[test]
fn decimal_identity_is_shared_by_hash_and_btree_containers() {
    use rustc_hash::FxHashSet;
    use std::collections::BTreeSet;

    let equivalent = [
        Value::decimal(100, 3, 2),
        Value::decimal(1_000, 38, 3),
        Value::integer(1),
        Value::float(1.0),
    ];

    let hash_values: FxHashSet<_> = equivalent.iter().cloned().collect();
    let tree_values: BTreeSet<_> = equivalent.into_iter().collect();
    assert_eq!(hash_values.len(), 1);
    assert_eq!(tree_values.len(), 1);
}

#[test]
fn test_compare_strings() {
    assert_eq!(
        Value::text("a").compare(&Value::text("b")).unwrap(),
        Ordering::Less
    );
    assert_eq!(
        Value::text("b").compare(&Value::text("b")).unwrap(),
        Ordering::Equal
    );
    assert_eq!(
        Value::text("c").compare(&Value::text("b")).unwrap(),
        Ordering::Greater
    );
}

#[test]
fn test_compare_null() {
    // NULL comparisons
    assert_eq!(
        Value::null(DataType::Integer)
            .compare(&Value::null(DataType::Float))
            .unwrap(),
        Ordering::Equal
    );

    // NULL vs non-NULL should error
    assert!(Value::null(DataType::Integer)
        .compare(&Value::integer(0))
        .is_err());
    assert!(Value::integer(0)
        .compare(&Value::null(DataType::Integer))
        .is_err());
}

#[test]
fn test_compare_json_error() {
    // JSON comparison only allows equality
    let j1 = Value::json(r#"{"a": 1}"#);
    let j2 = Value::json(r#"{"b": 2}"#);
    assert!(j1.compare(&j2).is_err());

    // Same JSON values are equal
    let j3 = Value::json(r#"{"a": 1}"#);
    assert_eq!(j1.compare(&j3).unwrap(), Ordering::Equal);
}

// =========================================================================
// Timestamp parsing tests
// =========================================================================

#[test]
fn test_parse_timestamp() {
    // RFC3339
    let ts = parse_timestamp("2024-01-15T10:30:00Z").unwrap();
    assert_eq!(ts.year(), 2024);
    assert_eq!(ts.month(), 1);
    assert_eq!(ts.day(), 15);
    assert_eq!(ts.hour(), 10);
    assert_eq!(ts.minute(), 30);

    // SQL format
    let ts = parse_timestamp("2024-01-15 10:30:00").unwrap();
    assert_eq!(ts.year(), 2024);

    // Date only
    let ts = parse_timestamp("2024-01-15").unwrap();
    assert_eq!(ts.year(), 2024);
    assert_eq!(ts.hour(), 0);

    // Invalid format
    assert!(parse_timestamp("not a date").is_err());
}

// =========================================================================
// Display tests
// =========================================================================

#[test]
fn test_display() {
    assert_eq!(Value::null(DataType::Integer).to_string(), "NULL");
    assert_eq!(Value::integer(42).to_string(), "42");
    assert_eq!(Value::float(3.5).to_string(), "3.5");
    assert_eq!(Value::text("hello").to_string(), "hello");
    assert_eq!(Value::boolean(true).to_string(), "true");
    assert_eq!(Value::boolean(false).to_string(), "false");
}

// =========================================================================
// Hash tests
// =========================================================================

#[test]
fn test_hash() {
    use rustc_hash::FxHashSet;

    let mut set = FxHashSet::default();
    set.insert(Value::integer(42));
    set.insert(Value::integer(42)); // Duplicate
    set.insert(Value::integer(43));

    assert_eq!(set.len(), 2);
    assert!(set.contains(&Value::integer(42)));
    assert!(set.contains(&Value::integer(43)));
}

#[test]
fn test_hash_integer_float_consistency() {
    use std::hash::{DefaultHasher, Hash, Hasher};

    fn hash_value(v: &Value) -> u64 {
        let mut hasher = DefaultHasher::new();
        v.hash(&mut hasher);
        hasher.finish()
    }

    // Basic case: Integer(5) and Float(5.0) must hash the same
    assert_eq!(
        hash_value(&Value::integer(5)),
        hash_value(&Value::float(5.0))
    );
    assert_eq!(
        hash_value(&Value::integer(-100)),
        hash_value(&Value::float(-100.0))
    );
    assert_eq!(
        hash_value(&Value::integer(0)),
        hash_value(&Value::float(0.0))
    );

    // Fractional floats should NOT hash the same as any integer
    assert_ne!(
        hash_value(&Value::float(5.5)),
        hash_value(&Value::integer(5))
    );
    assert_ne!(
        hash_value(&Value::float(5.5)),
        hash_value(&Value::integer(6))
    );

    // Largest consecutive integer representable in f64
    let largest_consecutive = (1_i64 << 53) - 1; // 9007199254740991
    assert_eq!(
        hash_value(&Value::integer(largest_consecutive)),
        hash_value(&Value::float(largest_consecutive as f64))
    );
    assert_eq!(
        hash_value(&Value::integer(-largest_consecutive)),
        hash_value(&Value::float(-largest_consecutive as f64))
    );

    // Boundary case: 2^53 is still exactly representable.
    let boundary = 1_i64 << 53; // 9007199254740992
    assert_eq!(
        hash_value(&Value::integer(boundary)),
        hash_value(&Value::float(boundary as f64))
    );

    // 2^53 + 1 rounds to 2^53 in f64, so it is a different exact key.
    let large = boundary + 1; // 9007199254740993
    let large_as_f64 = large as f64; // rounds to 9007199254740992.0
    assert_ne!(Value::integer(large), Value::float(large_as_f64));
}

#[test]
fn test_hash_in_hashmap() {
    use rustc_hash::FxHashMap;

    // Test that Integer and Float can be used as equivalent keys
    let mut map = FxHashMap::default();
    map.insert(Value::integer(42), "int");

    // Looking up with Float(42.0) should find the Integer(42) entry
    assert_eq!(map.get(&Value::float(42.0)), Some(&"int"));

    // Inserting Float(42.0) should overwrite Integer(42)
    map.insert(Value::float(42.0), "float");
    assert_eq!(map.len(), 1);
    assert_eq!(map.get(&Value::integer(42)), Some(&"float"));
}

#[test]
fn test_hash_nan_consistency() {
    use std::hash::{DefaultHasher, Hash, Hasher};

    fn hash_value(v: &Value) -> u64 {
        let mut hasher = DefaultHasher::new();
        v.hash(&mut hasher);
        hasher.finish()
    }

    // All NaN values must hash the same (they're equal in PartialEq)
    let nan1 = Value::float(f64::NAN);
    // Use a different NaN representation (quiet vs signaling doesn't matter for hash equality)
    let nan2 = Value::float(f64::from_bits(0x7ff8000000000001)); // Another NaN bit pattern
    let nan3 = Value::float(f64::INFINITY - f64::INFINITY);

    assert_eq!(hash_value(&nan1), hash_value(&nan2));
    assert_eq!(hash_value(&nan2), hash_value(&nan3));

    // Verify they're equal in PartialEq
    assert_eq!(nan1, nan2);
    assert_eq!(nan2, nan3);
}

#[test]
fn negative_epoch_nanos_round_trip_exactly() {
    for nanos in [
        i64::MIN,
        -1_000_000_001,
        -1_000_000_000,
        -999_999_999,
        -1,
        0,
        1,
        i64::MAX,
    ] {
        let expected = DateTime::from_timestamp_nanos(nanos);
        assert_eq!(Value::Integer(nanos).as_timestamp(), Some(expected));
        assert_eq!(
            Value::Integer(nanos).try_coerce_to_type(DataType::Timestamp),
            Ok(Value::Timestamp(expected))
        );
    }
}

#[test]
fn date_to_timestamp_coercion_uses_utc_midnight() {
    let date = Value::date(-1);
    assert_eq!(
        date.try_coerce_to_type(DataType::Timestamp),
        Ok(Value::Timestamp(
            DateTime::parse_from_rfc3339("1969-12-31T00:00:00Z")
                .unwrap()
                .with_timezone(&Utc)
        ))
    );
}

#[test]
fn public_extension_builders_reject_malformed_shapes() {
    assert!(Value::try_decimal(1, 0, 0).is_err());
    assert!(Value::try_decimal(1, 1, 2).is_err());
    assert!(Value::try_decimal(100, 2, 0).is_err());
    assert!(Value::try_decimal(i128::MAX, 38, 0).is_err());
    assert_eq!(
        Value::try_decimal(1234, 4, 2).unwrap().as_decimal_parts(),
        Some((1234, 4, 2))
    );

    assert!(Value::try_json("not json").is_err());
    assert_eq!(
        Value::try_json("{\"ok\":true}").unwrap().as_json(),
        Some("{\"ok\":true}")
    );
    assert_eq!(Value::Null(DataType::Json).as_json(), None);

    assert!(Value::try_vector_from_bytes(CompactArc::from(vec![1, 2, 3])).is_err());
    assert_eq!(
        Value::try_vector_from_bytes(CompactArc::from(1.25_f32.to_le_bytes().to_vec()))
            .unwrap()
            .as_vector_f32(),
        Some(vec![1.25])
    );
}

#[test]
fn external_marker_fast_path_preserves_the_plugin_comparison_boundary() {
    let type_ref = ExternalTypeRef::new([0x51; 16], 1).unwrap();
    let external = Value::try_external(type_ref, [0x10, 0x20]).unwrap();

    assert!(external.is_external());
    assert_eq!(external.as_external().unwrap().type_ref(), type_ref);
    assert!(matches!(
        external.compare(&external),
        Err(Error::IncomparableTypes)
    ));
    assert!(!Value::bytes(vec![0x10, 0x20]).is_external());
}

#[test]
fn checked_integer_coercion_never_saturates_or_invents_values() {
    for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY, 2_f64.powi(63)] {
        assert!(Value::Float(value)
            .try_coerce_to_type(DataType::Integer)
            .is_err());
        assert_eq!(Value::Float(value).as_int64(), None);
    }
    assert_eq!(
        Value::Float(-9_223_372_036_854_775_808.0).as_int64(),
        Some(i64::MIN)
    );
    assert!(Value::Text(SmartString::from("9223372036854775808"))
        .try_coerce_to_type(DataType::Integer)
        .is_err());
    let outside_nanosecond_range = DateTime::parse_from_rfc3339("2500-01-01T00:00:00Z")
        .unwrap()
        .with_timezone(&Utc);
    assert_eq!(Value::Timestamp(outside_nanosecond_range).as_int64(), None);
}

#[test]
fn mixed_non_numeric_values_are_not_structural_equalities() {
    let uuid = Value::uuid(*Uuid::nil().as_bytes());
    let uuid_text = Value::text(Uuid::nil().hyphenated().to_string());
    let timestamp = Value::Timestamp(DateTime::from_timestamp_nanos(0));
    let timestamp_text = Value::text(timestamp.as_string().unwrap());

    for (left, right) in [
        (&uuid, &uuid_text),
        (&timestamp, &timestamp_text),
        (&Value::Boolean(true), &Value::text("true")),
    ] {
        assert_ne!(left, right);
        assert!(left.compare(right).is_err());
        assert_eq!(left.partial_cmp(right), None);
        assert_ne!(left.cmp(right), Ordering::Equal);
    }
}
