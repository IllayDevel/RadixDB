// Copyright 2026 RadixDB Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Vector functions for SQL queries
//!
//! Vectors are stored as Extension(Vector) with packed LE f32 bytes.
//! Distance functions operate directly on raw bytes — zero allocation.

use radixdb_core::vector::validate_vector_bytes;
use radixdb_core::{DataType, Error, Result, Value};

pub use radixdb_core::vector::{
    cosine_distance, cosine_distance_bytes, ip_distance_bytes, l2_distance, l2_distance_bytes,
};

#[inline(always)]
fn read_f32(data: &[u8], index: usize) -> f32 {
    let offset = index * 4;
    f32::from_le_bytes([
        data[offset],
        data[offset + 1],
        data[offset + 2],
        data[offset + 3],
    ])
}

use crate::{FunctionDataType, FunctionInfo, FunctionSignature, FunctionType, ScalarFunction};

// ============================================================================
// Zero-copy byte extraction
// ============================================================================

/// Extract packed LE f32 payload bytes from a Value without allocation.
/// Returns (payload_bytes, dimension_count).
/// For Extension(Vector): zero-copy reference into the CompactArc data.
/// For Text: parses the string and returns owned bytes via the out buffer.
#[inline]
fn extract_vector_bytes<'a>(v: &'a Value, buf: &'a mut Vec<u8>) -> Result<Option<&'a [u8]>> {
    let bytes = match v {
        Value::Extension(data) if data.first() == Some(&(DataType::Vector as u8)) => {
            Some(&data[1..])
        }
        Value::Text(s) => {
            let Some(floats) = radixdb_core::value::parse_vector_str(s.as_ref()) else {
                return Ok(None);
            };
            buf.clear();
            buf.try_reserve(floats.len().saturating_mul(4))
                .map_err(|_| Error::invalid_argument("vector payload is too large"))?;
            for f in &floats {
                buf.extend_from_slice(&f.to_le_bytes());
            }
            Some(buf.as_slice())
        }
        _ => None,
    };
    if let Some(bytes) = bytes {
        validate_vector_bytes(bytes)?;
    }
    Ok(bytes)
}

/// Extract dimension count from a Value without allocation.
#[inline]
fn vector_dim_count(v: &Value) -> Result<Option<usize>> {
    match v {
        Value::Extension(data) if data.first() == Some(&(DataType::Vector as u8)) => {
            let bytes = &data[1..];
            validate_vector_bytes(bytes)?;
            Ok(Some(bytes.len() / 4))
        }
        Value::Text(s) => Ok(radixdb_core::value::parse_vector_str(s.as_ref()).map(|v| v.len())),
        _ => Ok(None),
    }
}

// ============================================================================
// SQL scalar functions
// ============================================================================

/// VEC_DISTANCE_L2(v1, v2) — Euclidean (L2) distance between two vectors
#[derive(Default)]
pub struct VecDistanceL2Function;

impl ScalarFunction for VecDistanceL2Function {
    fn name(&self) -> &str {
        "VEC_DISTANCE_L2"
    }

    fn info(&self) -> FunctionInfo {
        FunctionInfo::new(
            "VEC_DISTANCE_L2",
            FunctionType::Scalar,
            "Compute L2 (Euclidean) distance between two vectors",
            FunctionSignature::new(
                FunctionDataType::Float,
                vec![FunctionDataType::Any, FunctionDataType::Any],
                2,
                2,
            ),
        )
    }

    fn evaluate(&self, args: &[Value]) -> Result<Value> {
        crate::validate_arg_count!(args, "VEC_DISTANCE_L2", 2);
        if args[0].is_null() || args[1].is_null() {
            return Ok(Value::null_unknown());
        }
        let mut buf_a = Vec::new();
        let mut buf_b = Vec::new();
        let ba = extract_vector_bytes(&args[0], &mut buf_a)?;
        let bb = extract_vector_bytes(&args[1], &mut buf_b)?;
        match (ba, bb) {
            (Some(a), Some(b)) => {
                if a.len() != b.len() {
                    return Err(Error::invalid_argument(format!(
                        "VEC_DISTANCE_L2: dimension mismatch ({} vs {})",
                        a.len() / 4,
                        b.len() / 4
                    )));
                }
                Ok(Value::Float(l2_distance_bytes(a, b)?))
            }
            _ => Err(Error::invalid_argument(
                "VEC_DISTANCE_L2 requires two VECTOR arguments".to_string(),
            )),
        }
    }
}

/// VEC_DISTANCE_COSINE(v1, v2) — Cosine distance (1 - cosine_similarity)
#[derive(Default)]
pub struct VecDistanceCosineFunction;

impl ScalarFunction for VecDistanceCosineFunction {
    fn name(&self) -> &str {
        "VEC_DISTANCE_COSINE"
    }

    fn info(&self) -> FunctionInfo {
        FunctionInfo::new(
            "VEC_DISTANCE_COSINE",
            FunctionType::Scalar,
            "Compute cosine distance (1 - cosine_similarity) between two vectors",
            FunctionSignature::new(
                FunctionDataType::Float,
                vec![FunctionDataType::Any, FunctionDataType::Any],
                2,
                2,
            ),
        )
    }

    fn evaluate(&self, args: &[Value]) -> Result<Value> {
        crate::validate_arg_count!(args, "VEC_DISTANCE_COSINE", 2);
        if args[0].is_null() || args[1].is_null() {
            return Ok(Value::null_unknown());
        }
        let mut buf_a = Vec::new();
        let mut buf_b = Vec::new();
        let ba = extract_vector_bytes(&args[0], &mut buf_a)?;
        let bb = extract_vector_bytes(&args[1], &mut buf_b)?;
        match (ba, bb) {
            (Some(a), Some(b)) => {
                if a.len() != b.len() {
                    return Err(Error::invalid_argument(format!(
                        "VEC_DISTANCE_COSINE: dimension mismatch ({} vs {})",
                        a.len() / 4,
                        b.len() / 4
                    )));
                }
                Ok(Value::Float(cosine_distance_bytes(a, b)?))
            }
            _ => Err(Error::invalid_argument(
                "VEC_DISTANCE_COSINE requires two VECTOR arguments".to_string(),
            )),
        }
    }
}

/// VEC_DISTANCE_IP(v1, v2) — Negative inner product distance
#[derive(Default)]
pub struct VecDistanceIpFunction;

impl ScalarFunction for VecDistanceIpFunction {
    fn name(&self) -> &str {
        "VEC_DISTANCE_IP"
    }

    fn info(&self) -> FunctionInfo {
        FunctionInfo::new(
            "VEC_DISTANCE_IP",
            FunctionType::Scalar,
            "Compute negative inner product distance between two vectors",
            FunctionSignature::new(
                FunctionDataType::Float,
                vec![FunctionDataType::Any, FunctionDataType::Any],
                2,
                2,
            ),
        )
    }

    fn evaluate(&self, args: &[Value]) -> Result<Value> {
        crate::validate_arg_count!(args, "VEC_DISTANCE_IP", 2);
        if args[0].is_null() || args[1].is_null() {
            return Ok(Value::null_unknown());
        }
        let mut buf_a = Vec::new();
        let mut buf_b = Vec::new();
        let ba = extract_vector_bytes(&args[0], &mut buf_a)?;
        let bb = extract_vector_bytes(&args[1], &mut buf_b)?;
        match (ba, bb) {
            (Some(a), Some(b)) => {
                if a.len() != b.len() {
                    return Err(Error::invalid_argument(format!(
                        "VEC_DISTANCE_IP: dimension mismatch ({} vs {})",
                        a.len() / 4,
                        b.len() / 4
                    )));
                }
                Ok(Value::Float(ip_distance_bytes(a, b)?))
            }
            _ => Err(Error::invalid_argument(
                "VEC_DISTANCE_IP requires two VECTOR arguments".to_string(),
            )),
        }
    }
}

/// VEC_DIMS(v) — Returns the number of dimensions in a vector
#[derive(Default)]
pub struct VecDimsFunction;

impl ScalarFunction for VecDimsFunction {
    fn name(&self) -> &str {
        "VEC_DIMS"
    }

    fn info(&self) -> FunctionInfo {
        FunctionInfo::new(
            "VEC_DIMS",
            FunctionType::Scalar,
            "Returns the number of dimensions in a vector",
            FunctionSignature::new(FunctionDataType::Integer, vec![FunctionDataType::Any], 1, 1),
        )
    }

    fn evaluate(&self, args: &[Value]) -> Result<Value> {
        crate::validate_arg_count!(args, "VEC_DIMS", 1);
        if args[0].is_null() {
            return Ok(Value::null_unknown());
        }
        match vector_dim_count(&args[0])? {
            Some(n) => Ok(Value::Integer(n as i64)),
            None => Err(Error::invalid_argument(
                "VEC_DIMS requires a VECTOR argument".to_string(),
            )),
        }
    }
}

/// VEC_NORM(v) — Returns the L2 norm of a vector
#[derive(Default)]
pub struct VecNormFunction;

impl ScalarFunction for VecNormFunction {
    fn name(&self) -> &str {
        "VEC_NORM"
    }

    fn info(&self) -> FunctionInfo {
        FunctionInfo::new(
            "VEC_NORM",
            FunctionType::Scalar,
            "Returns the L2 norm (magnitude) of a vector",
            FunctionSignature::new(FunctionDataType::Float, vec![FunctionDataType::Any], 1, 1),
        )
    }

    fn evaluate(&self, args: &[Value]) -> Result<Value> {
        crate::validate_arg_count!(args, "VEC_NORM", 1);
        if args[0].is_null() {
            return Ok(Value::null_unknown());
        }
        let mut buf = Vec::new();
        match extract_vector_bytes(&args[0], &mut buf)? {
            Some(data) => {
                let len = data.len() / 4;
                let mut sum = 0.0f64;
                for i in 0..len {
                    let d = read_f32(data, i) as f64;
                    sum += d * d;
                }
                Ok(Value::Float(sum.sqrt()))
            }
            None => Err(Error::invalid_argument(
                "VEC_NORM requires a VECTOR argument".to_string(),
            )),
        }
    }
}

/// VEC_TO_TEXT(v) — Explicit cast from vector to text
#[derive(Default)]
pub struct VecToTextFunction;

impl ScalarFunction for VecToTextFunction {
    fn name(&self) -> &str {
        "VEC_TO_TEXT"
    }

    fn info(&self) -> FunctionInfo {
        FunctionInfo::new(
            "VEC_TO_TEXT",
            FunctionType::Scalar,
            "Convert a vector to its text representation",
            FunctionSignature::new(FunctionDataType::String, vec![FunctionDataType::Any], 1, 1),
        )
    }

    fn evaluate(&self, args: &[Value]) -> Result<Value> {
        crate::validate_arg_count!(args, "VEC_TO_TEXT", 1);
        if args[0].is_null() {
            return Ok(Value::null_unknown());
        }
        Ok(Value::text(args[0].to_string().as_str()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_l2_distance() {
        let a = vec![1.0f32, 0.0, 0.0];
        let b = vec![0.0f32, 1.0, 0.0];
        let dist = l2_distance(&a, &b).unwrap();
        assert!((dist - std::f64::consts::SQRT_2).abs() < 1e-10);
    }

    #[test]
    fn test_l2_distance_same() {
        let a = vec![1.0f32, 2.0, 3.0];
        let dist = l2_distance(&a, &a).unwrap();
        assert!(dist.abs() < 1e-10);
    }

    #[test]
    fn test_l2_distance_bytes() {
        let a = Value::vector(vec![1.0f32, 0.0, 0.0]);
        let b = Value::vector(vec![0.0f32, 1.0, 0.0]);
        let mut ba = Vec::new();
        let mut bb = Vec::new();
        let da = extract_vector_bytes(&a, &mut ba).unwrap().unwrap();
        let db = extract_vector_bytes(&b, &mut bb).unwrap().unwrap();
        let dist = l2_distance_bytes(da, db).unwrap();
        assert!((dist - std::f64::consts::SQRT_2).abs() < 1e-10);
    }

    #[test]
    fn test_cosine_distance_orthogonal() {
        let a = vec![1.0f32, 0.0];
        let b = vec![0.0f32, 1.0];
        let dist = cosine_distance(&a, &b).unwrap();
        assert!((dist - 1.0).abs() < 1e-10);
    }

    #[test]
    fn test_cosine_distance_same() {
        let a = vec![1.0f32, 2.0, 3.0];
        let dist = cosine_distance(&a, &a).unwrap();
        assert!(dist.abs() < 1e-10);
    }

    #[test]
    fn test_cosine_distance_bytes() {
        let a = Value::vector(vec![1.0f32, 0.0]);
        let b = Value::vector(vec![0.0f32, 1.0]);
        let mut ba = Vec::new();
        let mut bb = Vec::new();
        let da = extract_vector_bytes(&a, &mut ba).unwrap().unwrap();
        let db = extract_vector_bytes(&b, &mut bb).unwrap().unwrap();
        let dist = cosine_distance_bytes(da, db).unwrap();
        assert!((dist - 1.0).abs() < 1e-10);
    }

    #[test]
    fn malformed_and_mismatched_vectors_return_errors() {
        assert!(l2_distance_bytes(&[0, 0, 0], &[0, 0, 0]).is_err());
        assert!(cosine_distance(&[1.0], &[1.0, 2.0]).is_err());

        let mut raw = vec![DataType::Vector as u8];
        raw.extend_from_slice(&[0, 0, 0]);
        let malformed = Value::Extension(radixdb_core::CompactArc::from(raw));
        assert!(VecDimsFunction
            .evaluate(std::slice::from_ref(&malformed))
            .is_err());
        assert!(VecNormFunction.evaluate(&[malformed]).is_err());
    }

    #[test]
    fn test_vec_dims() {
        let func = VecDimsFunction;
        let v = Value::text("[1.0, 2.0, 3.0]");
        assert_eq!(func.evaluate(&[v]).unwrap(), Value::Integer(3));
    }

    #[test]
    fn test_vec_dims_extension() {
        let func = VecDimsFunction;
        let v = Value::vector(vec![1.0, 2.0, 3.0]);
        assert_eq!(func.evaluate(&[v]).unwrap(), Value::Integer(3));
    }

    #[test]
    fn test_vec_norm() {
        let func = VecNormFunction;
        let v = Value::text("[3.0, 4.0]");
        let result = func.evaluate(&[v]).unwrap();
        if let Value::Float(f) = result {
            assert!((f - 5.0).abs() < 1e-10);
        } else {
            panic!("expected Float");
        }
    }

    #[test]
    fn test_vec_distance_l2() {
        let func = VecDistanceL2Function;
        let a = Value::text("[1.0, 0.0, 0.0]");
        let b = Value::text("[0.0, 1.0, 0.0]");
        let result = func.evaluate(&[a, b]).unwrap();
        if let Value::Float(f) = result {
            assert!((f - std::f64::consts::SQRT_2).abs() < 1e-10);
        } else {
            panic!("expected Float");
        }
    }

    #[test]
    fn test_vec_distance_null() {
        let func = VecDistanceL2Function;
        let a = Value::text("[1.0, 2.0]");
        let result = func.evaluate(&[a, Value::null_unknown()]).unwrap();
        assert!(result.is_null());
    }

    #[test]
    fn test_vec_distance_dimension_mismatch() {
        let func = VecDistanceL2Function;
        let a = Value::text("[1.0, 2.0]");
        let b = Value::text("[1.0, 2.0, 3.0]");
        assert!(func.evaluate(&[a, b]).is_err());
    }
}
