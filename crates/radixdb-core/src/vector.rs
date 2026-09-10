// Copyright 2026 RadixDB Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

//! Allocation-free vector distance primitives shared by storage and SQL.

use crate::{Error, Result};

#[inline]
pub fn validate_vector_bytes(data: &[u8]) -> Result<()> {
    if !data.len().is_multiple_of(4) {
        return Err(Error::invalid_argument(format!(
            "malformed VECTOR payload: {} bytes is not aligned to f32",
            data.len()
        )));
    }
    Ok(())
}

#[inline]
fn validate_equal_bytes(a: &[u8], b: &[u8]) -> Result<()> {
    validate_vector_bytes(a)?;
    validate_vector_bytes(b)?;
    if a.len() != b.len() {
        return Err(Error::invalid_argument(format!(
            "vector dimension mismatch ({} vs {})",
            a.len() / 4,
            b.len() / 4
        )));
    }
    Ok(())
}

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

/// L2 (Euclidean) distance on raw little-endian f32 byte slices.
#[inline]
pub fn l2_distance_bytes(a: &[u8], b: &[u8]) -> Result<f64> {
    validate_equal_bytes(a, b)?;
    let len = a.len() / 4;
    let mut sum = 0.0f64;
    let mut index = 0;
    while index + 4 <= len {
        let d0 = (read_f32(a, index) - read_f32(b, index)) as f64;
        let d1 = (read_f32(a, index + 1) - read_f32(b, index + 1)) as f64;
        let d2 = (read_f32(a, index + 2) - read_f32(b, index + 2)) as f64;
        let d3 = (read_f32(a, index + 3) - read_f32(b, index + 3)) as f64;
        sum += d0 * d0 + d1 * d1 + d2 * d2 + d3 * d3;
        index += 4;
    }
    while index < len {
        let distance = (read_f32(a, index) - read_f32(b, index)) as f64;
        sum += distance * distance;
        index += 1;
    }
    Ok(sum.sqrt())
}

/// Cosine distance (`1 - cosine_similarity`) on raw LE f32 bytes.
#[inline]
pub fn cosine_distance_bytes(a: &[u8], b: &[u8]) -> Result<f64> {
    validate_equal_bytes(a, b)?;
    let len = a.len() / 4;
    let mut dot = 0.0f64;
    let mut norm_a = 0.0f64;
    let mut norm_b = 0.0f64;
    for index in 0..len {
        let ai = read_f32(a, index) as f64;
        let bi = read_f32(b, index) as f64;
        dot += ai * bi;
        norm_a += ai * ai;
        norm_b += bi * bi;
    }
    let denominator = norm_a.sqrt() * norm_b.sqrt();
    if denominator == 0.0 {
        Ok(1.0)
    } else {
        Ok((1.0 - (dot / denominator)).max(0.0))
    }
}

/// Negative inner-product distance on raw LE f32 bytes.
#[inline]
pub fn ip_distance_bytes(a: &[u8], b: &[u8]) -> Result<f64> {
    validate_equal_bytes(a, b)?;
    let len = a.len() / 4;
    let mut dot = 0.0f64;
    for index in 0..len {
        dot += (read_f32(a, index) as f64) * (read_f32(b, index) as f64);
    }
    Ok(-dot)
}

/// L2 (Euclidean) distance on decoded f32 slices.
#[inline]
pub fn l2_distance(a: &[f32], b: &[f32]) -> Result<f64> {
    if a.len() != b.len() {
        return Err(Error::invalid_argument(format!(
            "vector dimension mismatch ({} vs {})",
            a.len(),
            b.len()
        )));
    }
    let mut sum = 0.0f64;
    let len = a.len();
    let mut index = 0;
    while index + 4 <= len {
        let d0 = (a[index] - b[index]) as f64;
        let d1 = (a[index + 1] - b[index + 1]) as f64;
        let d2 = (a[index + 2] - b[index + 2]) as f64;
        let d3 = (a[index + 3] - b[index + 3]) as f64;
        sum += d0 * d0 + d1 * d1 + d2 * d2 + d3 * d3;
        index += 4;
    }
    while index < len {
        let distance = (a[index] - b[index]) as f64;
        sum += distance * distance;
        index += 1;
    }
    Ok(sum.sqrt())
}

/// Cosine distance on decoded f32 slices.
#[inline]
pub fn cosine_distance(a: &[f32], b: &[f32]) -> Result<f64> {
    if a.len() != b.len() {
        return Err(Error::invalid_argument(format!(
            "vector dimension mismatch ({} vs {})",
            a.len(),
            b.len()
        )));
    }
    let mut dot = 0.0f64;
    let mut norm_a = 0.0f64;
    let mut norm_b = 0.0f64;
    for index in 0..a.len() {
        let ai = a[index] as f64;
        let bi = b[index] as f64;
        dot += ai * bi;
        norm_a += ai * ai;
        norm_b += bi * bi;
    }
    let denominator = norm_a.sqrt() * norm_b.sqrt();
    if denominator == 0.0 {
        Ok(1.0)
    } else {
        Ok((1.0 - (dot / denominator)).max(0.0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encode(values: &[f32]) -> Vec<u8> {
        values
            .iter()
            .flat_map(|value| value.to_le_bytes())
            .collect()
    }

    #[test]
    fn byte_and_decoded_distances_are_identical() {
        let a = [1.0, -2.0, 3.5, 4.0, 9.0];
        let b = [-1.0, 2.0, 1.5, 8.0, 3.0];
        let encoded_a = encode(&a);
        let encoded_b = encode(&b);

        assert_eq!(
            l2_distance_bytes(&encoded_a, &encoded_b).unwrap(),
            l2_distance(&a, &b).unwrap()
        );
        assert_eq!(
            cosine_distance_bytes(&encoded_a, &encoded_b).unwrap(),
            cosine_distance(&a, &b).unwrap()
        );
    }

    #[test]
    fn malformed_and_mismatched_payloads_are_rejected() {
        assert!(l2_distance_bytes(&[0, 0, 0], &[0, 0, 0]).is_err());
        assert!(cosine_distance_bytes(&encode(&[1.0]), &encode(&[1.0, 2.0])).is_err());
        assert!(ip_distance_bytes(&encode(&[1.0]), &encode(&[1.0, 2.0])).is_err());
    }

    #[test]
    fn zero_vector_cosine_contract_is_stable() {
        assert_eq!(cosine_distance(&[0.0, 0.0], &[1.0, 2.0]).unwrap(), 1.0);
        assert_eq!(
            cosine_distance_bytes(&encode(&[0.0, 0.0]), &encode(&[1.0, 2.0])).unwrap(),
            1.0
        );
    }
}
