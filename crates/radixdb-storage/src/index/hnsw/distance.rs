// ─────────────────────────────────────────────────────────────
// f32-slice distance functions (LLVM auto-vectorizable)
// ─────────────────────────────────────────────────────────────

/// Reinterpret packed LE f32 bytes as an &[f32] slice.
///
/// Only called with slices from the internal `Vec<u8>` buffer, which is guaranteed
/// to be aligned by the system allocator (>= 16 bytes) with 4-byte-aligned offsets.
/// The runtime alignment check provides defense-in-depth: if alignment is wrong,
/// falls back to a safe copy-based conversion rather than invoking UB.
#[inline]
pub(super) fn as_f32_slice(bytes: &[u8]) -> &[f32] {
    debug_assert!(bytes.len().is_multiple_of(4));
    // SAFETY: bytes length is a multiple of 4 (asserted above). The internal vectors buffer is
    // allocated by the system allocator (>= 16-byte alignment), so the 4-byte f32 alignment holds.
    let (prefix, floats, _suffix) = unsafe { bytes.align_to::<f32>() };
    if prefix.is_empty() {
        floats
    } else {
        // Should never happen for internal vectors, but return empty rather than UB.
        // Callers dealing with potentially-unaligned data should use bytes_to_f32_vec() instead.
        debug_assert!(false, "unaligned vector data passed to as_f32_slice");
        &[]
    }
}

/// Convert unaligned LE f32 bytes to a Vec<f32>.
/// Used for query bytes from Extension values which may not be 4-byte aligned.
#[inline]
pub(super) fn bytes_to_f32_vec(bytes: &[u8]) -> Vec<f32> {
    let count = bytes.len() / 4;
    let mut out = Vec::with_capacity(count);
    for i in 0..count {
        let off = i * 4;
        out.push(f32::from_le_bytes([
            bytes[off],
            bytes[off + 1],
            bytes[off + 2],
            bytes[off + 3],
        ]));
    }
    out
}

/// Software prefetch: hint the CPU to start loading `addr` into L1 cache.
/// Uses inline assembly on aarch64 (PRFM PLDL1KEEP), no-op on other architectures.
#[inline(always)]
pub(super) fn prefetch_read(addr: *const u8) {
    #[cfg(target_arch = "aarch64")]
    // SAFETY: PRFM is a hint instruction that cannot trap or fault on any address (including
    // invalid ones). It has no side effects beyond populating the cache prefetch buffer.
    unsafe {
        std::arch::asm!(
            "prfm pldl1keep, [{addr}]",
            addr = in(reg) addr,
            options(nostack, preserves_flags, readonly),
        );
    }
    #[cfg(not(target_arch = "aarch64"))]
    {
        let _ = addr;
    }
}

/// NEON-accelerated L2 squared distance.
/// `#[target_feature(enable = "neon")]` forces LLVM to inline all NEON intrinsics
/// into the function body instead of emitting them as separate function calls.
#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
#[inline]
unsafe fn l2_distance_sq_neon(ap: *const f32, bp: *const f32, n: usize) -> f32 {
    use std::arch::aarch64::*;
    let mut acc0 = vdupq_n_f32(0.0);
    let mut acc1 = vdupq_n_f32(0.0);
    let mut acc2 = vdupq_n_f32(0.0);
    let mut acc3 = vdupq_n_f32(0.0);
    let end16 = n & !15;
    let mut i = 0;
    while i < end16 {
        let d0 = vsubq_f32(vld1q_f32(ap.add(i)), vld1q_f32(bp.add(i)));
        acc0 = vfmaq_f32(acc0, d0, d0);
        let d1 = vsubq_f32(vld1q_f32(ap.add(i + 4)), vld1q_f32(bp.add(i + 4)));
        acc1 = vfmaq_f32(acc1, d1, d1);
        let d2 = vsubq_f32(vld1q_f32(ap.add(i + 8)), vld1q_f32(bp.add(i + 8)));
        acc2 = vfmaq_f32(acc2, d2, d2);
        let d3 = vsubq_f32(vld1q_f32(ap.add(i + 12)), vld1q_f32(bp.add(i + 12)));
        acc3 = vfmaq_f32(acc3, d3, d3);
        i += 16;
    }
    acc0 = vaddq_f32(vaddq_f32(acc0, acc1), vaddq_f32(acc2, acc3));
    while i + 4 <= n {
        let d = vsubq_f32(vld1q_f32(ap.add(i)), vld1q_f32(bp.add(i)));
        acc0 = vfmaq_f32(acc0, d, d);
        i += 4;
    }
    let mut result = vaddvq_f32(acc0);
    while i < n {
        let d = *ap.add(i) - *bp.add(i);
        result += d * d;
        i += 1;
    }
    result
}

// ---------------------------------------------------------------------------
// x86_64 AVX2 + FMA helpers
// ---------------------------------------------------------------------------

/// Horizontal sum of 8 f32 lanes in an `__m256` register.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
#[inline]
unsafe fn hsum_avx(v: std::arch::x86_64::__m256) -> f32 {
    use std::arch::x86_64::*;
    // hi = v[4..7], lo = v[0..3]
    let hi = _mm256_extractf128_ps(v, 1);
    let lo = _mm256_castps256_ps128(v);
    let sum128 = _mm_add_ps(lo, hi); // [0+4, 1+5, 2+6, 3+7]
    let shuf = _mm_movehdup_ps(sum128); // [1+5, 1+5, 3+7, 3+7]
    let sums = _mm_add_ps(sum128, shuf); // [01+45, -, 23+67, -]
    let high64 = _mm_movehl_ps(sums, sums); // [23+67, -, -, -]
    _mm_cvtss_f32(_mm_add_ss(sums, high64))
}

/// AVX2+FMA L2 squared distance.
/// 4 accumulators × 8 lanes = 32-wide unrolling with FMA.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2,fma")]
#[inline]
unsafe fn l2_distance_sq_avx2(ap: *const f32, bp: *const f32, n: usize) -> f32 {
    use std::arch::x86_64::*;
    let mut acc0 = _mm256_setzero_ps();
    let mut acc1 = _mm256_setzero_ps();
    let mut acc2 = _mm256_setzero_ps();
    let mut acc3 = _mm256_setzero_ps();
    let end32 = n & !31;
    let mut i = 0;
    while i < end32 {
        let d0 = _mm256_sub_ps(_mm256_loadu_ps(ap.add(i)), _mm256_loadu_ps(bp.add(i)));
        acc0 = _mm256_fmadd_ps(d0, d0, acc0);
        let d1 = _mm256_sub_ps(
            _mm256_loadu_ps(ap.add(i + 8)),
            _mm256_loadu_ps(bp.add(i + 8)),
        );
        acc1 = _mm256_fmadd_ps(d1, d1, acc1);
        let d2 = _mm256_sub_ps(
            _mm256_loadu_ps(ap.add(i + 16)),
            _mm256_loadu_ps(bp.add(i + 16)),
        );
        acc2 = _mm256_fmadd_ps(d2, d2, acc2);
        let d3 = _mm256_sub_ps(
            _mm256_loadu_ps(ap.add(i + 24)),
            _mm256_loadu_ps(bp.add(i + 24)),
        );
        acc3 = _mm256_fmadd_ps(d3, d3, acc3);
        i += 32;
    }
    // Merge 4 accumulators → 1
    acc0 = _mm256_add_ps(_mm256_add_ps(acc0, acc1), _mm256_add_ps(acc2, acc3));
    // 8-wide tail
    while i + 8 <= n {
        let d = _mm256_sub_ps(_mm256_loadu_ps(ap.add(i)), _mm256_loadu_ps(bp.add(i)));
        acc0 = _mm256_fmadd_ps(d, d, acc0);
        i += 8;
    }
    let mut result = hsum_avx(acc0);
    // Scalar remainder
    while i < n {
        let d = *ap.add(i) - *bp.add(i);
        result += d * d;
        i += 1;
    }
    result
}

/// AVX2+FMA cosine distance.
/// 3 quantity chains (dot, norm_a, norm_b) × 4 accumulators × 8 lanes = 32-wide.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2,fma")]
#[inline]
unsafe fn cosine_distance_avx2(ap: *const f32, bp: *const f32, n: usize) -> f32 {
    use std::arch::x86_64::*;
    let mut dot0 = _mm256_setzero_ps();
    let mut dot1 = _mm256_setzero_ps();
    let mut dot2 = _mm256_setzero_ps();
    let mut dot3 = _mm256_setzero_ps();
    let mut na0 = _mm256_setzero_ps();
    let mut na1 = _mm256_setzero_ps();
    let mut na2 = _mm256_setzero_ps();
    let mut na3 = _mm256_setzero_ps();
    let mut nb0 = _mm256_setzero_ps();
    let mut nb1 = _mm256_setzero_ps();
    let mut nb2 = _mm256_setzero_ps();
    let mut nb3 = _mm256_setzero_ps();
    let end32 = n & !31;
    let mut i = 0;
    while i < end32 {
        let a0 = _mm256_loadu_ps(ap.add(i));
        let b0 = _mm256_loadu_ps(bp.add(i));
        dot0 = _mm256_fmadd_ps(a0, b0, dot0);
        na0 = _mm256_fmadd_ps(a0, a0, na0);
        nb0 = _mm256_fmadd_ps(b0, b0, nb0);
        let a1 = _mm256_loadu_ps(ap.add(i + 8));
        let b1 = _mm256_loadu_ps(bp.add(i + 8));
        dot1 = _mm256_fmadd_ps(a1, b1, dot1);
        na1 = _mm256_fmadd_ps(a1, a1, na1);
        nb1 = _mm256_fmadd_ps(b1, b1, nb1);
        let a2 = _mm256_loadu_ps(ap.add(i + 16));
        let b2 = _mm256_loadu_ps(bp.add(i + 16));
        dot2 = _mm256_fmadd_ps(a2, b2, dot2);
        na2 = _mm256_fmadd_ps(a2, a2, na2);
        nb2 = _mm256_fmadd_ps(b2, b2, nb2);
        let a3 = _mm256_loadu_ps(ap.add(i + 24));
        let b3 = _mm256_loadu_ps(bp.add(i + 24));
        dot3 = _mm256_fmadd_ps(a3, b3, dot3);
        na3 = _mm256_fmadd_ps(a3, a3, na3);
        nb3 = _mm256_fmadd_ps(b3, b3, nb3);
        i += 32;
    }
    // Merge 4 accumulators → 1 per quantity
    dot0 = _mm256_add_ps(_mm256_add_ps(dot0, dot1), _mm256_add_ps(dot2, dot3));
    na0 = _mm256_add_ps(_mm256_add_ps(na0, na1), _mm256_add_ps(na2, na3));
    nb0 = _mm256_add_ps(_mm256_add_ps(nb0, nb1), _mm256_add_ps(nb2, nb3));
    // 8-wide tail
    while i + 8 <= n {
        let av = _mm256_loadu_ps(ap.add(i));
        let bv = _mm256_loadu_ps(bp.add(i));
        dot0 = _mm256_fmadd_ps(av, bv, dot0);
        na0 = _mm256_fmadd_ps(av, av, na0);
        nb0 = _mm256_fmadd_ps(bv, bv, nb0);
        i += 8;
    }
    let mut dot = hsum_avx(dot0);
    let mut norm_a = hsum_avx(na0);
    let mut norm_b = hsum_avx(nb0);
    // Scalar remainder
    while i < n {
        let av = *ap.add(i);
        let bv = *bp.add(i);
        dot += av * bv;
        norm_a += av * av;
        norm_b += bv * bv;
        i += 1;
    }
    let denom = (norm_a * norm_b).sqrt();
    if denom < f32::EPSILON {
        1.0
    } else {
        (1.0 - dot / denom).max(0.0)
    }
}

/// AVX2+FMA negative inner product distance.
/// 4 accumulators × 8 lanes = 32-wide FMA unrolling.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2,fma")]
#[inline]
unsafe fn ip_distance_avx2(ap: *const f32, bp: *const f32, n: usize) -> f32 {
    use std::arch::x86_64::*;
    let mut acc0 = _mm256_setzero_ps();
    let mut acc1 = _mm256_setzero_ps();
    let mut acc2 = _mm256_setzero_ps();
    let mut acc3 = _mm256_setzero_ps();
    let end32 = n & !31;
    let mut i = 0;
    while i < end32 {
        acc0 = _mm256_fmadd_ps(_mm256_loadu_ps(ap.add(i)), _mm256_loadu_ps(bp.add(i)), acc0);
        acc1 = _mm256_fmadd_ps(
            _mm256_loadu_ps(ap.add(i + 8)),
            _mm256_loadu_ps(bp.add(i + 8)),
            acc1,
        );
        acc2 = _mm256_fmadd_ps(
            _mm256_loadu_ps(ap.add(i + 16)),
            _mm256_loadu_ps(bp.add(i + 16)),
            acc2,
        );
        acc3 = _mm256_fmadd_ps(
            _mm256_loadu_ps(ap.add(i + 24)),
            _mm256_loadu_ps(bp.add(i + 24)),
            acc3,
        );
        i += 32;
    }
    // Merge 4 accumulators → 1
    acc0 = _mm256_add_ps(_mm256_add_ps(acc0, acc1), _mm256_add_ps(acc2, acc3));
    // 8-wide tail
    while i + 8 <= n {
        acc0 = _mm256_fmadd_ps(_mm256_loadu_ps(ap.add(i)), _mm256_loadu_ps(bp.add(i)), acc0);
        i += 8;
    }
    let mut dot = hsum_avx(acc0);
    // Scalar remainder
    while i < n {
        dot += *ap.add(i) * *bp.add(i);
        i += 1;
    }
    -dot
}

/// L2 squared distance on f32 slices.
///
/// AArch64: NEON FMA intrinsics (`vfmaq_f32`) with 16-wide unrolling — guaranteed FMA
/// that the compiler cannot emit from scalar code without `-ffast-math`.
/// x86_64: AVX2+FMA with 32-wide unrolling (runtime feature detection).
/// Other: 4-accumulator scalar loop with bounds-check-free pointer access.
#[inline(always)]
pub(super) fn l2_distance_sq_f32(a: &[f32], b: &[f32]) -> f32 {
    assert_eq!(a.len(), b.len(), "HNSW distance: vector length mismatch");

    #[cfg(target_arch = "aarch64")]
    {
        // SAFETY: a and b are valid f32 slices of equal length (asserted above); pointers and
        // length are derived from slice references. NEON is always available on aarch64.
        unsafe { l2_distance_sq_neon(a.as_ptr(), b.as_ptr(), a.len()) }
    }

    #[cfg(not(target_arch = "aarch64"))]
    {
        #[cfg(target_arch = "x86_64")]
        {
            if is_x86_feature_detected!("avx2") && is_x86_feature_detected!("fma") {
                // SAFETY: a and b are valid f32 slices of equal length; AVX2+FMA availability
                // is verified by is_x86_feature_detected at runtime.
                return unsafe { l2_distance_sq_avx2(a.as_ptr(), b.as_ptr(), a.len()) };
            }
        }
        let n = a.len();
        let ap = a.as_ptr();
        let bp = b.as_ptr();
        let mut s0 = 0.0f32;
        let mut s1 = 0.0f32;
        let mut s2 = 0.0f32;
        let mut s3 = 0.0f32;
        let end4 = n & !3;
        let mut i = 0;
        // SAFETY: ap and bp point to slices of length n; loop indices i..i+3 < end4 <= n, and
        // the tail loop uses i < n. All pointer offsets are within bounds.
        unsafe {
            while i < end4 {
                let d0 = *ap.add(i) - *bp.add(i);
                let d1 = *ap.add(i + 1) - *bp.add(i + 1);
                let d2 = *ap.add(i + 2) - *bp.add(i + 2);
                let d3 = *ap.add(i + 3) - *bp.add(i + 3);
                s0 += d0 * d0;
                s1 += d1 * d1;
                s2 += d2 * d2;
                s3 += d3 * d3;
                i += 4;
            }
            while i < n {
                let d = *ap.add(i) - *bp.add(i);
                s0 += d * d;
                i += 1;
            }
        }
        (s0 + s1) + (s2 + s3)
    }
}

/// NEON-accelerated cosine distance helper.
#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
#[inline]
unsafe fn cosine_distance_neon(ap: *const f32, bp: *const f32, n: usize) -> f32 {
    use std::arch::aarch64::*;
    let mut dot0 = vdupq_n_f32(0.0);
    let mut dot1 = vdupq_n_f32(0.0);
    let mut dot2 = vdupq_n_f32(0.0);
    let mut dot3 = vdupq_n_f32(0.0);
    let mut na0 = vdupq_n_f32(0.0);
    let mut na1 = vdupq_n_f32(0.0);
    let mut na2 = vdupq_n_f32(0.0);
    let mut na3 = vdupq_n_f32(0.0);
    let mut nb0 = vdupq_n_f32(0.0);
    let mut nb1 = vdupq_n_f32(0.0);
    let mut nb2 = vdupq_n_f32(0.0);
    let mut nb3 = vdupq_n_f32(0.0);
    let end16 = n & !15;
    let mut i = 0;
    while i < end16 {
        let a0 = vld1q_f32(ap.add(i));
        let b0 = vld1q_f32(bp.add(i));
        dot0 = vfmaq_f32(dot0, a0, b0);
        na0 = vfmaq_f32(na0, a0, a0);
        nb0 = vfmaq_f32(nb0, b0, b0);
        let a1 = vld1q_f32(ap.add(i + 4));
        let b1 = vld1q_f32(bp.add(i + 4));
        dot1 = vfmaq_f32(dot1, a1, b1);
        na1 = vfmaq_f32(na1, a1, a1);
        nb1 = vfmaq_f32(nb1, b1, b1);
        let a2 = vld1q_f32(ap.add(i + 8));
        let b2 = vld1q_f32(bp.add(i + 8));
        dot2 = vfmaq_f32(dot2, a2, b2);
        na2 = vfmaq_f32(na2, a2, a2);
        nb2 = vfmaq_f32(nb2, b2, b2);
        let a3 = vld1q_f32(ap.add(i + 12));
        let b3 = vld1q_f32(bp.add(i + 12));
        dot3 = vfmaq_f32(dot3, a3, b3);
        na3 = vfmaq_f32(na3, a3, a3);
        nb3 = vfmaq_f32(nb3, b3, b3);
        i += 16;
    }
    dot0 = vaddq_f32(vaddq_f32(dot0, dot1), vaddq_f32(dot2, dot3));
    na0 = vaddq_f32(vaddq_f32(na0, na1), vaddq_f32(na2, na3));
    nb0 = vaddq_f32(vaddq_f32(nb0, nb1), vaddq_f32(nb2, nb3));
    while i + 4 <= n {
        let av = vld1q_f32(ap.add(i));
        let bv = vld1q_f32(bp.add(i));
        dot0 = vfmaq_f32(dot0, av, bv);
        na0 = vfmaq_f32(na0, av, av);
        nb0 = vfmaq_f32(nb0, bv, bv);
        i += 4;
    }
    let mut dot = vaddvq_f32(dot0);
    let mut norm_a = vaddvq_f32(na0);
    let mut norm_b = vaddvq_f32(nb0);
    while i < n {
        let av = *ap.add(i);
        let bv = *bp.add(i);
        dot += av * bv;
        norm_a += av * av;
        norm_b += bv * bv;
        i += 1;
    }
    let denom = (norm_a * norm_b).sqrt();
    if denom < f32::EPSILON {
        1.0
    } else {
        // Clamp: f32 rounding can produce tiny negatives for near-identical vectors
        (1.0 - dot / denom).max(0.0)
    }
}

/// Cosine distance on f32 slices: 1 - dot(a,b)/(norm_a * norm_b)
/// Returns f32 in [0, 2] range.
///
/// AArch64: NEON FMA with 3 accumulator chains (dot, norm_a, norm_b), 16-wide.
/// x86_64: AVX2+FMA with 32-wide unrolling (runtime feature detection).
/// Other: 4-accumulator scalar loop per quantity.
#[inline(always)]
pub(super) fn cosine_distance_f32(a: &[f32], b: &[f32]) -> f32 {
    assert_eq!(a.len(), b.len(), "HNSW distance: vector length mismatch");

    #[cfg(target_arch = "aarch64")]
    {
        // SAFETY: a and b are valid f32 slices of equal length (asserted above); pointers and
        // length are derived from slice references. NEON is always available on aarch64.
        unsafe { cosine_distance_neon(a.as_ptr(), b.as_ptr(), a.len()) }
    }

    #[cfg(not(target_arch = "aarch64"))]
    {
        #[cfg(target_arch = "x86_64")]
        {
            if is_x86_feature_detected!("avx2") && is_x86_feature_detected!("fma") {
                // SAFETY: a and b are valid f32 slices of equal length; AVX2+FMA availability
                // is verified by is_x86_feature_detected at runtime.
                return unsafe { cosine_distance_avx2(a.as_ptr(), b.as_ptr(), a.len()) };
            }
        }
        let n = a.len();
        let ap = a.as_ptr();
        let bp = b.as_ptr();
        let (mut d0, mut d1, mut d2, mut d3) = (0.0f32, 0.0f32, 0.0f32, 0.0f32);
        let (mut a0, mut a1, mut a2, mut a3) = (0.0f32, 0.0f32, 0.0f32, 0.0f32);
        let (mut b0, mut b1, mut b2, mut b3) = (0.0f32, 0.0f32, 0.0f32, 0.0f32);
        let end4 = n & !3;
        let mut i = 0;
        // SAFETY: ap and bp point to slices of length n; loop indices i..i+3 < end4 <= n, and
        // the tail loop uses i < n. All pointer offsets are within bounds.
        unsafe {
            while i < end4 {
                let av0 = *ap.add(i);
                let av1 = *ap.add(i + 1);
                let av2 = *ap.add(i + 2);
                let av3 = *ap.add(i + 3);
                let bv0 = *bp.add(i);
                let bv1 = *bp.add(i + 1);
                let bv2 = *bp.add(i + 2);
                let bv3 = *bp.add(i + 3);
                d0 += av0 * bv0;
                d1 += av1 * bv1;
                d2 += av2 * bv2;
                d3 += av3 * bv3;
                a0 += av0 * av0;
                a1 += av1 * av1;
                a2 += av2 * av2;
                a3 += av3 * av3;
                b0 += bv0 * bv0;
                b1 += bv1 * bv1;
                b2 += bv2 * bv2;
                b3 += bv3 * bv3;
                i += 4;
            }
            while i < n {
                let av = *ap.add(i);
                let bv = *bp.add(i);
                d0 += av * bv;
                a0 += av * av;
                b0 += bv * bv;
                i += 1;
            }
        }
        let dot = (d0 + d1) + (d2 + d3);
        let norm_a = (a0 + a1) + (a2 + a3);
        let norm_b = (b0 + b1) + (b2 + b3);
        let denom = (norm_a * norm_b).sqrt();
        if denom < f32::EPSILON {
            1.0
        } else {
            (1.0 - dot / denom).max(0.0)
        }
    }
}

/// NEON-accelerated negative inner product helper.
#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
#[inline]
unsafe fn ip_distance_neon(ap: *const f32, bp: *const f32, n: usize) -> f32 {
    use std::arch::aarch64::*;
    let mut acc0 = vdupq_n_f32(0.0);
    let mut acc1 = vdupq_n_f32(0.0);
    let mut acc2 = vdupq_n_f32(0.0);
    let mut acc3 = vdupq_n_f32(0.0);
    let end16 = n & !15;
    let mut i = 0;
    while i < end16 {
        acc0 = vfmaq_f32(acc0, vld1q_f32(ap.add(i)), vld1q_f32(bp.add(i)));
        acc1 = vfmaq_f32(acc1, vld1q_f32(ap.add(i + 4)), vld1q_f32(bp.add(i + 4)));
        acc2 = vfmaq_f32(acc2, vld1q_f32(ap.add(i + 8)), vld1q_f32(bp.add(i + 8)));
        acc3 = vfmaq_f32(acc3, vld1q_f32(ap.add(i + 12)), vld1q_f32(bp.add(i + 12)));
        i += 16;
    }
    acc0 = vaddq_f32(vaddq_f32(acc0, acc1), vaddq_f32(acc2, acc3));
    while i + 4 <= n {
        acc0 = vfmaq_f32(acc0, vld1q_f32(ap.add(i)), vld1q_f32(bp.add(i)));
        i += 4;
    }
    let mut dot = vaddvq_f32(acc0);
    while i < n {
        dot += *ap.add(i) * *bp.add(i);
        i += 1;
    }
    -dot
}

/// Negative inner product distance on f32 slices: -dot(a,b)
/// HNSW minimizes distance, so -dot maximizes similarity.
///
/// AArch64: NEON FMA with 16-wide unrolling.
/// x86_64: AVX2+FMA with 32-wide unrolling (runtime feature detection).
/// Other: 4-accumulator scalar loop.
#[inline(always)]
pub(super) fn ip_distance_f32(a: &[f32], b: &[f32]) -> f32 {
    assert_eq!(a.len(), b.len(), "HNSW distance: vector length mismatch");

    #[cfg(target_arch = "aarch64")]
    {
        // SAFETY: a and b are valid f32 slices of equal length (asserted above); pointers and
        // length are derived from slice references. NEON is always available on aarch64.
        unsafe { ip_distance_neon(a.as_ptr(), b.as_ptr(), a.len()) }
    }

    #[cfg(not(target_arch = "aarch64"))]
    {
        #[cfg(target_arch = "x86_64")]
        {
            if is_x86_feature_detected!("avx2") && is_x86_feature_detected!("fma") {
                // SAFETY: a and b are valid f32 slices of equal length; AVX2+FMA availability
                // is verified by is_x86_feature_detected at runtime.
                return unsafe { ip_distance_avx2(a.as_ptr(), b.as_ptr(), a.len()) };
            }
        }
        let n = a.len();
        let ap = a.as_ptr();
        let bp = b.as_ptr();
        let mut s0 = 0.0f32;
        let mut s1 = 0.0f32;
        let mut s2 = 0.0f32;
        let mut s3 = 0.0f32;
        let end4 = n & !3;
        let mut i = 0;
        // SAFETY: ap and bp point to slices of length n; loop indices i..i+3 < end4 <= n, and
        // the tail loop uses i < n. All pointer offsets are within bounds.
        unsafe {
            while i < end4 {
                s0 += *ap.add(i) * *bp.add(i);
                s1 += *ap.add(i + 1) * *bp.add(i + 1);
                s2 += *ap.add(i + 2) * *bp.add(i + 2);
                s3 += *ap.add(i + 3) * *bp.add(i + 3);
                i += 4;
            }
            while i < n {
                s0 += *ap.add(i) * *bp.add(i);
                i += 1;
            }
        }
        -((s0 + s1) + (s2 + s3))
    }
}
