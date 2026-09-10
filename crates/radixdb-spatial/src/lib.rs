//! Spatial proving plugin for the public RadixDB plugin SDK.
//!
//! The implementation deliberately has no engine, catalog, executor, storage,
//! WAL, page, or MVCC dependency. It proves that external types, native
//! functions, core-owned indexes, and generic planner support compose without
//! a spatial branch in the database core.

use std::cmp::Ordering;

use radixdb_plugin::prelude::*;

pub const PACKAGE_ID: [u8; 16] = [
    0x8f, 0x45, 0x9a, 0x6d, 0x5c, 0x31, 0x4f, 0x5e, 0xa0, 0x3d, 0x41, 0x2c, 0x68, 0x1e, 0xf1, 0x20,
];
pub const PACKAGE_NAME: &str = "radixdb_spatial";
pub const PACKAGE_VERSION: &str = "1.0.0";

const MAX_POLYGON_POINTS: usize = 256;
const POLYGON_MAX_BYTES: usize = 4 + MAX_POLYGON_POINTS * 16;
const MAX_CANDIDATE_SPANS: usize = 1024;
const MAX_COVER_DEPTH: u32 = 28;
const MAX_COVER_NODES: usize = 100_000;

#[radixdb_plugin(
    id = "8f459a6d-5c31-4f5e-a03d-412c681ef120",
    name = "radixdb_spatial",
    version = "1.0.0"
)]
pub mod spatial {
    use super::*;

    #[derive(Debug, Default, Clone, Copy, RadixType)]
    #[radix_type(
        id = "point",
        name = "point",
        codec = 1,
        semantic_revision = 1,
        storage = "fixed",
        max_bytes = 16,
        equality = point_equal,
        hash = point_hash,
        ordering = point_compare
    )]
    pub struct Point {
        #[radix_field(codec = "f64-le")]
        pub x: f64,
        #[radix_field(codec = "f64-le")]
        pub y: f64,
    }

    #[derive(Debug, Default, Clone, Copy, RadixType)]
    #[radix_type(
        id = "box2d",
        name = "box2d",
        codec = 1,
        semantic_revision = 1,
        storage = "fixed",
        max_bytes = 32,
        equality = box_equal,
        hash = box_hash,
        ordering = box_compare
    )]
    pub struct Box2d {
        #[radix_field(codec = "f64-le")]
        pub min_x: f64,
        #[radix_field(codec = "f64-le")]
        pub min_y: f64,
        #[radix_field(codec = "f64-le")]
        pub max_x: f64,
        #[radix_field(codec = "f64-le")]
        pub max_y: f64,
    }

    #[derive(Debug, Default, Clone, RadixType)]
    #[radix_type(
        id = "polygon",
        name = "polygon",
        codec = 1,
        semantic_revision = 1,
        storage = "variable",
        max_bytes = 4100,
        equality = polygon_equal,
        hash = polygon_hash,
        ordering = polygon_compare,
        manual = PolygonCodec
    )]
    pub struct Polygon {
        pub points: Vec<Point>,
    }

    pub struct PolygonCodec;

    impl ManualCodec<Polygon> for PolygonCodec {
        fn encode(value: &Polygon, output: &mut CodecWriter) -> PluginResult<()> {
            if value.points.len() > MAX_POLYGON_POINTS {
                return Err(PluginError::limit_exceeded("polygon exceeds 256 vertices"));
            }
            output.write(&(value.points.len() as u32).to_le_bytes())?;
            for point in &value.points {
                output.write(&point.x.to_bits().to_le_bytes())?;
                output.write(&point.y.to_bits().to_le_bytes())?;
            }
            Ok(())
        }

        fn decode(input: &mut CodecReader<'_>) -> PluginResult<Polygon> {
            let count = u32::from_le_bytes(input.read(4)?.try_into().expect("fixed width"));
            let count = usize::try_from(count)
                .map_err(|_| PluginError::invalid_input("polygon vertex count overflow"))?;
            if count > MAX_POLYGON_POINTS {
                return Err(PluginError::limit_exceeded("polygon exceeds 256 vertices"));
            }
            let mut points = Vec::with_capacity(count);
            for _ in 0..count {
                let x = f64::from_bits(u64::from_le_bytes(
                    input.read(8)?.try_into().expect("fixed width"),
                ));
                let y = f64::from_bits(u64::from_le_bytes(
                    input.read(8)?.try_into().expect("fixed width"),
                ));
                points.push(Point { x, y });
            }
            Ok(Polygon { points })
        }

        fn corpus() -> Vec<Polygon> {
            vec![
                Polygon::default(),
                Polygon {
                    points: vec![
                        Point { x: 0.0, y: 0.0 },
                        Point { x: 4.0, y: 0.0 },
                        Point { x: 0.0, y: 3.0 },
                    ],
                },
                Polygon {
                    points: vec![Point { x: -1.0, y: 1.0 }; MAX_POLYGON_POINTS],
                },
            ]
        }
    }

    fn normalized(value: f64) -> f64 {
        if value.is_nan() {
            f64::from_bits(0x7ff8_0000_0000_0000)
        } else if value == 0.0 {
            0.0
        } else {
            value
        }
    }

    fn point_equal(left: &Point, right: &Point) -> bool {
        normalized(left.x).to_bits() == normalized(right.x).to_bits()
            && normalized(left.y).to_bits() == normalized(right.y).to_bits()
    }

    fn point_hash(value: &Point, sink: &mut HashSink<'_>) -> PluginResult<()> {
        sink.f64_bits(normalized(value.x))?;
        sink.f64_bits(normalized(value.y))
    }

    fn point_compare(left: &Point, right: &Point) -> Ordering {
        morton_bytes(*left).cmp(&morton_bytes(*right))
    }

    fn box_equal(left: &Box2d, right: &Box2d) -> bool {
        box_components(left)
            .into_iter()
            .zip(box_components(right))
            .all(|(left, right)| left.to_bits() == right.to_bits())
    }

    fn box_hash(value: &Box2d, sink: &mut HashSink<'_>) -> PluginResult<()> {
        for component in box_components(value) {
            sink.f64_bits(component)?;
        }
        Ok(())
    }

    fn box_compare(left: &Box2d, right: &Box2d) -> Ordering {
        box_components(left)
            .into_iter()
            .map(f64::to_bits)
            .cmp(box_components(right).into_iter().map(f64::to_bits))
    }

    fn box_components(value: &Box2d) -> [f64; 4] {
        [
            normalized(value.min_x),
            normalized(value.min_y),
            normalized(value.max_x),
            normalized(value.max_y),
        ]
    }

    fn polygon_equal(left: &Polygon, right: &Polygon) -> bool {
        left.points.len() == right.points.len()
            && left
                .points
                .iter()
                .zip(&right.points)
                .all(|(left, right)| point_equal(left, right))
    }

    fn polygon_hash(value: &Polygon, sink: &mut HashSink<'_>) -> PluginResult<()> {
        let mut canonical = Vec::with_capacity(POLYGON_MAX_BYTES);
        canonical.extend_from_slice(&(value.points.len() as u32).to_le_bytes());
        for point in &value.points {
            canonical.extend_from_slice(&normalized(point.x).to_bits().to_le_bytes());
            canonical.extend_from_slice(&normalized(point.y).to_bits().to_le_bytes());
        }
        sink.bytes(&canonical)
    }

    fn polygon_compare(left: &Polygon, right: &Polygon) -> Ordering {
        left.points
            .iter()
            .map(|point| morton_bytes(*point))
            .cmp(right.points.iter().map(|point| morton_bytes(*point)))
    }

    fn finite_point(point: Point) -> PluginResult<Point> {
        if point.x.is_finite() && point.y.is_finite() {
            Ok(point)
        } else {
            Err(PluginError::domain("spatial coordinates must be finite"))
        }
    }

    fn valid_box(value: Box2d) -> PluginResult<Box2d> {
        if ![value.min_x, value.min_y, value.max_x, value.max_y]
            .into_iter()
            .all(f64::is_finite)
        {
            return Err(PluginError::domain("box coordinates must be finite"));
        }
        if value.min_x > value.max_x || value.min_y > value.max_y {
            return Err(PluginError::domain("box minima must not exceed maxima"));
        }
        Ok(value)
    }

    #[radixdb_scalar(
        id = "distance",
        name = "st_distance",
        semantic_revision = 1,
        immutable,
        strict,
        parallel_safe,
        cost = 4,
        cancellation = "bounded",
        max_output_bytes = 8
    )]
    pub fn distance(left: Point, right: Point) -> PluginResult<f64> {
        let left = finite_point(left)?;
        let right = finite_point(right)?;
        Ok((left.x - right.x).hypot(left.y - right.y))
    }

    #[radixdb_batch(for_scalar = "distance", rows_per_cancel_check = 64)]
    pub fn distance_batch(
        left: ColumnView<'_, Point>,
        right: ColumnView<'_, Point>,
        output: &mut ColumnBuilder<'_, '_, f64>,
        context: &CallContext<'_>,
    ) -> PluginResult<()> {
        for (row, (left, right)) in left.zip(right).enumerate() {
            if row % 64 == 0 {
                context.check_cancelled()?;
            }
            output.push(distance(left?, right?)?)?;
        }
        Ok(())
    }

    #[radixdb_scalar(
        id = "contains",
        name = "st_contains",
        semantic_revision = 1,
        immutable,
        strict,
        parallel_safe,
        cost = 8,
        cancellation = "bounded",
        max_output_bytes = 1
    )]
    pub fn contains(polygon: Polygon, point: Point) -> PluginResult<bool> {
        let point = finite_point(point)?;
        for vertex in &polygon.points {
            finite_point(*vertex)?;
        }
        Ok(polygon_contains(&polygon.points, point))
    }

    #[radixdb_batch(for_scalar = "contains", rows_per_cancel_check = 32)]
    pub fn contains_batch(
        polygons: ColumnView<'_, Polygon>,
        points: ColumnView<'_, Point>,
        output: &mut ColumnBuilder<'_, '_, bool>,
        context: &CallContext<'_>,
    ) -> PluginResult<()> {
        for (row, (polygon, point)) in polygons.zip(points).enumerate() {
            if row % 32 == 0 {
                context.check_cancelled()?;
            }
            output.push(contains(polygon?, point?)?)?;
        }
        Ok(())
    }

    #[radixdb_scalar(
        id = "intersects",
        name = "st_intersects",
        semantic_revision = 1,
        immutable,
        strict,
        parallel_safe,
        cost = 2,
        cancellation = "bounded",
        max_output_bytes = 1
    )]
    pub fn intersects(left: Box2d, right: Box2d) -> PluginResult<bool> {
        let left = valid_box(left)?;
        let right = valid_box(right)?;
        Ok(left.min_x <= right.max_x
            && left.max_x >= right.min_x
            && left.min_y <= right.max_y
            && left.max_y >= right.min_y)
    }

    #[radixdb_batch(for_scalar = "intersects", rows_per_cancel_check = 64)]
    pub fn intersects_batch(
        left: ColumnView<'_, Box2d>,
        right: ColumnView<'_, Box2d>,
        output: &mut ColumnBuilder<'_, '_, bool>,
        context: &CallContext<'_>,
    ) -> PluginResult<()> {
        for (row, (left, right)) in left.zip(right).enumerate() {
            if row % 64 == 0 {
                context.check_cancelled()?;
            }
            output.push(intersects(left?, right?)?)?;
        }
        Ok(())
    }

    #[radixdb_scalar(
        id = "within_box",
        name = "st_within_box",
        semantic_revision = 1,
        immutable,
        strict,
        parallel_safe,
        cost = 2,
        cancellation = "bounded",
        max_output_bytes = 1
    )]
    pub fn within_box(point: Point, bounds: Box2d) -> PluginResult<bool> {
        let point = finite_point(point)?;
        let bounds = valid_box(bounds)?;
        Ok(point.x >= bounds.min_x
            && point.x <= bounds.max_x
            && point.y >= bounds.min_y
            && point.y <= bounds.max_y)
    }

    #[radixdb_scalar(
        id = "within_radius",
        name = "st_dwithin",
        semantic_revision = 1,
        immutable,
        strict,
        parallel_safe,
        cost = 5,
        cancellation = "bounded",
        max_output_bytes = 1
    )]
    pub fn within_radius(point: Point, center: Point, radius: f64) -> PluginResult<bool> {
        if !radius.is_finite() || radius < 0.0 {
            return Err(PluginError::domain(
                "radius must be finite and non-negative",
            ));
        }
        Ok(distance(point, center)? <= radius)
    }

    #[radixdb_scalar(
        id = "point_lt",
        name = "point_lt",
        semantic_revision = 1,
        immutable,
        strict,
        parallel_safe,
        cost = 1,
        cancellation = "bounded",
        max_output_bytes = 1
    )]
    fn point_lt(left: Point, right: Point) -> PluginResult<bool> {
        Ok(point_compare(&left, &right) == Ordering::Less)
    }

    #[radixdb_scalar(
        id = "point_le",
        name = "point_le",
        semantic_revision = 1,
        immutable,
        strict,
        parallel_safe,
        cost = 1,
        cancellation = "bounded",
        max_output_bytes = 1
    )]
    fn point_le(left: Point, right: Point) -> PluginResult<bool> {
        Ok(point_compare(&left, &right) != Ordering::Greater)
    }

    #[radixdb_scalar(
        id = "point_eq",
        name = "point_eq",
        semantic_revision = 1,
        immutable,
        strict,
        parallel_safe,
        cost = 1,
        cancellation = "bounded",
        max_output_bytes = 1
    )]
    fn point_eq(left: Point, right: Point) -> PluginResult<bool> {
        Ok(point_equal(&left, &right))
    }

    #[radixdb_scalar(
        id = "point_ge",
        name = "point_ge",
        semantic_revision = 1,
        immutable,
        strict,
        parallel_safe,
        cost = 1,
        cancellation = "bounded",
        max_output_bytes = 1
    )]
    fn point_ge(left: Point, right: Point) -> PluginResult<bool> {
        Ok(point_compare(&left, &right) != Ordering::Less)
    }

    #[radixdb_scalar(
        id = "point_gt",
        name = "point_gt",
        semantic_revision = 1,
        immutable,
        strict,
        parallel_safe,
        cost = 1,
        cancellation = "bounded",
        max_output_bytes = 1
    )]
    fn point_gt(left: Point, right: Point) -> PluginResult<bool> {
        Ok(point_compare(&left, &right) == Ordering::Greater)
    }

    #[radixdb_operator(
        id = "point_lt_operator",
        symbol = "<",
        semantic_revision = 1,
        function = "point_lt",
        left = Point,
        right = Point,
        result = bool
    )]
    #[allow(dead_code)]
    fn point_lt_operator() {}

    #[radixdb_operator(
        id = "point_le_operator",
        symbol = "<=",
        semantic_revision = 1,
        function = "point_le",
        left = Point,
        right = Point,
        result = bool
    )]
    #[allow(dead_code)]
    fn point_le_operator() {}

    #[radixdb_operator(
        id = "point_eq_operator",
        symbol = "=",
        semantic_revision = 1,
        function = "point_eq",
        left = Point,
        right = Point,
        result = bool
    )]
    #[allow(dead_code)]
    fn point_eq_operator() {}

    #[radixdb_operator(
        id = "point_ge_operator",
        symbol = ">=",
        semantic_revision = 1,
        function = "point_ge",
        left = Point,
        right = Point,
        result = bool
    )]
    #[allow(dead_code)]
    fn point_ge_operator() {}

    #[radixdb_operator(
        id = "point_gt_operator",
        symbol = ">",
        semantic_revision = 1,
        function = "point_gt",
        left = Point,
        right = Point,
        result = bool
    )]
    #[allow(dead_code)]
    fn point_gt_operator() {}

    #[radixdb_operator_class(
        id = "point_morton_btree",
        semantic_revision = 1,
        access_method = "btree",
        input = Point,
        key = BoundedBytes::<16>,
        key_codec_revision = 1
    )]
    pub fn point_morton_key(point: Point) -> PluginResult<BoundedBytes<16>> {
        BoundedBytes::new(morton_bytes(point).to_vec())
    }

    #[radixdb_planner_support(
        id = "within_box_support",
        name = "st_within_box_support",
        semantic_revision = 1,
        for_function = "within_box",
        operator_class = "point_morton_btree",
        always_recheck,
        max_spans = 1024,
        max_output_bytes = 49152
    )]
    fn within_box_support(
        predicate: PredicateView<'_>,
        output: &mut CandidatePlanBuilder<'_, '_>,
    ) -> PluginResult<()> {
        if predicate.indexed_argument()? != 0 || predicate.argument_count()? != 2 {
            return Err(PluginError::invalid_input(
                "st_within_box support requires indexed point argument 0",
            ));
        }
        let bounds = predicate
            .constant::<Box2d>(1)?
            .ok_or_else(|| PluginError::domain("NULL box has no candidate range"))?;
        publish_cover(valid_box(bounds)?, output)
    }

    #[radixdb_planner_support(
        id = "within_radius_support",
        name = "st_dwithin_support",
        semantic_revision = 1,
        for_function = "within_radius",
        operator_class = "point_morton_btree",
        always_recheck,
        max_spans = 1024,
        max_output_bytes = 49152
    )]
    fn within_radius_support(
        predicate: PredicateView<'_>,
        output: &mut CandidatePlanBuilder<'_, '_>,
    ) -> PluginResult<()> {
        if predicate.indexed_argument()? != 0 || predicate.argument_count()? != 3 {
            return Err(PluginError::invalid_input(
                "st_dwithin support requires indexed point argument 0",
            ));
        }
        let center = predicate
            .constant::<Point>(1)?
            .ok_or_else(|| PluginError::domain("NULL center has no candidate range"))?;
        let radius = predicate
            .constant::<f64>(2)?
            .ok_or_else(|| PluginError::domain("NULL radius has no candidate range"))?;
        let center = finite_point(center)?;
        if !radius.is_finite() || radius < 0.0 {
            return Err(PluginError::domain(
                "radius must be finite and non-negative",
            ));
        }
        publish_cover(
            Box2d {
                min_x: center.x - radius,
                min_y: center.y - radius,
                max_x: center.x + radius,
                max_y: center.y + radius,
            },
            output,
        )
    }

    fn publish_cover(bounds: Box2d, output: &mut CandidatePlanBuilder<'_, '_>) -> PluginResult<()> {
        let spans = candidate_spans(bounds)?;
        // The callback does not own table statistics. `1` means "selective
        // candidate plan available", not an exact cardinality claim; the
        // mandatory residual recheck remains the correctness authority.
        output.set_estimate(1, spans.len() as u32)?;
        for span in spans {
            output.push_span(span)?;
        }
        Ok(())
    }

    pub fn candidate_spans(bounds: Box2d) -> PluginResult<Vec<CandidateSpan>> {
        let bounds = valid_box(bounds)?;
        let query = IntegerBox {
            min_x: sortable_bits(bounds.min_x),
            min_y: sortable_bits(bounds.min_y),
            max_x: sortable_bits(bounds.max_x),
            max_y: sortable_bits(bounds.max_y),
        };
        let mut remaining_nodes = MAX_COVER_NODES;
        let mut spans = cover_node(0, 0, 0, query, &mut remaining_nodes);
        spans.sort_by(|left, right| left.start.cmp(&right.start));
        Ok(spans)
    }

    fn polygon_contains(points: &[Point], point: Point) -> bool {
        if points.len() < 3 {
            return false;
        }
        let mut inside = false;
        let mut previous = points[points.len() - 1];
        for &current in points {
            if point_on_segment(previous, current, point) {
                return true;
            }
            let crosses = (current.y > point.y) != (previous.y > point.y)
                && point.x
                    < (previous.x - current.x) * (point.y - current.y) / (previous.y - current.y)
                        + current.x;
            if crosses {
                inside = !inside;
            }
            previous = current;
        }
        inside
    }

    fn point_on_segment(left: Point, right: Point, point: Point) -> bool {
        let cross =
            (point.y - left.y) * (right.x - left.x) - (point.x - left.x) * (right.y - left.y);
        if cross.abs() > f64::EPSILON * 16.0 {
            return false;
        }
        point.x >= left.x.min(right.x)
            && point.x <= left.x.max(right.x)
            && point.y >= left.y.min(right.y)
            && point.y <= left.y.max(right.y)
    }

    fn sortable_bits(value: f64) -> u64 {
        let bits = normalized(value).to_bits();
        if bits & (1_u64 << 63) != 0 {
            !bits
        } else {
            bits ^ (1_u64 << 63)
        }
    }

    pub fn morton_bytes(point: Point) -> [u8; 16] {
        let x = sortable_bits(point.x);
        let y = sortable_bits(point.y);
        let mut morton = 0_u128;
        for bit in (0..64).rev() {
            morton = (morton << 1) | u128::from((x >> bit) & 1);
            morton = (morton << 1) | u128::from((y >> bit) & 1);
        }
        morton.to_be_bytes()
    }

    #[derive(Clone, Copy)]
    struct IntegerBox {
        min_x: u64,
        min_y: u64,
        max_x: u64,
        max_y: u64,
    }

    fn cover_node(
        depth: u32,
        x_prefix: u64,
        y_prefix: u64,
        query: IntegerBox,
        remaining_nodes: &mut usize,
    ) -> Vec<CandidateSpan> {
        if *remaining_nodes == 0 {
            return vec![prefix_span(depth, x_prefix, y_prefix)];
        }
        *remaining_nodes -= 1;
        let node = prefix_box(depth, x_prefix, y_prefix);
        if !integer_boxes_intersect(node, query) {
            return Vec::new();
        }
        if integer_box_contains(query, node) || depth == MAX_COVER_DEPTH {
            return vec![prefix_span(depth, x_prefix, y_prefix)];
        }

        let mut children = Vec::new();
        for x_bit in 0..=1 {
            for y_bit in 0..=1 {
                children.extend(cover_node(
                    depth + 1,
                    (x_prefix << 1) | x_bit,
                    (y_prefix << 1) | y_bit,
                    query,
                    remaining_nodes,
                ));
                if children.len() > MAX_CANDIDATE_SPANS {
                    return vec![prefix_span(depth, x_prefix, y_prefix)];
                }
            }
        }
        children
    }

    fn prefix_box(depth: u32, x_prefix: u64, y_prefix: u64) -> IntegerBox {
        if depth == 0 {
            return IntegerBox {
                min_x: 0,
                min_y: 0,
                max_x: u64::MAX,
                max_y: u64::MAX,
            };
        }
        let suffix_bits = 64 - depth;
        let suffix_mask = (1_u64 << suffix_bits) - 1;
        IntegerBox {
            min_x: x_prefix << suffix_bits,
            min_y: y_prefix << suffix_bits,
            max_x: (x_prefix << suffix_bits) | suffix_mask,
            max_y: (y_prefix << suffix_bits) | suffix_mask,
        }
    }

    fn integer_boxes_intersect(left: IntegerBox, right: IntegerBox) -> bool {
        left.min_x <= right.max_x
            && left.max_x >= right.min_x
            && left.min_y <= right.max_y
            && left.max_y >= right.min_y
    }

    fn integer_box_contains(outer: IntegerBox, inner: IntegerBox) -> bool {
        outer.min_x <= inner.min_x
            && outer.max_x >= inner.max_x
            && outer.min_y <= inner.min_y
            && outer.max_y >= inner.max_y
    }

    fn prefix_span(depth: u32, x_prefix: u64, y_prefix: u64) -> CandidateSpan {
        let mut prefix = 0_u128;
        for bit in (0..depth).rev() {
            prefix = (prefix << 1) | u128::from((x_prefix >> bit) & 1);
            prefix = (prefix << 1) | u128::from((y_prefix >> bit) & 1);
        }
        let suffix_bits = 128 - depth * 2;
        let start = if suffix_bits == 128 {
            0
        } else {
            prefix << suffix_bits
        };
        let end = if suffix_bits == 128 {
            u128::MAX
        } else {
            start | ((1_u128 << suffix_bits) - 1)
        };
        CandidateSpan {
            start: start.to_be_bytes().to_vec(),
            end: end.to_be_bytes().to_vec(),
        }
    }
}

pub use spatial::{Box2d, Point, Polygon};

pub fn descriptor() -> &'static radixdb_plugin::__private::abi::RadixPluginDescriptorV1 {
    spatial::__radixdb_descriptor()
}

pub fn encode<T: RadixType>(value: &T) -> PluginResult<Vec<u8>> {
    let mut output = CodecWriter::new(T::MAX_BYTES as usize);
    value.encode(&mut output)?;
    Ok(output.into_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn descriptor_and_all_three_codecs_pass_public_sdk_gates() {
        radixdb_plugin::testing::validate_descriptor_graph(descriptor()).unwrap();
        assert_eq!(descriptor().type_count, 3);
        assert_eq!(descriptor().planner_support_count, 2);
        radixdb_plugin::testing::check_type::<Point>().unwrap();
        radixdb_plugin::testing::check_type::<Box2d>().unwrap();
        radixdb_plugin::testing::check_type::<Polygon>().unwrap();
    }

    #[test]
    fn geometry_edge_cases_are_explicit() {
        let triangle = Polygon {
            points: vec![
                Point { x: 0.0, y: 0.0 },
                Point { x: 4.0, y: 0.0 },
                Point { x: 0.0, y: 4.0 },
            ],
        };
        assert!(spatial::contains(triangle.clone(), Point { x: 1.0, y: 1.0 }).unwrap());
        assert!(spatial::contains(triangle.clone(), Point { x: 2.0, y: 0.0 }).unwrap());
        assert!(!spatial::contains(triangle, Point { x: 4.0, y: 4.0 }).unwrap());
        assert!(spatial::intersects(
            Box2d {
                min_x: 0.0,
                min_y: 0.0,
                max_x: 1.0,
                max_y: 1.0,
            },
            Box2d {
                min_x: 1.0,
                min_y: 1.0,
                max_x: 2.0,
                max_y: 2.0,
            },
        )
        .unwrap());
        assert!(
            spatial::distance(Point { x: 0.0, y: 0.0 }, Point { x: 3.0, y: 4.0 }).unwrap() == 5.0
        );
        assert!(spatial::distance(
            Point {
                x: f64::NAN,
                y: 0.0,
            },
            Point::default(),
        )
        .is_err());
    }

    #[test]
    fn morton_cover_has_no_false_negatives_and_is_bounded() {
        let bounds = Box2d {
            min_x: 1002.5,
            min_y: 2001.25,
            max_x: 1003.75,
            max_y: 2008.5,
        };
        let spans = spatial::candidate_spans(bounds).unwrap();
        assert!(!spans.is_empty());
        assert!(spans.len() <= MAX_CANDIDATE_SPANS);
        for x in 10025..=10037 {
            for y in 20013..=20085 {
                let point = Point {
                    x: f64::from(x) / 10.0,
                    y: f64::from(y) / 10.0,
                };
                let key = spatial::morton_bytes(point);
                assert!(spans
                    .iter()
                    .any(|span| span.start.as_slice() <= key.as_slice()
                        && key.as_slice() <= span.end.as_slice()));
            }
        }
    }

    #[test]
    fn deterministic_differential_cover_matches_exact_scan() {
        let queries = [
            Box2d {
                min_x: -750.0,
                min_y: -500.0,
                max_x: -125.0,
                max_y: 300.0,
            },
            Box2d {
                min_x: -0.0,
                min_y: -0.0,
                max_x: 0.0,
                max_y: 0.0,
            },
            Box2d {
                min_x: 100.0,
                min_y: 200.0,
                max_x: 700.0,
                max_y: 850.0,
            },
        ];
        let mut state = 0x6a09_e667_f3bc_c909_u64;
        let points = (0..65_536)
            .map(|_| {
                state = state
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1_442_695_040_888_963_407);
                let x = ((state >> 11) as f64 / ((1_u64 << 53) as f64)) * 2_000.0 - 1_000.0;
                state = state
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1_442_695_040_888_963_407);
                let y = ((state >> 11) as f64 / ((1_u64 << 53) as f64)) * 2_000.0 - 1_000.0;
                Point { x, y }
            })
            .chain([Point { x: -0.0, y: 0.0 }, Point { x: 0.0, y: -0.0 }])
            .collect::<Vec<_>>();

        for query in queries {
            let spans = spatial::candidate_spans(query).unwrap();
            for point in &points {
                let exact = spatial::within_box(*point, query).unwrap();
                let key = spatial::morton_bytes(*point);
                let candidate = spans.iter().any(|span| {
                    span.start.as_slice() <= key.as_slice() && key.as_slice() <= span.end.as_slice()
                });
                assert!(!exact || candidate, "candidate cover lost point {point:?}");
            }
        }
    }

    #[test]
    fn scalar_and_batch_results_match_for_all_primitives() {
        use radixdb_plugin::testing::{invoke_batch, invoke_scalar, TestValue};

        let point_rows = [
            (Point { x: 0.0, y: 0.0 }, Point { x: 3.0, y: 4.0 }),
            (Point { x: -2.0, y: 7.0 }, Point { x: 1.0, y: 3.0 }),
        ];
        let rows = point_rows
            .iter()
            .map(|(left, right)| {
                Ok(vec![
                    TestValue::external(descriptor(), left)?,
                    TestValue::external(descriptor(), right)?,
                ])
            })
            .collect::<PluginResult<Vec<_>>>()
            .unwrap();
        assert_batch_parity("distance", &rows);

        let polygon = Polygon {
            points: vec![
                Point { x: 0.0, y: 0.0 },
                Point { x: 4.0, y: 0.0 },
                Point { x: 0.0, y: 4.0 },
            ],
        };
        let rows = [Point { x: 1.0, y: 1.0 }, Point { x: 5.0, y: 5.0 }]
            .iter()
            .map(|point| {
                Ok(vec![
                    TestValue::external(descriptor(), &polygon)?,
                    TestValue::external(descriptor(), point)?,
                ])
            })
            .collect::<PluginResult<Vec<_>>>()
            .unwrap();
        assert_batch_parity("contains", &rows);

        let boxes = [
            Box2d {
                min_x: 0.0,
                min_y: 0.0,
                max_x: 2.0,
                max_y: 2.0,
            },
            Box2d {
                min_x: 5.0,
                min_y: 5.0,
                max_x: 6.0,
                max_y: 6.0,
            },
        ];
        let probe = Box2d {
            min_x: 1.0,
            min_y: 1.0,
            max_x: 3.0,
            max_y: 3.0,
        };
        let rows = boxes
            .iter()
            .map(|bounds| {
                Ok(vec![
                    TestValue::external(descriptor(), bounds)?,
                    TestValue::external(descriptor(), &probe)?,
                ])
            })
            .collect::<PluginResult<Vec<_>>>()
            .unwrap();
        assert_batch_parity("intersects", &rows);

        fn assert_batch_parity(name: &str, rows: &[Vec<TestValue>]) {
            let batch = invoke_batch(descriptor(), name, rows, Default::default()).unwrap();
            assert!(batch.finished);
            for (index, arguments) in rows.iter().enumerate() {
                let scalar =
                    invoke_scalar(descriptor(), name, arguments, Default::default()).unwrap();
                assert_eq!(scalar.outputs.as_slice(), &batch.outputs[index..=index]);
            }
        }
    }

    #[test]
    fn authoring_crate_has_only_the_public_sdk_dependency() {
        let manifest = include_str!("../Cargo.toml");
        let dependencies = manifest
            .split("[dependencies]")
            .nth(1)
            .unwrap()
            .split('[')
            .next()
            .unwrap();
        assert_eq!(
            dependencies
                .lines()
                .filter(|line| !line.trim().is_empty())
                .count(),
            1
        );
        assert!(dependencies.contains("radixdb-plugin"));
        for forbidden in [
            "radixdb-storage",
            "radixdb-executor",
            "radixdb-catalog",
            "radixdb-core",
            "radixdb-plugin-host",
            "radixdb-plugin-abi",
        ] {
            assert!(!dependencies.contains(forbidden));
        }
    }
}
