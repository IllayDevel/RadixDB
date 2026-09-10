use radixdb_plugin::prelude::*;

#[radixdb_plugin(
    id = "0199f8d2-7fb2-7c21-b5c1-6cb59c96b410",
    name = "radixdb_pair",
    version = "1.0.0"
)]
mod pair_plugin {
    use super::*;

    #[derive(Debug, Default, Clone, Copy, RadixType)]
    #[radix_type(
        id = "pair",
        name = "pair",
        codec = 1,
        semantic_revision = 1,
        storage = "fixed",
        max_bytes = 16,
        equality = pair_equal,
        hash = pair_hash,
        ordering = pair_compare
    )]
    struct Pair {
        #[radix_field(codec = "i64-le")]
        left: i64,
        #[radix_field(codec = "i64-le")]
        right: i64,
    }

    fn pair_equal(left: &Pair, right: &Pair) -> bool {
        left.left == right.left && left.right == right.right
    }

    fn pair_hash(value: &Pair, sink: &mut HashSink<'_>) -> PluginResult<()> {
        sink.i64(value.left)?;
        sink.i64(value.right)
    }

    fn pair_compare(left: &Pair, right: &Pair) -> std::cmp::Ordering {
        (left.left, left.right).cmp(&(right.left, right.right))
    }

    #[radixdb_scalar(
        id = "pair_sum",
        name = "pair_sum",
        semantic_revision = 1,
        immutable,
        strict,
        parallel_safe,
        cost = 1,
        cancellation = "bounded",
        max_output_bytes = 8
    )]
    fn pair_sum(value: Pair) -> PluginResult<i64> {
        value
            .left
            .checked_add(value.right)
            .ok_or_else(|| PluginError::domain("pair sum overflow"))
    }
}
