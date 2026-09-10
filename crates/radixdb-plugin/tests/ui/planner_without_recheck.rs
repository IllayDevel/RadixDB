use radixdb_plugin::prelude::*;

#[radixdb_plugin(
    id = "62a041a9-a33a-4609-b7d3-5c63f883d797",
    name = "bad_planner",
    version = "1.0.0"
)]
mod plugin {
    use super::*;

    #[derive(Debug, Default, RadixType)]
    #[radix_type(
        id = "key",
        name = "key",
        codec = 1,
        semantic_revision = 1,
        storage = "fixed",
        max_bytes = 8,
        equality = key_equal,
        hash = key_hash
    )]
    struct Key {
        #[radix_field(codec = "i64-le")]
        value: i64,
    }

    fn key_equal(left: &Key, right: &Key) -> bool {
        left.value == right.value
    }

    fn key_hash(value: &Key, sink: &mut HashSink<'_>) -> PluginResult<()> {
        sink.i64(value.value)
    }

    #[radixdb_scalar(
        id = "probe",
        name = "probe",
        semantic_revision = 1,
        immutable,
        strict,
        cost = 1,
        cancellation = "bounded"
    )]
    fn probe(value: Key) -> PluginResult<i64> {
        Ok(value.value)
    }

    #[radixdb_scalar(
        id = "key_eq",
        name = "key_eq",
        semantic_revision = 1,
        immutable,
        strict,
        cost = 1,
        cancellation = "bounded"
    )]
    fn key_eq(left: Key, right: Key) -> PluginResult<bool> {
        Ok(left.value == right.value)
    }

    #[radixdb_operator(
        id = "key_eq_operator",
        symbol = "=",
        semantic_revision = 1,
        function = "key_eq",
        left = Key,
        right = Key,
        result = bool
    )]
    #[allow(dead_code)]
    fn key_eq_operator() {}

    #[radixdb_operator_class(
        id = "key_hash",
        semantic_revision = 1,
        access_method = "hash",
        input = Key,
        key = i64,
        key_codec_revision = 1
    )]
    fn key(value: Key) -> PluginResult<i64> {
        Ok(value.value)
    }

    #[radixdb_planner_support(
        id = "bad_support",
        name = "bad_support",
        semantic_revision = 1,
        for_function = "probe",
        operator_class = "key_hash",
        max_spans = 4,
        max_output_bytes = 64
    )]
    fn support(
        _predicate: PredicateView<'_>,
        _output: &mut CandidatePlanBuilder<'_, '_>,
    ) -> PluginResult<()> {
        Ok(())
    }
}

fn main() {}
