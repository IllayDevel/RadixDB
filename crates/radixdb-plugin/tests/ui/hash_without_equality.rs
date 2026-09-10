use radixdb_plugin::{HashSink, PluginResult, RadixType};

#[derive(Default, RadixType)]
#[radix_type(
    id = "bad_hash",
    name = "bad_hash",
    codec = 1,
    semantic_revision = 1,
    storage = "fixed",
    max_bytes = 8,
    hash = bad_hash
)]
struct BadHash {
    #[radix_field(codec = "u64-le")]
    value: u64,
}

fn bad_hash(value: &BadHash, sink: &mut HashSink<'_>) -> PluginResult<()> {
    sink.u64(value.value)
}

fn main() {}
