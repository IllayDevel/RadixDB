use radixdb_plugin::{HashSink, RadixType};

#[derive(Default, RadixType)]
#[radix_type(
    id = "bad_callback",
    name = "bad_callback",
    codec = 1,
    semantic_revision = 1,
    storage = "fixed",
    max_bytes = 8,
    equality = bad_equal
)]
struct BadCallback {
    #[radix_field(codec = "u64-le")]
    value: u64,
}

fn bad_equal(_left: &BadCallback, _sink: &mut HashSink<'_>) -> bool {
    true
}

fn main() {}
