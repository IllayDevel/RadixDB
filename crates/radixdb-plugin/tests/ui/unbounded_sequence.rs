use radixdb_plugin::RadixType;

#[derive(Default, RadixType)]
#[radix_type(
    id = "unbounded",
    name = "unbounded",
    codec = 1,
    semantic_revision = 1,
    storage = "variable",
    max_bytes = 1024
)]
struct Unbounded {
    #[radix_field(codec = "u32-le")]
    values: Vec<u32>,
}

fn main() {}
