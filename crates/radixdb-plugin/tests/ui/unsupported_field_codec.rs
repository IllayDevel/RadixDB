use radixdb_plugin::RadixType;

#[derive(Default, RadixType)]
#[radix_type(
    id = "wrong_codec",
    name = "wrong_codec",
    codec = 1,
    semantic_revision = 1,
    storage = "fixed",
    max_bytes = 8
)]
struct WrongCodec {
    #[radix_field(codec = "native-endian")]
    value: u64,
}

fn main() {}
