use radixdb_plugin::prelude::*;

#[radixdb_plugin(
    id = "32dfed10-198c-473b-aa35-6c13c98adb2b",
    name = "duplicate_overload",
    version = "1.0.0"
)]
mod plugin {
    use super::*;

    #[radixdb_scalar(
        id = "first",
        name = "same_name",
        semantic_revision = 1,
        immutable,
        strict,
        cost = 1,
        cancellation = "bounded"
    )]
    fn first(value: i64) -> PluginResult<i64> {
        Ok(value)
    }

    #[radixdb_scalar(
        id = "second",
        name = "same_name",
        semantic_revision = 1,
        immutable,
        strict,
        cost = 1,
        cancellation = "bounded"
    )]
    fn second(value: i64) -> PluginResult<i64> {
        Ok(value)
    }
}

fn main() {}
