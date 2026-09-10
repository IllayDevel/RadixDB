use radixdb_plugin::radixdb_plugin;

#[radixdb_plugin(
    id = "ed1f0c3e-c424-4f90-b8f1-145c0bb682af",
    name = "forbidden_storage",
    version = "1.0.0",
    storage_access
)]
mod forbidden_storage {}

fn main() {}
