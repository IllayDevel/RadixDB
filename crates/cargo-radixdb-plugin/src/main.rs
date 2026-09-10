fn main() {
    if let Err(error) = cargo_radixdb_plugin::run() {
        eprintln!("cargo-radixdb-plugin: {error}");
        std::process::exit(1);
    }
}
