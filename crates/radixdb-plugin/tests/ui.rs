#[test]
fn invalid_authoring_contracts_fail_at_compile_time() {
    let tests = trybuild::TestCases::new();
    tests.compile_fail("tests/ui/*.rs");
}
