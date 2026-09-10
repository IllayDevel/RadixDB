#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MANIFEST="$ROOT/crates/radixdb-plugin/tests/fixtures/proof-plugin/Cargo.toml"
TOOL="$ROOT/target/debug/cargo-radixdb-plugin"
IMAGE="docker.io/library/rust:1.97.0-bookworm"
ATTESTATION="rust:1.97.0-bookworm"
WORK="$(mktemp -d "$ROOT/target/plugin-package-tooling.XXXXXX")"
EVIDENCE="$ROOT/target/v1.2-evidence/plugin-security"

cleanup() {
    case "$WORK" in
        "$ROOT"/target/plugin-package-tooling.*) rm -rf -- "$WORK" ;;
        *) printf 'refusing unsafe cleanup path: %s\n' "$WORK" >&2 ;;
    esac
}
trap cleanup EXIT

expect_failure() {
    local label="$1"
    shift
    if "$@" >"$WORK/$label.stdout" 2>"$WORK/$label.stderr"; then
        printf 'expected failure succeeded: %s\n' "$label" >&2
        return 1
    fi
}

cargo build --locked -p cargo-radixdb-plugin
"$TOOL" check --manifest-path "$MANIFEST" --target-dir "$WORK/native-check"
"$TOOL" build --manifest-path "$MANIFEST" --target-dir "$WORK/native-build"
LIBRARY="$WORK/native-build/x86_64-unknown-linux-gnu/release/libradixdb_sdk_proof_plugin.so"
"$TOOL" inspect --library "$LIBRARY" >"$WORK/raw-inspect.json"
"$TOOL" test-host --manifest-path "$MANIFEST" --target-dir "$WORK/native-host"

cp -a "${MANIFEST%/*}" "$WORK/abort-project"
sed -i "s#path = \"../../..\"#path = \"$ROOT/crates/radixdb-plugin\"#" \
    "$WORK/abort-project/Cargo.toml"
printf '\n[profile.release]\npanic = "abort"\n' >>"$WORK/abort-project/Cargo.toml"
expect_failure aborting-release-profile \
    "$TOOL" check --manifest-path "$WORK/abort-project/Cargo.toml"

expect_failure package-without-attestation \
    "$TOOL" package --manifest-path "$MANIFEST" --output-dir "$WORK/forbidden-a"
expect_failure package-with-spoofed-host-attestation \
    env RADIXDB_PLUGIN_BUILD_IMAGE="$ATTESTATION" \
    "$TOOL" package --manifest-path "$MANIFEST" --output-dir "$WORK/forbidden-b"

cat >"$WORK/crashing.c" <<'EOF'
__attribute__((visibility("default")))
void *radixdb_plugin_entry_v1(const void *host, unsigned int *status) {
    (void)host;
    (void)status;
    __builtin_trap();
}
EOF
cc -shared -fPIC -fvisibility=hidden -Wl,--build-id=none \
    -o "$WORK/libcrashing.so" "$WORK/crashing.c"
expect_failure isolated-crashing-entrypoint \
    "$TOOL" inspect --library "$WORK/libcrashing.so"

mkdir "$WORK/version-only" "$WORK/undeclared-change"
cp "$MANIFEST" "$WORK/version-only/Cargo.toml"
cp "${MANIFEST%/*}/Cargo.lock" "$WORK/version-only/Cargo.lock"
cp "${MANIFEST%/*}/radixdb-plugin-golden.toml" \
    "$WORK/version-only/radixdb-plugin-golden.toml"
mkdir "$WORK/version-only/src"
cp "${MANIFEST%/*}/src/lib.rs" "$WORK/version-only/src/lib.rs"
sed -i 's/version = "1.0.0"/version = "1.0.1"/' \
    "$WORK/version-only/Cargo.toml" "$WORK/version-only/src/lib.rs"
sed -i 's#path = "../../.."#path = "/workspace/crates/radixdb-plugin"#' \
    "$WORK/version-only/Cargo.toml"
cp -a "$WORK/version-only/." "$WORK/undeclared-change/"
sed -i 's/checked_add/checked_sub/' "$WORK/undeclared-change/src/lib.rs"

WORK_REL="${WORK#"$ROOT"/}"
podman run --rm --security-opt label=disable --userns=keep-id \
    -e RADIXDB_PLUGIN_BUILD_IMAGE="$ATTESTATION" \
    -v "$ROOT:/workspace" -w /workspace "$IMAGE" sh -c "
set -eu
cargo run --locked -p cargo-radixdb-plugin -- package \
  --manifest-path crates/radixdb-plugin/tests/fixtures/proof-plugin/Cargo.toml \
  --target-dir '$WORK_REL/container-a' \
  --output-dir '$WORK_REL/package-a'
cargo run --locked -p cargo-radixdb-plugin -- package \
  --manifest-path crates/radixdb-plugin/tests/fixtures/proof-plugin/Cargo.toml \
  --target-dir '$WORK_REL/container-b' \
  --output-dir '$WORK_REL/package-b'
cargo generate-lockfile --manifest-path '$WORK_REL/version-only/Cargo.toml'
cargo run --locked -p cargo-radixdb-plugin -- package \
  --manifest-path '$WORK_REL/version-only/Cargo.toml' \
  --target-dir '$WORK_REL/container-version' \
  --previous-package '$WORK_REL/package-a' \
  --output-dir '$WORK_REL/package-version'
cargo generate-lockfile --manifest-path '$WORK_REL/undeclared-change/Cargo.toml'
if cargo run --locked -p cargo-radixdb-plugin -- package \
  --manifest-path '$WORK_REL/undeclared-change/Cargo.toml' \
  --target-dir '$WORK_REL/container-undeclared' \
  --previous-package '$WORK_REL/package-a' \
  --output-dir '$WORK_REL/package-undeclared'; then
    echo 'undeclared semantic change was accepted' >&2
    exit 41
fi
"

(
    cd "$WORK/package-a"
    find . -type f -print0 | sort -z | xargs -0 sha256sum
) >"$WORK/package-a.sha256"
(
    cd "$WORK/package-b"
    find . -type f -print0 | sort -z | xargs -0 sha256sum
) >"$WORK/package-b.sha256"
diff -u "$WORK/package-a.sha256" "$WORK/package-b.sha256"

"$TOOL" inspect --package "$WORK/package-a" >"$WORK/package-inspect.json"
"$TOOL" inspect --package "$WORK/package-version" \
    >"$WORK/package-version-inspect.json"

mkdir -p "$EVIDENCE"
cp "$WORK/package-inspect.json" "$EVIDENCE/package-n-minus-1.json"
cp "$WORK/package-version-inspect.json" "$EVIDENCE/package-n.json"
{
    printf 'package N-1 library: '
    sha256sum "$WORK/package-a/lib/libsdk_proof.so"
    printf 'package N library: '
    sha256sum "$WORK/package-version/lib/libsdk_proof.so"
} | tee "$EVIDENCE/artifact-identities.txt"

for case_name in library manifest provenance golden compatibility extra-file; do
    cp -a "$WORK/package-a" "$WORK/tamper-$case_name"
done
printf '\001' >>"$WORK/tamper-library/lib/libsdk_proof.so"
sed -i 's/panic_strategy = "unwind"/panic_strategy = "abort"/' \
    "$WORK/tamper-manifest/radixdb-plugin.toml"
sed -i 's/panic_strategy = "unwind"/panic_strategy = "abort"/' \
    "$WORK/tamper-provenance/radixdb-plugin-provenance.toml"
sed -i 's/0100000000000000ffffffffffffffff/0200000000000000ffffffffffffffff/' \
    "$WORK/tamper-golden/radixdb-plugin-golden.toml"
sed -i 's/compatible = true/compatible = false/' \
    "$WORK/tamper-compatibility/radixdb-plugin-compatibility.toml"
touch "$WORK/tamper-extra-file/undeclared"

expect_failure tampered-library "$TOOL" inspect --package "$WORK/tamper-library"
expect_failure tampered-manifest "$TOOL" inspect --package "$WORK/tamper-manifest"
expect_failure tampered-provenance "$TOOL" inspect --package "$WORK/tamper-provenance"
expect_failure tampered-golden "$TOOL" inspect --package "$WORK/tamper-golden"
expect_failure tampered-compatibility \
    "$TOOL" inspect --package "$WORK/tamper-compatibility"
expect_failure undeclared-package-file \
    "$TOOL" inspect --package "$WORK/tamper-extra-file"

printf 'plugin package tooling gate passed\n'
