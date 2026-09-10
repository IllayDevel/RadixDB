#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
commit="${1:-$(git -C "${repo_dir}" rev-parse --verify HEAD)}"
[[ "${commit}" =~ ^[0-9a-f]{40}$ ]] || { echo "expected a full Git commit" >&2; exit 64; }
[[ "${commit}" == "$(git -C "${repo_dir}" rev-parse --verify HEAD)" ]] || {
  echo "bundle commit must be current HEAD" >&2; exit 65;
}
[[ -z "$(git -C "${repo_dir}" status --porcelain)" ]] || {
  echo "refusing to build PostgreSQL soak bundle from a dirty tree" >&2; exit 65;
}

build_cache="${repo_dir}/target/remote-soak-build-cache"
mkdir -p "${build_cache}/cargo-registry" "${build_cache}/target"
exec 9>"${build_cache}/build.lock"
flock -n 9 || { echo "another remote soak bundle build owns the cache" >&2; exit 75; }
container="${RADIXDB_SOAK_CONTAINER_ENGINE:-podman}"
builder_tag="${RADIXDB_SOAK_BUILDER_IMAGE:-docker.io/library/rust:1.97.0-bookworm}"
command -v "${container}" >/dev/null
"${container}" pull "${builder_tag}" >/dev/null
builder_id="$("${container}" image inspect --format '{{.Id}}' "${builder_tag}")"
builder_digest="$("${container}" image inspect --format '{{.Digest}}' "${builder_tag}")"
[[ -n "${builder_id}" && -n "${builder_digest}" ]] || {
  echo "builder image has no immutable identity" >&2; exit 69;
}

temporary="$(mktemp -d)"
cleanup() { chmod -R u+w -- "${temporary}" 2>/dev/null || true; rm -rf -- "${temporary}"; }
trap cleanup EXIT
source_dir="${temporary}/source"
mkdir -p "${source_dir}"
git -C "${repo_dir}" archive "${commit}" | tar -x -C "${source_dir}"
source_epoch="$(git -C "${repo_dir}" show -s --format=%ct "${commit}")"

"${container}" run --rm \
  --volume "${source_dir}:/workspace:Z" \
  --volume "${build_cache}/cargo-registry:/usr/local/cargo/registry:Z" \
  --volume "${build_cache}/target:/workspace/target:Z" \
  --workdir /workspace \
  --env "RADIXDB_GIT_COMMIT=${commit}" \
  --env "SOURCE_DATE_EPOCH=${source_epoch}" \
  "${builder_id}" bash -euo pipefail -c '
    test "$(rustc --version | awk "{print \$2}")" = 1.97.0
    cargo test --locked -p radixdb-soak
    cargo clippy --locked -p radixdb-soak --all-targets -- -D warnings
    cargo build --locked --release -p radixdb-soak --bin radixdb-soak
    cargo build --locked --release -p radixdb-soak --bin radixdb-soak-observer
  '

output="${repo_dir}/target/remote-soak-dist/${commit}-postgresql"
[[ ! -e "${output}" ]] || { echo "bundle already exists: ${output}" >&2; exit 73; }
mkdir -p "${output}/bin" "${output}/etc" "${output}/systemd"
install -m 0755 "${build_cache}/target/release/radixdb-soak" "${output}/bin/"
install -m 0755 "${build_cache}/target/release/radixdb-soak-observer" "${output}/bin/"
common="${source_dir}/crates/radixdb-soak/deploy"
deploy="${common}/postgresql"
install -m 0644 "${deploy}/soak-6h.toml" "${output}/etc/"
install -m 0644 "${deploy}/postgresql.conf" "${deploy}/pg_hba.conf" "${output}/etc/"
install -m 0644 "${common}/radixdb-soak.slice" "${output}/systemd/"
install -m 0644 "${deploy}/radixdb-soak-db.service" "${output}/systemd/"
install -m 0644 "${common}/radixdb-soak-agent.service" "${output}/systemd/"
install -m 0644 "${common}/radixdb-soak-observer.service" "${output}/systemd/"
install -m 0755 "${deploy}/install.sh" "${output}/bin/install.sh"
install -m 0755 "${deploy}/prepare-run.sh" "${output}/bin/prepare-run.sh"
install -m 0755 "${deploy}/stop-run.sh" "${output}/bin/stop-run.sh"
install -m 0644 "${deploy}/README.md" "${output}/README.md"
install -m 0644 "${source_dir}/LICENSE" "${output}/LICENSE"
install -m 0644 "${source_dir}/NOTICE" "${output}/NOTICE"

soak_identity="$("${output}/bin/radixdb-soak" --version)"
observer_identity="$("${output}/bin/radixdb-soak-observer" --version)"
for identity in "${soak_identity}" "${observer_identity}"; do
  [[ "${identity}" == *"git=${commit}"* && "${identity}" == *"profile=release"* ]] || {
    echo "release identity mismatch: ${identity}" >&2; exit 65;
  }
done
max_glibc="$(readelf --version-info "${output}/bin/radixdb-soak" "${output}/bin/radixdb-soak-observer" |
  sed -n 's/.*Name: GLIBC_\([0-9.]*\).*/\1/p' | sort -Vu | tail -n1)"
[[ -n "${max_glibc}" ]] || { echo "could not determine GLIBC requirement" >&2; exit 65; }
if [[ "$(printf '%s\n%s\n' 2.36 "${max_glibc}" | sort -V | tail -n1)" != 2.36 ]]; then
  echo "bundle requires GLIBC_${max_glibc}, newer than Debian bookworm baseline" >&2
  exit 65
fi
if rg -n -i 'ssh_password|authorization: basic|\.secret/' "${output}" >/dev/null; then
  echo "bundle contains forbidden secret material" >&2; exit 65;
fi
cat >"${output}/RELEASE" <<EOF
format=1
engine=postgresql
git_commit=${commit}
source_date_epoch=${source_epoch}
builder_image=${builder_tag}
builder_digest=${builder_digest}
rust_version=1.97.0
max_glibc=${max_glibc}
server_identity=external-postgresql
soak_identity=${soak_identity}
observer_identity=${observer_identity}
EOF
(
  cd "${output}"
  find . -type f ! -name SHA256SUMS -printf '%P\n' | LC_ALL=C sort |
    while IFS= read -r file; do sha256sum "${file}"; done >SHA256SUMS
  sha256sum -c SHA256SUMS
)
echo "PostgreSQL soak bundle: ${output}"
