#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
commit="${1:-$(git -C "${repo_dir}" rev-parse --verify HEAD)}"
deployment_profile="${RADIXDB_SOAK_DEPLOYMENT_PROFILE:-default}"
case "${deployment_profile}" in
  default|atom-hdd) ;;
  *) echo "RADIXDB_SOAK_DEPLOYMENT_PROFILE must be default or atom-hdd" >&2; exit 64;;
esac
[[ "${commit}" =~ ^[0-9a-f]{40}$ ]] || { echo "expected a full Git commit" >&2; exit 64; }
git -C "${repo_dir}" cat-file -e "${commit}^{commit}"
[[ "${commit}" == "$(git -C "${repo_dir}" rev-parse --verify HEAD)" ]] || {
  echo "bundle commit must be current HEAD" >&2; exit 65;
}
[[ -z "$(git -C "${repo_dir}" status --porcelain)" ]] || {
  echo "refusing to build remote soak bundle from a dirty tree" >&2; exit 65;
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
    cargo build --locked --release -p radixdb --bin radixdb-server
    cargo build --locked --release -p radixdb-soak --bin radixdb-soak
    cargo build --locked --release -p radixdb-soak --bin radixdb-soak-observer
  '

output_suffix=""
if [[ "${deployment_profile}" != default ]]; then
  output_suffix="-${deployment_profile}"
fi
output="${repo_dir}/target/remote-soak-dist/${commit}${output_suffix}"
[[ ! -e "${output}" ]] || { echo "bundle already exists: ${output}" >&2; exit 73; }
mkdir -p "${output}/bin" "${output}/etc" "${output}/systemd"
install -m 0755 "${build_cache}/target/release/radixdb-server" "${output}/bin/"
install -m 0755 "${build_cache}/target/release/radixdb-soak" "${output}/bin/"
install -m 0755 "${build_cache}/target/release/radixdb-soak-observer" "${output}/bin/"
deploy="${source_dir}/crates/radixdb-soak/deploy"
install -m 0644 "${deploy}/server.toml" "${output}/etc/"
for profile in smoke tuning 6h 24h 48h; do
  install -m 0644 "${deploy}/soak-${profile}.toml" "${output}/etc/"
done
install -m 0644 "${deploy}/radixdb-soak.slice" "${output}/systemd/"
install -m 0644 "${deploy}/radixdb-soak-db.service" "${output}/systemd/"
install -m 0644 "${deploy}/radixdb-soak-agent.service" "${output}/systemd/"
install -m 0644 "${deploy}/radixdb-soak-observer.service" "${output}/systemd/"
install -m 0755 "${deploy}/install.sh" "${output}/bin/install.sh"
install -m 0755 "${deploy}/prepare-run.sh" "${output}/bin/prepare-run.sh"
install -m 0755 "${deploy}/stop-run.sh" "${output}/bin/stop-run.sh"
if [[ "${deployment_profile}" == atom-hdd ]]; then
  atom_deploy="${deploy}/atom-hdd"
  install -m 0644 "${atom_deploy}/server.toml" "${output}/etc/server.toml"
  install -m 0644 "${atom_deploy}/radixdb-soak.slice" "${output}/systemd/radixdb-soak.slice"
  install -m 0644 "${atom_deploy}/radixdb-soak-db.service" \
    "${output}/systemd/radixdb-soak-db.service"
  install -m 0755 "${atom_deploy}/prepare-run.sh" "${output}/bin/prepare-run.sh"
  install -m 0755 "${atom_deploy}/stop-run.sh" "${output}/bin/stop-run.sh"
fi
install -m 0644 "${source_dir}/LICENSE" "${output}/LICENSE"
install -m 0644 "${source_dir}/NOTICE" "${output}/NOTICE"

server_identity="$("${output}/bin/radixdb-server" --version)"
soak_identity="$("${output}/bin/radixdb-soak" --version)"
observer_identity="$("${output}/bin/radixdb-soak-observer" --version)"
for identity in "${server_identity}" "${soak_identity}" "${observer_identity}"; do
  [[ "${identity}" == *"git=${commit}"* && "${identity}" == *"profile=release"* ]] || {
    echo "release identity mismatch: ${identity}" >&2; exit 65;
  }
done
max_glibc="$(readelf --version-info "${output}/bin/radixdb-server" "${output}/bin/radixdb-soak" "${output}/bin/radixdb-soak-observer" |
  sed -n 's/.*Name: GLIBC_\([0-9.]*\).*/\1/p' | sort -Vu | tail -n1)"
[[ -n "${max_glibc}" ]] || { echo "could not determine GLIBC requirement" >&2; exit 65; }
if [[ "$(printf '%s\n%s\n' 2.36 "${max_glibc}" | sort -V | tail -n1)" != 2.36 ]]; then
  echo "bundle requires GLIBC_${max_glibc}, newer than Debian bookworm baseline" >&2
  exit 65
fi
if strings "${output}/bin/radixdb-server" | grep -Fq RADIXDB_TEST_MUTATION; then
  echo "production server contains the test mutation selector" >&2; exit 65
fi
if rg -n -i 'ssh_password|authorization: basic|\.secret/' "${output}" >/dev/null; then
  echo "bundle contains forbidden secret material" >&2; exit 65
fi

cat >"${output}/RELEASE" <<EOF
format=1
git_commit=${commit}
deployment_profile=${deployment_profile}
source_date_epoch=${source_epoch}
builder_image=${builder_tag}
builder_digest=${builder_digest}
rust_version=1.97.0
max_glibc=${max_glibc}
server_identity=${server_identity}
soak_identity=${soak_identity}
observer_identity=${observer_identity}
EOF
(
  cd "${output}"
  find . -type f ! -name SHA256SUMS -printf '%P\n' | LC_ALL=C sort |
    while IFS= read -r file; do sha256sum "${file}"; done >SHA256SUMS
  sha256sum -c SHA256SUMS
)
echo "remote soak bundle: ${output}"
