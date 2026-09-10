#!/usr/bin/env bash
set -euo pipefail

if [[ "$#" -ne 3 ]]; then
  echo "usage: $0 SOURCE_BINARY INSTALLED_BINARY DEBUG_ARTIFACT" >&2
  exit 64
fi

source_binary="$1"
installed_binary="$2"
debug_artifact="$3"

[[ -x "${source_binary}" ]] || {
  echo "missing executable artifact: ${source_binary}" >&2
  exit 1
}
command -v objcopy >/dev/null || {
  echo "objcopy is required to split release debug information" >&2
  exit 69
}
command -v readelf >/dev/null || {
  echo "readelf is required to verify split release artifacts" >&2
  exit 69
}
if ! readelf -S "${source_binary}" | grep -Eq '\.debug_(info|line)'; then
  echo "release input has no debug information to preserve: ${source_binary}" >&2
  exit 65
fi

mkdir -p -- "$(dirname -- "${installed_binary}")" "$(dirname -- "${debug_artifact}")"
install -m 0755 "${source_binary}" "${installed_binary}"
objcopy --only-keep-debug "${source_binary}" "${debug_artifact}"
objcopy --strip-debug "${installed_binary}"
objcopy --add-gnu-debuglink="${debug_artifact}" "${installed_binary}"
chmod 0644 "${debug_artifact}"

debug_name="$(basename -- "${debug_artifact}")"
readelf --string-dump=.gnu_debuglink "${installed_binary}" |
  grep -Fq "${debug_name}" || {
    echo "installed binary does not reference ${debug_name}" >&2
    exit 65
  }

source_build_id="$(readelf -n "${source_binary}" | sed -n 's/.*Build ID: //p' | head -n1)"
installed_build_id="$(readelf -n "${installed_binary}" | sed -n 's/.*Build ID: //p' | head -n1)"
debug_build_id="$(readelf -n "${debug_artifact}" 2>/dev/null |
  sed -n 's/.*Build ID: //p' | head -n1)"
[[ -n "${source_build_id}" && "${installed_build_id}" == "${source_build_id}" &&
   "${debug_build_id}" == "${source_build_id}" ]] || {
  echo "split release artifacts do not preserve one Build ID" >&2
  exit 65
}

source_size="$(stat -c %s "${source_binary}")"
installed_size="$(stat -c %s "${installed_binary}")"
[[ "${installed_size}" -lt "${source_size}" ]] || {
  echo "split release binary did not shrink" >&2
  exit 65
}
