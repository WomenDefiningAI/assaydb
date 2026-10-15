#!/bin/sh
set -eu
case "$0" in /*) ;; *) echo 'invoke this protected launcher by absolute path' >&2; exit 2 ;; esac
case "${1-}" in /*) ;; *) echo 'first argument must be the absolute pinned Python path' >&2; exit 2 ;; esac
runtime_python="$1"
shift
exec /usr/bin/env -i PATH=/usr/bin:/bin LC_ALL=C \
  ASSAY_RUNTIME_POLICY="${ASSAY_RUNTIME_POLICY:?}" \
  ASSAY_RUNTIME_POLICY_SHA256="${ASSAY_RUNTIME_POLICY_SHA256:?}" \
  ASSAY_RUNTIME_ROOTS="${ASSAY_RUNTIME_ROOTS:?}" \
  "$runtime_python" -I -S -B "${0%/*}/runtime_launch.py" "$@"
