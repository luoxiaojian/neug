#!/usr/bin/env bash
# Prepare local test databases and run Parquet COPY TO export tests.
#
# Usage (from repo root):
#   ./scripts/run_parquet_export_tests.sh
#   ./scripts/run_parquet_export_tests.sh --skip-build
#   ./scripts/run_parquet_export_tests.sh --skip-tinysnb   # if tinysnb bulk_loader fails locally
#
# Environment:
#   NEUG_BUILD_DIR   Root CMake build tree (default: <repo>/build)
#   TINYSNB_DB_DIR   Database path for tinysnb export tests (default: /tmp/tinysnb)
#   COMP_GRAPH_DB_DIR Database path for comprehensive_graph export tests (default: /tmp/comprehensive_graph)
#   PYTEST_ARGS      Extra args forwarded to pytest (default: -sv)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
BUILD_DIR="${NEUG_BUILD_DIR:-${REPO_ROOT}/build}"
TINYSNB_DB="${TINYSNB_DB_DIR:-/tmp/tinysnb}"
COMP_GRAPH_DB="${COMP_GRAPH_DB_DIR:-/tmp/comprehensive_graph}"
BULK_LOADER="${BUILD_DIR}/tools/utils/bulk_loader"
PY_BIND_DIR="${REPO_ROOT}/tools/python_bind"
PYTEST_ARGS="${PYTEST_ARGS:--sv}"

SKIP_BUILD=0
SKIP_TINYSNB=0
for arg in "$@"; do
  case "${arg}" in
    --skip-build) SKIP_BUILD=1 ;;
    --skip-tinysnb) SKIP_TINYSNB=1 ;;
    -h|--help)
      sed -n '2,12p' "$0"
      exit 0
      ;;
    *)
      echo "Unknown argument: ${arg}" >&2
      exit 1
      ;;
  esac
done

if [[ "${SKIP_BUILD}" -eq 0 ]]; then
  echo "==> Building neug + parquet extension + bulk_loader"
  cmake --build "${BUILD_DIR}" \
    --target neug neug_parquet_extension neug_py_bind bulk_loader \
    -j"$(sysctl -n hw.ncpu 2>/dev/null || nproc)"
fi

if [[ ! -x "${BULK_LOADER}" ]]; then
  echo "bulk_loader not found at ${BULK_LOADER}. Run 'make python-dev' or 'make cpp-build' first." >&2
  exit 1
fi

prepare_db() {
  local dataset_dir="$1"
  local db_dir="$2"
  local graph_yaml="${dataset_dir}/graph.yaml"
  local import_yaml="${dataset_dir}/import.yaml"

  if [[ ! -f "${graph_yaml}" || ! -f "${import_yaml}" ]]; then
    echo "Missing graph/import yaml under ${dataset_dir}" >&2
    exit 1
  fi

  echo "==> Loading ${dataset_dir} -> ${db_dir}"
  rm -rf "${db_dir}"
  FLEX_DATA_DIR="${dataset_dir}" "${BULK_LOADER}" \
    -g "${graph_yaml}" \
    -l "${import_yaml}" \
    -d "${db_dir}"
}

prepare_db "${REPO_ROOT}/example_dataset/comprehensive_graph" "${COMP_GRAPH_DB}"

if [[ "${SKIP_TINYSNB}" -eq 0 ]]; then
  if ! prepare_db "${REPO_ROOT}/example_dataset/tinysnb" "${TINYSNB_DB}"; then
    echo "WARNING: tinysnb bulk load failed; skipping TestParquetExport." >&2
    SKIP_TINYSNB=1
  fi
fi

echo "==> Running Parquet export tests"
cd "${PY_BIND_DIR}"
export NEUG_RUN_EXTENSION_TESTS=1
PYTEST_TARGETS=(
  tests/test_export.py::TestExportComprehensiveGraph::test_export_comprehensive_graph_to_parquet
  tests/test_export.py::TestExportComprehensiveGraph::test_export_comprehensive_graph_vertex_to_parquet
  tests/test_export.py::TestExportComprehensiveGraph::test_export_comprehensive_graph_edge_to_parquet
)
if [[ "${SKIP_TINYSNB}" -eq 0 ]]; then
  PYTEST_TARGETS=(tests/test_export.py::TestParquetExport "${PYTEST_TARGETS[@]}")
fi
python3 -m pytest ${PYTEST_ARGS} "${PYTEST_TARGETS[@]}"

echo "==> Parquet export tests passed"
