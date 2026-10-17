#!/usr/bin/env bash
# Write a Parquet file with nullable columns and check it with pyarrow.
# Needs python3 with pyarrow (e.g. nix-shell -p 'python3.withPackages (p: [p.pyarrow])').
set -euo pipefail
cd "$(dirname "$0")/.."
lake build parquet-nulls-test
out="$(mktemp -d)/nulls.parquet"
./.lake/build/bin/parquet-nulls-test "$out"
python3 ArrowLeanTests/check_parquet_nulls.py "$out"
