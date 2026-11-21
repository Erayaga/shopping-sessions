#!/bin/sh
# Full correctness gate before any commit: unit tests, fixtures, dev-answer, reference seeds.
set -u
cd "$(dirname "$0")/.."
./scripts/dev.sh test 2>&1 | grep "test result" || { echo "TESTS FAILED"; exit 1; }
./scripts/dev.sh fixtures 2>&1 | tail -1
OUT_FORMAT=csv ./scripts/dev.sh run dev out-csv >/dev/null 2>&1
printf "dev-answer: "; python -I scripts/check_csv.py diff ../shopping-sessions-files/dev-answer/dev-answer out-csv | tail -1
for s in 2 3 4; do
  OUT_FORMAT=csv ./scripts/dev.sh run seed$s out-s$s >/dev/null 2>&1
  printf "seed $s: "; python -I scripts/check_csv.py diff ref-s$s out-s$s | tail -1
done
# Parquet path: official compare.py on the dev set, official fixture runner.
export MSYS_NO_PATHCONV=1 PATH="$PATH:/c/Program Files/Docker/Docker/resources/bin"
ROOT=$(pwd -W 2>/dev/null || pwd)
ANS=$(cd ../shopping-sessions-files/dev-answer/dev-answer && (pwd -W 2>/dev/null || pwd))
printf "parquet dev-answer: "
docker run --rm -v "$ROOT:/w" -v "$ANS:/ans:ro" -v sessions-target:/w/target -w /w -e DATA_DIR=dev -e OUTPUT_DIR=/tmp/o sessions-check sh -c 'target/release/sessions 2>/dev/null && python scripts/compare.py /tmp/o /ans | tail -1'
printf "parquet fixtures: "
docker run --rm -v "$ROOT:/w" -v sessions-target:/w/target -w /w sessions-check python scripts/run_fixtures.py fixtures -- target/release/sessions 2>&1 | tail -1
