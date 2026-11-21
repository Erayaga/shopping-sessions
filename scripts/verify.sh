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
