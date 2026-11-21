#!/bin/sh
# Dev loop on a machine where native binaries can't run (Smart App Control):
# everything Rust runs in a Linux container; comparisons run on the host.
#   scripts/dev.sh build            cargo build --release (cached volumes)
#   scripts/dev.sh test             cargo test
#   scripts/dev.sh fixtures         run all fixtures (CSV mode), diff vs expected
#   scripts/dev.sh run DATA OUT     run the binary on DATA, OUT_FORMAT from env (default parquet)
#   scripts/dev.sh gen SEED EVENTS CLICKS OUT    build+run the generator
set -eu
export MSYS_NO_PATHCONV=1 MSYS2_ARG_CONV_EXCL="*"
export PATH="$PATH:/c/Program Files/Docker/Docker/resources/bin"
cd "$(dirname "$0")/.."
ROOT=$(pwd -W 2>/dev/null || pwd)
IMG=rust:1-bookworm
RUN="docker run --rm -v $ROOT:/w -v sessions-target:/w/target -v sessions-cargo:/usr/local/cargo/registry -w /w -e RUSTFLAGS=-Ctarget-cpu=x86-64-v3"
case "$1" in
  build) $RUN $IMG cargo build --release ;;
  test) $RUN $IMG cargo test --release ;;
  fixtures)
    $RUN $IMG sh -c 'cargo build --release -q && rm -rf fx-out && for d in fixtures/*/; do c=$(basename $d); mkdir -p fx-out/$c; DATA_DIR=$d/input OUTPUT_DIR=fx-out/$c OUT_FORMAT=csv NCPU=4 target/release/sessions 2>/dev/null || echo "CRASH $c"; done'
    fail=0; n=0
    for d in fixtures/*/; do c=$(basename "$d"); n=$((n+1))
      if python -I scripts/check_csv.py diff "fixtures/$c/expected" "fx-out/$c" >/tmp/fx.$$ 2>&1; then :; else fail=$((fail+1)); echo "FAIL $c"; cat /tmp/fx.$$; fi
    done; rm -f /tmp/fx.$$
    echo "$((n-fail))/$n fixtures passed" ;;
  run) shift; $RUN -v "$(cd "$1" && (pwd -W 2>/dev/null || pwd))":/data:ro -e DATA_DIR=/data -e OUTPUT_DIR=/w/$2 -e OUT_FORMAT="${OUT_FORMAT:-parquet}" -e NCPU="${NCPU:-8}" $IMG bash -c "rm -rf /w/$2 && time target/release/sessions" ;;
  gen) shift; docker run --rm -v "$ROOT/../shopping-sessions-files/generator/generator:/g" -v sessions-gentarget:/g/target -v sessions-cargo:/usr/local/cargo/registry -v "$ROOT:/w" -w /g $IMG sh -c "cargo build --release -q && target/release/generate --seed $1 --events $2 --clicks $3 --output /w/$4" ;;
  *) echo "unknown command"; exit 2 ;;
esac
