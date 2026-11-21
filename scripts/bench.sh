#!/bin/sh
# Build the submission image and time N scorer-style runs on the 20M volume.
export MSYS_NO_PATHCONV=1 PATH="$PATH:/c/Program Files/Docker/Docker/resources/bin"
cd "$(dirname "$0")/.."
docker build -q -t shopping-sessions:latest . >/dev/null || exit 1
for i in $(seq ${1:-4}); do
  docker run --rm --network none --read-only --tmpfs /tmp:rw,size=64m --tmpfs /output:rw,size=2g,uid=1000 \
    --cpuset-cpus 0-7 --memory 11g --memory-swap 11g --user 1000:1000 -v ${VOL:-sessions-data20}:/data:ro -e NCPU=8 \
    shopping-sessions:latest 2>&1 | grep -vE "links: (parsed|edges|union|names)" | tr '\n' ' ' | sed 's/  */ /g'; echo
done
