#!/usr/bin/env python3
"""Stdlib-only checks of CSV outputs.

  check_csv.py fixtures FIXTURES_DIR -- CMD...   run CMD DATA OUT on every case, diff OUT/*.csv
  check_csv.py diff DIR_A DIR_B                  diff two folders of user_daily.csv/campaign_daily.csv
"""
from collections import Counter
import os
import subprocess
import sys
import tempfile

TABLES = ("user_daily", "campaign_daily")


def load(path):
    with open(path, encoding="utf-8", newline="") as f:
        lines = f.read().split("\n")
    header = lines[0]
    return header, Counter(l for l in lines[1:] if l != "")


def diff(a_dir, b_dir, quiet=False):
    ok = True
    for t in TABLES:
        ha, ra = load(os.path.join(a_dir, f"{t}.csv"))
        hb, rb = load(os.path.join(b_dir, f"{t}.csv"))
        if ha != hb:
            ok = False
            print(f"  {t}: header {ha!r} vs {hb!r}")
        only_a, only_b = ra - rb, rb - ra
        if only_a or only_b:
            ok = False
            if not quiet:
                print(f"  {t}: rows {sum(ra.values())} vs {sum(rb.values())}; "
                      f"only in A {sum(only_a.values())}, only in B {sum(only_b.values())}")
                for r in sorted(only_a)[:8]:
                    print(f"    A: {r!r}")
                for r in sorted(only_b)[:8]:
                    print(f"    B: {r!r}")
    return ok


def main():
    mode = sys.argv[1]
    if mode == "diff":
        ok = diff(sys.argv[2], sys.argv[3])
        print("MATCH" if ok else "DIFFER")
        sys.exit(0 if ok else 1)
    sep = sys.argv.index("--")
    fx, cmd = sys.argv[2], sys.argv[sep + 1:]
    cases = sorted(d for d in os.listdir(fx) if os.path.isdir(os.path.join(fx, d)))
    failed = []
    for case in cases:
        data = os.path.join(fx, case, "input")
        with tempfile.TemporaryDirectory() as out:
            env = dict(os.environ, DATA_DIR=data, OUTPUT_DIR=out, SCRATCH_DIR=out)
            p = subprocess.run(cmd + [data, out], env=env, capture_output=True, text=True)
            if p.returncode != 0:
                failed.append(case)
                print(f"FAIL {case}: exit {p.returncode}\n{p.stderr[-2000:]}")
                continue
            if diff(os.path.join(fx, case, "expected"), out):
                print(f"ok   {case}")
            else:
                failed.append(case)
                print(f"FAIL {case}  (A = expected, B = actual)")
    print(f"\n{len(cases) - len(failed)}/{len(cases)} fixtures passed")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
