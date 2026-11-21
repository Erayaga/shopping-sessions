#!/usr/bin/env python3
"""
Shopping Sessions: slow, literal reference implementation (stdlib only).

Purpose: pin down the exact semantics of rules.md so the fast pipeline can be
diffed against it on any generated dataset. Not meant to be fast.

Usage: reference.py DATA_DIR OUT_DIR
Writes OUT_DIR/user_daily.csv and OUT_DIR/campaign_daily.csv (the same CSV
layout as fixtures/*/expected and dev-answer).
"""
import glob
import json
import os
import sys

DAY_MS = 86_400_000
GAP_MS = 1_800_000
WEEK_MS = 7 * DAY_MS
LATE_MS = DAY_MS

DIGITS = frozenset(b"0123456789")


# ---------------------------------------------------------------- CSV dialect
def parse_line(line, nfields):
    """Return a list of fields (bytes or None for empty) or None if malformed."""
    parts = line.split(b",")
    if len(parts) != nfields:
        return None
    out = []
    for p in parts:
        if b'"' in p:
            if len(p) < 2 or p[0] != 0x22 or p[-1] != 0x22 or b'"' in p[1:-1]:
                return None
            p = p[1:-1]
        out.append(p if p else None)
    return out


def read_rows(path, nfields):
    with open(path, "rb") as f:
        data = f.read()
    if not data:
        return
    lines = data.split(b"\n")
    if data.endswith(b"\n"):
        lines.pop()  # the terminator of the last line, not an empty line
    for line in lines[1:]:  # first line of each file is the header
        row = parse_line(line, nfields)
        if row is not None:
            yield row


def read_dir(data, sub, nfields):
    for path in sorted(glob.glob(os.path.join(data, sub, "*.csv"))):
        yield from read_rows(path, nfields)


# ---------------------------------------------------------------- timestamps
def is_digits(b):
    return len(b) > 0 and all(c in DIGITS for c in b)


def days_from_civil(y, m, d):
    y -= m <= 2
    era = (y if y >= 0 else y - 399) // 400
    yoe = y - era * 400
    doy = (153 * (m + (-3 if m > 2 else 9)) + 2) // 5 + d - 1
    doe = yoe * 365 + yoe // 4 - yoe // 100 + doy
    return era * 146097 + doe - 719468


def civil_from_days(z):
    z += 719468
    era = (z if z >= 0 else z - 146096) // 146097
    doe = z - era * 146097
    yoe = (doe - doe // 1460 + doe // 36524 - doe // 146096) // 365
    y = yoe + era * 400
    doy = doe - (365 * yoe + yoe // 4 - yoe // 100)
    mp = (5 * doy + 2) // 153
    d = doy - (153 * mp + 2) // 5 + 1
    m = mp + (3 if mp < 10 else -9)
    return (y + (m <= 2), m, d)


def days_in_month(y, m):
    if m == 2:
        leap = (y % 4 == 0 and y % 100 != 0) or y % 400 == 0
        return 29 if leap else 28
    return 30 if m in (4, 6, 9, 11) else 31


def parse_datetime(b):
    """b[:19] = YYYY-MM-DDTHH:MM:SS. Returns epoch ms or None."""
    if (b[4] != 0x2D or b[7] != 0x2D or b[10] != 0x54 or b[13] != 0x3A or b[16] != 0x3A):
        return None
    for s in (b[0:4], b[5:7], b[8:10], b[11:13], b[14:16], b[17:19]):
        if not is_digits(s):
            return None
    y, mo, d = int(b[0:4]), int(b[5:7]), int(b[8:10])
    h, mi, s = int(b[11:13]), int(b[14:16]), int(b[17:19])
    if not (1 <= mo <= 12) or not (1 <= d <= days_in_month(y, mo)):
        return None
    if h > 23 or mi > 59 or s > 59:
        return None
    return ((days_from_civil(y, mo, d) * 24 + h) * 60 + mi) * 60_000 + s * 1000


def parse_ts(b):
    """Returns epoch ms (int, possibly huge) or None for a malformed value."""
    if b is None:
        return None
    if is_digits(b):
        return int(b)  # F3
    if len(b) == 20 and b[19] == 0x5A:  # F1
        return parse_datetime(b)
    if len(b) == 25 and b[19] in (0x2B, 0x2D) and b[22] == 0x3A:  # F2
        base = parse_datetime(b)
        if base is None:
            return None
        oh, om = b[20:22], b[23:25]
        if not is_digits(oh) or not is_digits(om):
            return None
        oh, om = int(oh), int(om)
        if oh > 23 or om > 59:
            return None
        off = (oh * 60 + om) * 60_000
        return base - off if b[19] == 0x2B else base + off
    return None


# ---------------------------------------------------------------- union-find
class DSU:
    def __init__(self):
        self.parent = {}

    def find(self, x):
        p = self.parent
        root = x
        while p.get(root, root) != root:
            root = p[root]
        while p.get(x, x) != root:
            p[x], x = root, p[x]
        return root

    def union(self, a, b):
        ra, rb = self.find(a), self.find(b)
        if ra == rb:
            return
        # keep the byte-wise smaller ID as root, so the root is the name
        if rb < ra:
            ra, rb = rb, ra
        self.parent[rb] = ra


def main():
    data, out = sys.argv[1], sys.argv[2]
    with open(os.path.join(data, "manifest.json")) as f:
        manifest = json.load(f)
    ws = parse_ts(manifest["window_start"].encode())
    we = parse_ts(manifest["window_end"].encode())

    products = {r[0]: r[1] for r in read_rows(os.path.join(data, "products.csv"), 2) if r[0] is not None}
    campaigns = {r[0]: r[1] for r in read_rows(os.path.join(data, "campaigns.csv"), 2) if r[0] is not None}

    # ---- links
    dsu = DSU()
    for a, b in read_dir(data, "links", 2):
        if a is None or b is None:
            continue
        dsu.union(a, b)
    person = dsu.find

    # ---- events: dedup on event_id, smallest ingest_ts wins (before validation)
    kept = {}
    for row in read_dir(data, "events", 7):
        eid = row[0]
        if eid is None:
            continue
        ing = parse_ts(row[3])  # always valid F1 per the rules
        cur = kept.get(eid)
        if cur is None or ing < cur[0]:
            kept[eid] = (ing, row)

    by_person = {}
    for ing, (eid, uid, ts_b, _, etype, pid, price_b) in kept.values():
        if uid is None or pid is None:
            continue
        ts = parse_ts(ts_b)
        if ts is None or not (ws <= ts < we):
            continue
        if etype not in (b"view", b"cart", b"purchase"):
            continue
        price = 0
        if etype == b"purchase":
            if price_b is None or not is_digits(price_b):
                continue
            price = int(price_b)
            if not (1 <= price <= 10_000_000):
                continue
        if ing - ts > LATE_MS:
            continue
        cat = products.get(pid, b"unknown")
        by_person.setdefault(person(uid), []).append((ts, etype == b"purchase", price, cat))

    # ---- clicks (dedup is a no-op for credit and assists: copies are identical)
    clicks_by_person = {}
    cws = ws - WEEK_MS
    for cid, uid, camp, ts_b in read_dir(data, "clicks", 4):
        if cid is None or uid is None or camp is None or camp not in campaigns:
            continue
        ts = parse_ts(ts_b)
        if ts is None or not (cws <= ts < we):
            continue
        clicks_by_person.setdefault(person(uid), []).append((ts, cid, camp))

    # ---- sessions, attribution, aggregation
    user_daily = {}  # (person, day) -> [sessions, events, revenue, {cat: sum}]
    camp_daily = {}  # (camp, day) -> [sessions, purchasing, revenue, assisted]
    for p, evs in by_person.items():
        evs.sort(key=lambda e: e[0])
        clicks = clicks_by_person.get(p, [])
        sessions = []
        prev = None
        for e in evs:
            if prev is None or e[0] - prev > GAP_MS:
                sessions.append([e[0], []])
            sessions[-1][1].append(e)
            prev = e[0]
        for start, sevs in sessions:
            day = start // DAY_MS
            rev = sum(e[2] for e in sevs if e[1])
            purchasing = any(e[1] for e in sevs)
            u = user_daily.setdefault((p, day), [0, 0, 0, {}])
            u[0] += 1
            u[1] += len(sevs)
            u[2] += rev
            for e in sevs:
                if e[1]:
                    u[3][e[3]] = u[3].get(e[3], 0) + e[2]
            best = None
            assists = set()
            for c in clicks:
                if start - WEEK_MS <= c[0] <= start:
                    assists.add(c[2])
                    if best is None or c[0] > best[0] or (c[0] == best[0] and c[1] < best[1]):
                        best = c
            credit = best[2] if best else b"organic"
            cd = camp_daily.setdefault((credit, day), [0, 0, 0, 0])
            cd[0] += 1
            cd[1] += purchasing
            cd[2] += rev
            for a in assists:
                camp_daily.setdefault((a, day), [0, 0, 0, 0])[3] += 1

    os.makedirs(out, exist_ok=True)

    def fmt_day(d):
        y, m, dd = civil_from_days(d)
        return f"{y:04d}-{m:02d}-{dd:02d}"

    with open(os.path.join(out, "user_daily.csv"), "w", encoding="utf-8", newline="") as f:
        f.write("user_id,day,sessions,events,revenue_cents,top_category\n")
        for (p, day), (s, n, rev, cats) in sorted(user_daily.items()):
            top = ""
            if cats:
                top = min(cats.items(), key=lambda kv: (-kv[1], kv[0]))[0].decode()
            f.write(f"{p.decode()},{fmt_day(day)},{s},{n},{rev},{top}\n")
    with open(os.path.join(out, "campaign_daily.csv"), "w", encoding="utf-8", newline="") as f:
        f.write("campaign_id,channel,day,sessions,purchasing_sessions,revenue_cents,assisted_sessions\n")
        for (c, day), (s, ps, rev, a) in sorted(camp_daily.items()):
            ch = b"organic" if c == b"organic" else campaigns[c]
            f.write(f"{c.decode()},{ch.decode()},{fmt_day(day)},{s},{ps},{rev},{a}\n")


if __name__ == "__main__":
    main()
