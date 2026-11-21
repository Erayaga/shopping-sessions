# Shopping Sessions: native Rust pipeline

Reads `/data`, writes `/output/user_daily/` and `/output/campaign_daily/` as Parquet, following `rules.md` exactly.

- `src/parse.rs`: the CSV dialect, the three timestamp forms, prices.
- `src/main.rs`: links (union-find, people named by their smallest ID) -> events parsed and validated in parallel, partitioned by `event_id` hash -> dedup (smallest `ingest_ts`) -> partitioned by person -> sessions, credit (latest click within 7 days) and assists (merged 7-day intervals per campaign).
- `src/out.rs`: Parquet writer (one file per thread) and a CSV mode (`OUT_FORMAT=csv`) for diffing.
- `reference/reference.py`: slow, stdlib-only literal implementation used to cross-check other seeds.

## Test

```sh
scripts/dev.sh fixtures                  # 36 edge cases
scripts/dev.sh gen 1 1000000 250000 dev  # dev set
OUT_FORMAT=csv scripts/dev.sh run dev out-csv
python scripts/check_csv.py diff path/to/dev-answer out-csv
```
