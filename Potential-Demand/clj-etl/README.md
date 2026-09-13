# Clojure tabular ETL

Batch jobs for non-spatial feature engineering. Spatial overlay and ML stay in Python.

## Preferred: Docker (no host JDK)

From `Potential-Demand/`:

```bash
docker compose build clj-etl
make etl-clj DATES=20230305
# or full smoke:
make docker-build && make smoke
```

Image: `clj-etl/Dockerfile` (`eclipse-temurin:17` + Clojure CLI + tablecloth deps).

## Optional: local JDK + Clojure CLI

```bash
# JDK 17+
curl -sL https://github.com/clojure/brew-install/releases/latest/download/posix-install.sh \
  | bash -s -- --prefix "$PWD/.tools"
clojure -M:run --dates 20230305 --staging ../staging --population-dir ../input/data/population
```

Makefile **defaults to Docker** for `etl-clj`; local CLI is only needed if you invoke `clojure` yourself.

## Staging schema

| File | Role | Key columns | Notes |
|------|------|-------------|-------|
| `staging/txn_raw.parquet` | Bridge input | `on_stop_id`, `off_stop_id`, `on_time`, `off_time` | From pickle |
| `staging/stop_grid_map.parquet` | Bridge input | `stop_id`, `gridid` | Spatial overlay in Python |
| `staging/grid_ids.parquet` | Bridge input | `GridID` | Grid list |
| `staging/base_keys.parquet` | ETL output | `GridID`, `Date`, `Hour`, `Weekday`, `IsWeekend` | Full key space |
| `staging/features_population.parquet` | ETL output | `GridID`, `Date`, `Hour` + age/work/live/tour | fillna 0 |
| `staging/features_transaction.parquet` | ETL output | `GridID`, `Date`, `Hour`, `OnCounts`, `OffCounts`, `NetCounts` | fillna 0 |

Null policy: left/full joins fill missing numeric features with `0` before write.

Smoke fixtures live under `../fixtures/mini/` (see `make fixtures`).
