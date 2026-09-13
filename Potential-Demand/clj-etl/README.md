# Clojure tabular ETL

Batch jobs for non-spatial feature engineering. Spatial overlay and ML stay in Python.

## Requirements

- JDK 17+
- [Clojure CLI](https://clojure.org/guides/install_clojure)

Optional local install (used by Makefile when present):

```bash
curl -sL https://github.com/clojure/brew-install/releases/latest/download/posix-install.sh | bash -s -- --prefix "$PWD/.tools"
```

## Run (from this directory)

```bash
# After Python bridge has written staging/*.parquet inputs:
clojure -M:run --dates 20230305,20230311,20230317,20230322
```

Working directory for Makefile targets is `Potential-Demand/`; paths below are relative to that root.

## Staging schema

| File | Role | Key columns | Notes |
|------|------|-------------|-------|
| `staging/txn_raw.parquet` | Bridge input | `on_stop_id`, `off_stop_id`, `on_time`, `off_time` | From pickle |
| `staging/stop_grid_map.parquet` | Bridge input | `stop_id`, `gridid` | Spatial overlay in Python |
| `staging/grid_ids.parquet` | Bridge input | `GridID` | All Taipei grids |
| `staging/base_keys.parquet` | ETL output | `GridID`, `Date`, `Hour`, `Weekday`, `IsWeekend` | Full key space |
| `staging/features_population.parquet` | ETL output | `GridID`, `Date`, `Hour` + age/work/live/tour counts | fillna 0 |
| `staging/features_transaction.parquet` | ETL output | `GridID`, `Date`, `Hour`, `OnCounts`, `OffCounts`, `NetCounts` | fillna 0 |

Null policy: left/full joins fill missing numeric features with `0` before write.
