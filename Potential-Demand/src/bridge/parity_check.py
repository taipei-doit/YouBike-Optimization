"""
Compare Clojure staging tabular features against legacy Python preprocess methods.

Exit code 0 on match; 1 on mismatch. Requires staging outputs and local input data.
"""

from __future__ import annotations

import argparse
import sys

import pandas as pd

import preprocess

DEFAULT_DATES = ['20230305', '20230311', '20230317', '20230322']
KEYS = ['GridID', 'Date', 'Hour']

TXN_COLS = ['OnCounts', 'OffCounts', 'NetCounts']
POP_COLS = [
    'Age_15_17_Counts', 'Age_18_21_Counts', 'Age_22_29_Counts', 'Age_30_39_Counts',
    'Age_40_49_Counts', 'Age_50_59_Counts', 'Age_60_64_Counts', 'Age_Over65_Counts',
    'Age_Total_Counts', 'WorkPopulationCounts', 'LivePopulationCounts',
    'TourPopulationCounts',
]


def _legacy_transaction(p: preprocess.Preprocess) -> pd.DataFrame:
    tran_on_df, tran_off_df = p.transaction()
    tran_on_keys = ['gridid', 'on_date', 'on_hour']
    tran_off_keys = ['gridid', 'off_date', 'off_hour']
    tran_on_df = (
        tran_on_df[tran_on_keys + ['counts']]
        .groupby(tran_on_keys).sum().reset_index()
        .rename(columns={
            'gridid': 'GridID', 'on_date': 'Date', 'on_hour': 'Hour', 'counts': 'OnCounts'
        })
    )
    tran_off_df = (
        tran_off_df[tran_off_keys + ['counts']]
        .groupby(tran_off_keys).sum().reset_index()
        .rename(columns={
            'gridid': 'GridID', 'off_date': 'Date', 'off_hour': 'Hour', 'counts': 'OffCounts'
        })
    )
    for df in (tran_on_df, tran_off_df):
        df['Date'] = df['Date'].astype(str)
        df['GridID'] = df['GridID'].astype(int)
        df['Hour'] = df['Hour'].astype(int)

    merged = tran_on_df.merge(tran_off_df, how='outer', on=KEYS).fillna(0)
    merged['NetCounts'] = merged['OffCounts'] - merged['OnCounts']
    return merged[KEYS + TXN_COLS]


def _legacy_population(p: preprocess.Preprocess) -> pd.DataFrame:
    population_df = p.population().rename(columns={
        '網格編號': 'GridID', '日期': 'Date', '時間': 'Hour'
    })
    population_df['Date'] = population_df['Date'].astype(str)
    population_df['GridID'] = population_df['GridID'].astype(int)
    population_df['Hour'] = population_df['Hour'].astype(int)
    cols = [c for c in POP_COLS if c in population_df.columns]
    return population_df[KEYS + cols]


def _compare(name: str, legacy: pd.DataFrame, staging: pd.DataFrame, cols: list) -> bool:
    legacy = legacy.copy()
    staging = staging.copy()
    for df in (legacy, staging):
        df['Date'] = df['Date'].astype(str)
        df['GridID'] = df['GridID'].astype(int)
        df['Hour'] = df['Hour'].astype(int)

    use_cols = [c for c in cols if c in legacy.columns and c in staging.columns]
    if not use_cols:
        print(f'[FAIL] {name}: no overlapping feature columns')
        return False

    merged = legacy[KEYS + use_cols].merge(
        staging[KEYS + use_cols],
        on=KEYS,
        how='outer',
        suffixes=('_legacy', '_staging'),
        indicator=True,
    )
    only_legacy = (merged['_merge'] == 'left_only').sum()
    only_staging = (merged['_merge'] == 'right_only').sum()
    both = merged[merged['_merge'] == 'both']

    ok = True
    if only_legacy or only_staging:
        print(
            f'[FAIL] {name}: key mismatch '
            f'(legacy-only={only_legacy}, staging-only={only_staging})'
        )
        ok = False

    for col in use_cols:
        left = both[f'{col}_legacy'].fillna(0).astype(float)
        right = both[f'{col}_staging'].fillna(0).astype(float)
        if not (left == right).all():
            diff = (left - right).abs()
            print(
                f'[FAIL] {name}.{col}: max_abs_diff={diff.max()} '
                f'mismatched_rows={(diff > 0).sum()}'
            )
            ok = False

    if ok:
        print(f'[OK] {name}: {len(both)} keys, columns {use_cols}')
    return ok


def run(date_list: list[str]) -> int:
    p = preprocess.Preprocess(date_list=date_list)

    txn_staging = pd.read_parquet('staging/features_transaction.parquet')
    pop_staging = pd.read_parquet('staging/features_population.parquet')

    print('Computing legacy transaction features (Python)...')
    txn_legacy = _legacy_transaction(p)
    print('Computing legacy population features (Python)...')
    pop_legacy = _legacy_population(p)

    ok_txn = _compare('transaction', txn_legacy, txn_staging, TXN_COLS)
    ok_pop = _compare('population', pop_legacy, pop_staging, POP_COLS)
    return 0 if (ok_txn and ok_pop) else 1


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--dates',
        default=','.join(DEFAULT_DATES),
        help='Comma-separated YYYYMMDD dates',
    )
    args = parser.parse_args()
    dates = [d.strip() for d in args.dates.split(',') if d.strip()]
    sys.exit(run(dates))


if __name__ == '__main__':
    main()
