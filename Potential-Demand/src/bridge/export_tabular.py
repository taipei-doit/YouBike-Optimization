"""
Export tabular staging artifacts for Clojure ETL.

Writes:
  - staging/txn_raw.parquet
  - staging/stop_grid_map.parquet
  - staging/grid_ids.parquet

Spatial stop→grid overlay stays in Python; Clojure only does tabular aggregation.
"""

from __future__ import annotations

import argparse
import configparser
import os
import pickle
from ast import literal_eval
from typing import List, Optional

import geopandas as gpd
import pandas as pd
from shapely.geometry import Point


DEFAULT_DATES = ['20230305', '20230311', '20230317', '20230322']
STAGING_DIR = 'staging'


def _overlay_point_to_grid(
    point_df: gpd.GeoDataFrame,
    grid_poly: gpd.GeoDataFrame,
    point_key: str,
) -> pd.DataFrame:
    """Map unique stop points to grid polygons (same union overlay as preprocess)."""
    overlay_df = gpd.overlay(point_df, grid_poly, how='union').explode().reset_index()
    overlay_df = overlay_df[[point_key, 'gridid', 'geometry']]
    overlay_df = overlay_df[overlay_df[point_key].notnull()]
    overlay_df = overlay_df[overlay_df['gridid'].notnull()]
    overlay_df['gridid'] = overlay_df['gridid'].astype(int)
    overlay_df = overlay_df.drop(columns=['geometry'])
    return overlay_df.drop_duplicates(subset=[point_key]).reset_index(drop=True)


def load_paths(
    transaction_dir: Optional[str] = None,
    population_dir: Optional[str] = None,
) -> dict:
    if transaction_dir and population_dir:
        return {'transaction': transaction_dir, 'population': population_dir}

    config = configparser.ConfigParser()
    config.read('input/params.ini', encoding='utf-8')
    file_path = config['FILEPATH']
    return {
        'transaction': transaction_dir or literal_eval(file_path['transaction']),
        'population': population_dir or literal_eval(file_path['population']),
    }


def _load_txn_frame(pkl_path: str) -> pd.DataFrame:
    with open(pkl_path, 'rb') as handle:
        payload = pickle.load(handle)
    if isinstance(payload, pd.DataFrame):
        return payload
    if isinstance(payload, list):
        return pd.DataFrame(payload)
    raise TypeError(f'Unsupported txn pickle type: {type(payload)}')


def export_txn_raw(transaction_file_path: str, date_list: List[str], out_path: str) -> None:
    frames = []
    for date in date_list:
        pkl_path = (
            f'{transaction_file_path}/202303_txn_identified_transfer/{date}.pkl'
        )
        frames.append(_load_txn_frame(pkl_path))
    df = pd.concat(frames, ignore_index=True)

    for col in ('on_time', 'off_time'):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col]).astype(str)

    keep_cols = [
        c for c in ('on_stop_id', 'off_stop_id', 'on_time', 'off_time') if c in df.columns
    ]
    df[keep_cols].to_parquet(out_path, index=False)
    print(f'Wrote {out_path} ({len(df)} rows)')


def export_stop_grid_map(
    transaction_file_path: str,
    population_file_path: str,
    out_path: str,
) -> gpd.GeoDataFrame:
    stop_df = pd.read_csv(
        f'{transaction_file_path}/86ec099baa2d36c22ab3a87350b718de_export.csv'
    )
    stop_df = stop_df[['sno', 'lat', 'lng']].copy()
    stop_df['sno'] = stop_df['sno'].astype(str)
    stop_df['sno'] = 'U' + stop_df['sno'].str[3:]
    stop_df['geometry'] = stop_df.apply(lambda x: Point((x.lng, x.lat)), axis=1)
    stop_gdf = gpd.GeoDataFrame(stop_df, crs=4326).to_crs(3826)

    grid_df = gpd.read_file(
        f'{population_file_path}/FET_2023_grid_97.geojson'
    ).set_crs('epsg:3826')
    grid_poly = grid_df[['gridid', 'geometry']]

    point_df = stop_gdf[['sno', 'geometry']].drop_duplicates().reset_index(drop=True)
    point_df = point_df.rename(columns={'sno': 'stop_id'})
    mapped = _overlay_point_to_grid(point_df, grid_poly, 'stop_id')
    mapped.to_parquet(out_path, index=False)
    print(f'Wrote {out_path} ({len(mapped)} rows)')
    return grid_df


def export_grid_ids(grid_df: gpd.GeoDataFrame, out_path: str) -> None:
    grid_ids = pd.DataFrame({'GridID': grid_df['gridid'].astype(int).unique()})
    grid_ids.to_parquet(out_path, index=False)
    print(f'Wrote {out_path} ({len(grid_ids)} rows)')


def run(
    date_list: List[str] | None = None,
    transaction_dir: Optional[str] = None,
    population_dir: Optional[str] = None,
) -> None:
    date_list = date_list or DEFAULT_DATES
    paths = load_paths(transaction_dir, population_dir)
    os.makedirs(STAGING_DIR, exist_ok=True)

    export_txn_raw(
        paths['transaction'],
        date_list,
        f'{STAGING_DIR}/txn_raw.parquet',
    )
    grid_df = export_stop_grid_map(
        paths['transaction'],
        paths['population'],
        f'{STAGING_DIR}/stop_grid_map.parquet',
    )
    export_grid_ids(grid_df, f'{STAGING_DIR}/grid_ids.parquet')
    print('Bridge export complete.')


def main() -> None:
    parser = argparse.ArgumentParser(description='Export tabular staging for Clojure ETL')
    parser.add_argument(
        '--dates',
        type=str,
        default=','.join(DEFAULT_DATES),
        help='Comma-separated YYYYMMDD dates',
    )
    parser.add_argument(
        '--transaction-dir',
        type=str,
        default=None,
        help='Override transaction data dir (default: params.ini / fixtures)',
    )
    parser.add_argument(
        '--population-dir',
        type=str,
        default=None,
        help='Override population data dir (default: params.ini / fixtures)',
    )
    args = parser.parse_args()
    dates = [d.strip() for d in args.dates.split(',') if d.strip()]
    run(dates, args.transaction_dir, args.population_dir)


if __name__ == '__main__':
    main()
