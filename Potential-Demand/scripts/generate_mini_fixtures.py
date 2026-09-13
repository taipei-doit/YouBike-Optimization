"""
Generate tiny deterministic fixtures for bridge → etl-clj → parity smoke tests.

Stdlib-only. Writes under fixtures/mini/.
Txn pickle stores a list[dict] (bridge/preprocess coerce to DataFrame).
"""

from __future__ import annotations

import csv
import json
import pickle
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / 'fixtures' / 'mini'
SMOKE_DATE = '20230305'

STOPS = [
    {'sno': '500000001', 'lng': 121.5200, 'lat': 25.0400},
    {'sno': '500000002', 'lng': 121.5210, 'lat': 25.0410},
    {'sno': '500000003', 'lng': 121.5220, 'lat': 25.0420},
]

GRID_COORDS_3826 = [
    [300000.0, 2760000.0],
    [320000.0, 2760000.0],
    [320000.0, 2780000.0],
    [300000.0, 2780000.0],
    [300000.0, 2760000.0],
]


def _write_stops(txn_dir: Path) -> None:
    txn_dir.mkdir(parents=True, exist_ok=True)
    path = txn_dir / '86ec099baa2d36c22ab3a87350b718de_export.csv'
    with path.open('w', newline='', encoding='utf-8') as handle:
        writer = csv.DictWriter(handle, fieldnames=['sno', 'lng', 'lat'])
        writer.writeheader()
        writer.writerows(STOPS)


def _write_txn_pkl(txn_dir: Path) -> None:
    folder = txn_dir / '202303_txn_identified_transfer'
    folder.mkdir(parents=True, exist_ok=True)
    rows = [
        {
            'on_stop_id': 'U000001',
            'off_stop_id': 'U000002',
            'on_time': '2023-03-05 08:15:00',
            'off_time': '2023-03-05 08:40:00',
        },
        {
            'on_stop_id': 'U000001',
            'off_stop_id': 'U000001',
            'on_time': '2023-03-05 08:20:00',
            'off_time': '2023-03-05 09:05:00',
        },
        {
            'on_stop_id': 'U000002',
            'off_stop_id': 'U000003',
            'on_time': '2023-03-05 18:10:00',
            'off_time': '2023-03-05 18:35:00',
        },
    ]
    with open(folder / f'{SMOKE_DATE}.pkl', 'wb') as handle:
        pickle.dump(rows, handle)


def _write_grid_geojson(pop_dir: Path) -> None:
    pop_dir.mkdir(parents=True, exist_ok=True)
    fc = {
        'type': 'FeatureCollection',
        'crs': {
            'type': 'name',
            'properties': {'name': 'urn:ogc:def:crs:EPSG::3826'},
        },
        'features': [
            {
                'type': 'Feature',
                'properties': {'gridid': 1001},
                'geometry': {
                    'type': 'Polygon',
                    'coordinates': [GRID_COORDS_3826],
                },
            }
        ],
    }
    (pop_dir / 'FET_2023_grid_97.geojson').write_text(
        json.dumps(fc), encoding='utf-8'
    )


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict]) -> None:
    with path.open('w', newline='', encoding='utf-8') as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _write_population_csvs(pop_dir: Path) -> None:
    ages = [
        '15-17歲', '18-21歲', '22-29歲', '30-39歲',
        '40-49歲', '50-59歲', '60-64歲', '65歲以上',
    ]
    rows = []
    for hour in (8, 9, 18):
        for i, age in enumerate(ages):
            rows.append({
                '日期': '2023-03-05',
                '時間': hour,
                '網格編號': 1001,
                '年齡別': age,
                '放大後人數': float(10 + i + hour),
            })
    _write_csv(
        pop_dir / '台北市停留人口_資料集_1.csv',
        ['日期', '時間', '網格編號', '年齡別', '放大後人數'],
        rows,
    )

    role_rows = [
        {
            '日期': '2023-03-05',
            '時間': hour,
            '網格編號': 1001,
            '放大後人數': float(100 + hour),
        }
        for hour in (8, 9, 18)
    ]
    for name in (
        '台北市停留人口_資料集_2_工作人口.csv',
        '台北市停留人口_資料集_2_居住人口.csv',
        '台北市停留人口_資料集_2_遊客人口.csv',
    ):
        _write_csv(
            pop_dir / name,
            ['日期', '時間', '網格編號', '放大後人數'],
            role_rows,
        )


def main() -> None:
    txn_dir = OUT / 'transaction'
    pop_dir = OUT / 'population'
    _write_stops(txn_dir)
    _write_txn_pkl(txn_dir)
    _write_grid_geojson(pop_dir)
    _write_population_csvs(pop_dir)
    print(f'Wrote mini fixtures under {OUT}')
    print(f'Smoke date: {SMOKE_DATE}')


if __name__ == '__main__':
    main()
