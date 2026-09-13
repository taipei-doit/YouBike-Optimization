# Description
This project is a machine learning-based model designed to predict the potential demand for [YouBike](https://en.youbike.com.tw/region/main/).

We have divided Taipei City into 4,309 grids, each measuring 250 meters by 250 meters. We collect data within these grids that is relevant to the city or related to YouBike usage, such as the length of bicycle lanes and pedestrian walkway areas. There are primarily three types of datasets: EasyCard transaction data, signal data, and open data. And the EasyCard transaction data serves as the model's target, providing hourly rental and return counts. However, not all of these 4,309 grids have YouBike stations. Only those grids with existing YouBike stations can have rental and return counts derived from transaction data. Grids without stations will become inference data.

At the end, we will predict the hourly rental and return counts for grids that do not have YouBike stations yet. Through a formula, we will convert these counts into suggested additions of bike docks and the initial number of in-station bikes for each grid without stations. It's worth noting that not every grid without stations will require a YouBike station, especially in mountainous areas. For this part, we will establish certain criteria to filter grids that need station installation.

# Dataset
### EasyCard Transaction Data

This is de-identified data provided by **EasyCard Corporation**. And through EasyCard transaction data, we can calculate the hourly rental and return counts for each YouBike station.

### Signal Data

This is de-identified data provided by **Far EasTone Telecommunications Corporation**. Through this data, we can obtain hourly demographic information for each grid, including age groups, working population, residential population, and tourist population.

### Open Data
- **Transportation**

    Number of MRT Exits, MRT Passenger Flow (Inbound and Outbound), Number of Bus Stops, Number of Bus Routes

- **Road**

    Pedestrian Walkway Area, Marked Pedestrian Walkway Area, Bicycle Lane Length, Number of Trees on Sidewalks, Number of Streetlights, Road Network Area and Proportion, Length of Urban Expressways (Elevated Roads), Length of Provincial Highways, Length of Urban Roads (Avenues, Streets), Length of Urban Roads (Alleys, Lanes), Total Length"

- **Terrain**

    Slope, Slope Grade, Surface Elevation, NDVI Value, Green Coverage

- **Development**

    Commercial Total Volume and Ratio, Mixed-Use Total Volume and Ratio, Residential Total Volume and Ratio, Other Total Volume and Ratio, Total Volume and Ratio

- **Land**

    Natural Environment Percentage, Commercial Percentage, Residential Percentage, Mixed Residential Percentage, Industrial Percentage, Community Facilities Percentage, Educational Facilities Percentage, Recreational Facilities Percentage, Open Space Percentage, Transportation Facilities Percentage, Other Percentage

- **POI**

    Number of businesses, total review count, and total rating sum for the following business types: tourist attractions, shopping malls, laundry services, homestays, nightclubs, convenience stores, museums, supermarkets, clothing stores, bookstores, restaurants, cafes, and retail stores.

# Tech stack

| Layer | Tools | Notes |
|-------|--------|------|
| Tabular ETL (batch) | JDK 17+, Clojure CLI, tablecloth (`clj-etl/`) | Population + transaction aggregation |
| Python bridge | pandas, PyArrow, GeoPandas | `src/bridge/export_tabular.py` → staging Parquet |
| Spatial / open-data features | GeoPandas, Shapely, Rasterio, rasterstats | Remains in `preprocess.py` |
| Modeling | LightGBM, XGBoost, CatBoost, Optuna | `train.py` / `inference.py` |

Clojure does **not** own GIS, raster, or model training. See [`clj-etl/README.md`](clj-etl/README.md) for staging schemas.

```
pickle / stop CSV ──(Python bridge)──► staging/txn_raw.parquet
                                       staging/stop_grid_map.parquet
population CSV ──┐                     staging/grid_ids.parquet
                 ▼
            Clojure ETL ──► staging/base_keys.parquet
                            staging/features_*.parquet
                 ▼
     preprocess.py (GIS merge) ──► DF.csv ──► train / inference
```

# Usage

## Environment

```bash
conda env create -f environment.yml
conda activate youbike_potential_demand
```

Also install **JDK 17+** and the [Clojure CLI](https://clojure.org/guides/install_clojure).

## Hybrid pipeline (recommended)

From `Potential-Demand/`:

```bash
make pipeline                 # bridge → etl-clj → parity → preprocess
make bridge                   # Python: export staging inputs
make etl-clj                  # Clojure: tabular features
make parity                   # Compare Clojure vs legacy Python tabular outputs
make preprocess               # Python: merge staging + GIS → DF.csv
```

Override dates: `make pipeline DATES=20230305,20230311`.

## Legacy / modeling entrypoints

After staging + preprocess have produced `DF.csv`, you can still run `main.py` for modeling and inference (preprocess inside `main.py` now expects staging files).

Configure `input/params.ini` (`target` = `"on"` / `"off"`, Optuna search spaces, paths).

# Preprocess
As mentioned earlier, we have divided Taipei City into 4,309 grids, and we collect data for each grid. Each row of data includes grid ID, date, hour, rental and return counts, and features. If there are multiple YouBike stations within a single grid, the rental and return counts represent the sum of the counts for those stations within that hour.

Tabular rental/return and population features are produced by the Clojure batch and merged in `preprocess.py`; road/terrain/land/POI features are still computed in Python.

# Model Training
We have developed two sets of models, one for predicting rental counts and the other for predicting return counts. Through **ensemble learning** , including **LightGBM**, **XGBoost**, and **CatBoost**, to build these models.

For hyperparameter tuning, we initially defined parameter spaces for each hyperparameter separately and then utilized **Optuna** to select the optimal combination of hyperparameters that yields the best performance.

# License
This project is licensed under the Apache 2.0 License. For more details, please refer to the LICENSE file.

# Acknowledgements
The authors thank **Department of Transportation, Taipei City Government** for their invaluable collaboration throughout the project, from its initiation, through ongoing communication, to its successful execution. They played a crucial role in this endeavor. We would also like to express our gratitude to **YouBike Corporation**, **EasyCard Corporation**, and **Far EasTone Telecommunications Corporation** for providing the data that enabled us to complete this project.

# References
1. [臺北市資料大平台](https://data.taipei/)
2. [政府資料開放平台](https://data.gov.tw/)
3. [國土測繪圖資服務雲](https://maps.nlsc.gov.tw/S09SOA/)
4. [LightGBM](https://lightgbm.readthedocs.io/en/stable/)
5. [XGBoost](https://xgboost.readthedocs.io/en/stable/)
6. [CatBoost](https://catboost.ai/)
7. [Optuna](https://optuna.org/)
