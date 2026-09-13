(ns youbike-etl.io
  "Parquet / CSV helpers for staging exchange."
  (:require [tablecloth.api :as tc]
            [tech.v3.libs.parquet :as parquet]))

(defn- stringify-columns
  "Normalize column names to strings for stable joins with Python Parquet."
  [ds]
  (let [names (tc/column-names ds)
        rename (zipmap names (map name names))]
    (tc/rename-columns ds rename)))

(defn read-parquet
  [path]
  (-> (parquet/parquet->ds path)
      tc/dataset
      stringify-columns))

(defn write-parquet!
  [ds path]
  (let [ds (stringify-columns ds)]
    (parquet/ds->parquet ds path)
    (println (str "Wrote " path " (" (tc/row-count ds) " rows)"))))

(defn read-csv
  [path]
  (-> (tc/dataset path {:file-type :csv :separator \, :header-row? true})
      stringify-columns))
