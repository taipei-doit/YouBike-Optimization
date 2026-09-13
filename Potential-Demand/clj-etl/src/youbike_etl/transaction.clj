(ns youbike-etl.transaction
  "Tabular transaction aggregation after stop→grid map (spatial overlay stays in Python)."
  (:require [clojure.string :as str]
            [tablecloth.api :as tc]
            [youbike-etl.io :as io]))

(defn- parse-date-hour
  [ds time-col date-col hour-col]
  (-> ds
      (tc/map-columns date-col :string
                      (fn [t]
                        (-> (str t)
                            (subs 0 10)
                            (str/replace "-" "")))
                      time-col)
      (tc/map-columns hour-col :int64
                      (fn [t]
                        (Long/parseLong (subs (str t) 11 13)))
                      time-col)))

(defn- attach-grid
  [counts-ds stop-col stop-grid]
  (let [renamed-map (tc/rename-columns stop-grid {"stop_id" stop-col})]
    (-> (tc/left-join counts-ds renamed-map [stop-col])
        (tc/drop-missing "gridid")
        (tc/convert-types {"counts" :int64 "gridid" :int64}))))

(defn- agg-by-grid
  [ds date-col hour-col out-col]
  (-> ds
      (tc/group-by ["gridid" date-col hour-col])
      (tc/aggregate {out-col #(apply + (% "counts"))})
      (tc/ungroup)
      (tc/rename-columns {"gridid" "GridID"
                          date-col "Date"
                          hour-col "Hour"})))

(defn build-transaction-features
  [txn-raw-path stop-grid-path out-path]
  (let [raw (io/read-parquet txn-raw-path)
        stop-grid (io/read-parquet stop-grid-path)
        parsed (-> raw
                   (parse-date-hour "on_time" "on_date" "on_hour")
                   (parse-date-hour "off_time" "off_date" "off_hour"))
        on-stops (vec (distinct (tc/column parsed "on_stop_id")))
        on-dates (vec (distinct (tc/column parsed "on_date")))
        on-hours (vec (distinct (tc/column parsed "on_hour")))
        product (tc/dataset
                 (for [s on-stops d on-dates h on-hours]
                   {"on_stop_id" s "on_date" d "on_hour" (long h)
                    "off_stop_id" s "off_date" d "off_hour" (long h)}))
        on-counted (-> parsed
                       (tc/group-by ["on_stop_id" "on_date" "on_hour"])
                       (tc/aggregate {"counts" tc/row-count})
                       (tc/ungroup))
        off-counted (-> parsed
                        (tc/group-by ["off_stop_id" "off_date" "off_hour"])
                        (tc/aggregate {"counts" tc/row-count})
                        (tc/ungroup))
        on-full (-> (tc/left-join
                     (tc/select-columns product ["on_stop_id" "on_date" "on_hour"])
                     on-counted
                     ["on_stop_id" "on_date" "on_hour"])
                    (tc/replace-missing "counts" :value 0)
                    (attach-grid "on_stop_id" stop-grid))
        off-full (-> (tc/left-join
                      (tc/select-columns product ["off_stop_id" "off_date" "off_hour"])
                      off-counted
                      ["off_stop_id" "off_date" "off_hour"])
                     (tc/replace-missing "counts" :value 0)
                     (attach-grid "off_stop_id" stop-grid))
        on-grid (agg-by-grid on-full "on_date" "on_hour" "OnCounts")
        off-grid (agg-by-grid off-full "off_date" "off_hour" "OffCounts")
        joined (-> (tc/full-join on-grid off-grid ["GridID" "Date" "Hour"])
                   (tc/replace-missing ["OnCounts" "OffCounts"] :value 0)
                   (tc/map-columns "NetCounts" :int64
                                   (fn [off on] (long (- (long off) (long on))))
                                   "OffCounts" "OnCounts")
                   (tc/convert-types {"OnCounts" :int64
                                      "OffCounts" :int64
                                      "NetCounts" :int64
                                      "GridID" :int64
                                      "Hour" :int64
                                      "Date" :string}))]
    (io/write-parquet! joined out-path)
    joined))
