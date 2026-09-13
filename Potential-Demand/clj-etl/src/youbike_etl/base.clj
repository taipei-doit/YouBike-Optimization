(ns youbike-etl.base
  "Build GridID × Date × Hour base keys with weekend flags."
  (:require [tablecloth.api :as tc]
            [youbike-etl.io :as io])
  (:import [java.time LocalDate]
           [java.time.format DateTimeFormatter]))

(def ^:private yyyymmdd
  (DateTimeFormatter/ofPattern "yyyyMMdd"))

(defn- weekday-pandas
  "Match pandas .dt.weekday (Mon=0 .. Sun=6)."
  [^String date-str]
  (let [d (LocalDate/parse date-str yyyymmdd)
        iso (.getValue (.getDayOfWeek d))]
    (long (mod (dec iso) 7))))

(defn- weekend?
  [^String date-str]
  (if (> (weekday-pandas date-str) 4) 1 0))

(defn build-base-keys
  [grid-ids-path dates out-path]
  (let [grids (-> (io/read-parquet grid-ids-path)
                  (tc/column "GridID")
                  vec)
        hours (range 24)
        rows (for [g grids
                   d dates
                   h hours]
               {"GridID" (long g)
                "Date" (str d)
                "Hour" (long h)
                "Weekday" (weekday-pandas d)
                "IsWeekend" (long (weekend? d))})
        ds (tc/dataset rows)]
    (io/write-parquet! ds out-path)
    ds))
