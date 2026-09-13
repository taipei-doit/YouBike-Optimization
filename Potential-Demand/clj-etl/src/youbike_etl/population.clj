(ns youbike-etl.population
  "Population signal ETL mirroring preprocess.population + population_merge keys."
  (:require [clojure.string :as str]
            [tablecloth.api :as tc]
            [youbike-etl.io :as io]))

(def ^:private age-rename
  {"15-17歲" "Age_15_17_Counts"
   "18-21歲" "Age_18_21_Counts"
   "22-29歲" "Age_22_29_Counts"
   "30-39歲" "Age_30_39_Counts"
   "40-49歲" "Age_40_49_Counts"
   "50-59歲" "Age_50_59_Counts"
   "60-64歲" "Age_60_64_Counts"
   "65歲以上" "Age_Over65_Counts"})

(def ^:private age-cols
  (vals age-rename))

(defn- strip-dashes
  [ds col]
  (tc/map-columns ds col :string
                  (fn [v] (-> (str v) (str/replace "-" "")))))

(defn- load-pop-csv
  [path]
  (-> (io/read-csv path)
      (strip-dashes "日期")))

(defn- pivot-age
  [population-df]
  (let [grouped (-> population-df
                    (tc/group-by ["日期" "時間" "網格編號" "年齡別"])
                    (tc/aggregate {"放大後人數" #(apply + (% "放大後人數"))})
                    (tc/ungroup))
        wide (-> grouped
                 (tc/pivot->wider "年齡別" "放大後人數" {:drop-missing? false})
                 (tc/rename-columns age-rename))
        with-ages (reduce
                   (fn [ds col]
                     (if (some #{col} (map name (tc/column-names ds)))
                       ds
                       (tc/add-column ds col 0)))
                   wide
                   age-cols)
        age-names (vec age-cols)]
    (-> with-ages
        (tc/replace-missing age-names :value 0)
        (tc/map-columns "Age_Total_Counts" :float64
                        (fn [& xs] (double (apply + (map #(or % 0.0) xs))))
                        age-names))))

(defn- merge-role
  [df role-df out-col]
  (let [role (-> role-df
                 (tc/select-columns ["日期" "時間" "網格編號" "放大後人數"])
                 (tc/rename-columns {"放大後人數" out-col}))]
    (-> (tc/left-join df role ["日期" "時間" "網格編號"])
        (tc/replace-missing out-col :value 0))))

(defn build-population-features
  [population-dir out-path]
  (let [base (str population-dir)
        population-df (load-pop-csv (str base "/台北市停留人口_資料集_1.csv"))
        work-df (load-pop-csv (str base "/台北市停留人口_資料集_2_工作人口.csv"))
        live-df (load-pop-csv (str base "/台北市停留人口_資料集_2_居住人口.csv"))
        tour-df (load-pop-csv (str base "/台北市停留人口_資料集_2_遊客人口.csv"))
        pivoted (pivot-age population-df)
        merged (-> pivoted
                   (merge-role work-df "WorkPopulationCounts")
                   (merge-role live-df "LivePopulationCounts")
                   (merge-role tour-df "TourPopulationCounts"))
        out (-> merged
                (tc/rename-columns {"網格編號" "GridID"
                                    "日期" "Date"
                                    "時間" "Hour"})
                (tc/convert-types {"GridID" :int64
                                   "Date" :string
                                   "Hour" :int64}))]
    (io/write-parquet! out out-path)
    out))
