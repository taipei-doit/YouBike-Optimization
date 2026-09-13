(ns youbike-etl.core
  "CLI entry: run tabular ETL into ../staging Parquet files."
  (:require [clojure.string :as str]
            [youbike-etl.base :as base]
            [youbike-etl.population :as population]
            [youbike-etl.transaction :as transaction])
  (:gen-class))

(def default-dates
  ["20230305" "20230311" "20230317" "20230322"])

(defn- parse-args
  [args]
  (loop [args args
         acc {:dates default-dates
              :staging "../staging"
              :population-dir "../input/data/population"}]
    (cond
      (empty? args) acc
      (= "--dates" (first args))
      (recur (nnext args)
             (assoc acc :dates (str/split (second args) #",")))
      (= "--staging" (first args))
      (recur (nnext args) (assoc acc :staging (second args)))
      (= "--population-dir" (first args))
      (recur (nnext args) (assoc acc :population-dir (second args)))
      :else
      (throw (ex-info (str "Unknown arg: " (first args)) {:arg (first args)})))))

(defn run
  [{:keys [dates staging population-dir]}]
  (let [txn-raw (str staging "/txn_raw.parquet")
        stop-grid (str staging "/stop_grid_map.parquet")
        grid-ids (str staging "/grid_ids.parquet")
        base-out (str staging "/base_keys.parquet")
        pop-out (str staging "/features_population.parquet")
        txn-out (str staging "/features_transaction.parquet")]
    (println "Building base keys...")
    (base/build-base-keys grid-ids dates base-out)
    (println "Building population features...")
    (population/build-population-features population-dir pop-out)
    (println "Building transaction features...")
    (transaction/build-transaction-features txn-raw stop-grid txn-out)
    (println "Clojure ETL complete.")))

(defn -main
  [& args]
  (run (parse-args args)))
