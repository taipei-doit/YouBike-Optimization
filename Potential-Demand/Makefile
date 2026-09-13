# YouBike Potential-Demand hybrid pipeline
# Tabular ETL: Clojure | Spatial + ML: Python

DATES ?= 20230305,20230311,20230317,20230322
PYTHON ?= python
LOCAL_CLJ := $(CURDIR)/clj-etl/.tools/bin/clojure
CLJ ?= $(shell if [ -x "$(LOCAL_CLJ)" ]; then echo "$(LOCAL_CLJ)"; else echo clojure; fi)
export CLJ_CONFIG ?= $(CURDIR)/clj-etl/.clj-config

.PHONY: bridge etl-clj preprocess pipeline parity help

help:
	@echo "Targets:"
	@echo "  make bridge      - Export txn/stop-grid/grid_ids Parquet via Python"
	@echo "  make etl-clj     - Run Clojure tabular feature ETL into staging/"
	@echo "  make preprocess  - Merge staging + GIS features -> DF.csv"
	@echo "  make pipeline    - bridge + etl-clj + parity + preprocess"
	@echo "  make parity      - Compare Clojure staging vs legacy Python tabular"

bridge:
	$(PYTHON) -m src.bridge.export_tabular --dates $(DATES)

etl-clj:
	@mkdir -p "$(CLJ_CONFIG)"
	cd clj-etl && $(CLJ) -M:run --dates $(DATES) --staging ../staging --population-dir ../input/data/population

parity:
	$(PYTHON) -m src.bridge.parity_check --dates $(DATES)

preprocess:
	$(PYTHON) -c "import preprocess; preprocess.Preprocess(date_list='$(DATES)'.split(',')).run()"

pipeline: bridge etl-clj parity preprocess
	@echo "Hybrid pipeline finished."
