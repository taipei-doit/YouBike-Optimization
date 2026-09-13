# YouBike Potential-Demand hybrid pipeline
# Tabular ETL: Clojure (Docker by default) | Spatial + ML: Python
# Not microservices — file-based batch containers only.

DATES ?= 20230305,20230311,20230317,20230322
SMOKE_DATES ?= 20230305
PYTHON ?= python
COMPOSE ?= docker compose
TXN_DIR ?=
POP_DIR ?=
FIXTURE_TXN := fixtures/mini/transaction
FIXTURE_POP := fixtures/mini/population

.PHONY: help docker-build fixtures fixtures-docker bridge bridge-docker \
	etl-clj parity parity-docker preprocess smoke smoke-docker pipeline

help:
	@echo "Targets:"
	@echo "  make docker-build  - Build python-batch + clj-etl images"
	@echo "  make fixtures      - Generate fixtures/mini (needs pandas)"
	@echo "  make smoke         - Host python bridge/parity + Docker clj-etl"
	@echo "  make smoke-docker  - Full smoke in Docker (no host conda/JDK)"
	@echo "  make bridge        - Python: export staging inputs"
	@echo "  make etl-clj       - Clojure ETL via Docker (no host JDK)"
	@echo "  make parity        - Compare Clojure staging vs legacy Python"
	@echo "  make preprocess    - Merge staging + GIS -> DF.csv (real input/)"
	@echo "  make pipeline      - bridge + etl-clj + parity + preprocess"

docker-build:
	$(COMPOSE) build

fixtures:
	$(PYTHON) scripts/generate_mini_fixtures.py

fixtures-docker:
	$(COMPOSE) run --rm python-batch python scripts/generate_mini_fixtures.py

bridge:
	$(PYTHON) -m src.bridge.export_tabular --dates $(DATES) \
		$(if $(TXN_DIR),--transaction-dir $(TXN_DIR),) \
		$(if $(POP_DIR),--population-dir $(POP_DIR),)

bridge-docker:
	$(COMPOSE) run --rm python-batch python -m src.bridge.export_tabular --dates $(DATES) \
		$(if $(TXN_DIR),--transaction-dir $(TXN_DIR),) \
		$(if $(POP_DIR),--population-dir $(POP_DIR),)

etl-clj:
	$(COMPOSE) run --rm clj-etl \
		--dates $(DATES) \
		--staging ../staging \
		--population-dir $(if $(POP_DIR),../$(POP_DIR),../input/data/population)

parity:
	$(PYTHON) -m src.bridge.parity_check --dates $(DATES) \
		$(if $(TXN_DIR),--transaction-dir $(TXN_DIR),) \
		$(if $(POP_DIR),--population-dir $(POP_DIR),)

parity-docker:
	$(COMPOSE) run --rm python-batch python -m src.bridge.parity_check --dates $(DATES) \
		$(if $(TXN_DIR),--transaction-dir $(TXN_DIR),) \
		$(if $(POP_DIR),--population-dir $(POP_DIR),)

preprocess:
	$(PYTHON) -c "import preprocess; preprocess.Preprocess(date_list='$(DATES)'.split(',')).run()"

smoke: fixtures
	$(MAKE) bridge DATES=$(SMOKE_DATES) TXN_DIR=$(FIXTURE_TXN) POP_DIR=$(FIXTURE_POP)
	$(MAKE) etl-clj DATES=$(SMOKE_DATES) POP_DIR=$(FIXTURE_POP)
	$(MAKE) parity DATES=$(SMOKE_DATES) TXN_DIR=$(FIXTURE_TXN) POP_DIR=$(FIXTURE_POP)
	@echo "Smoke (bridge + etl-clj + parity) OK."

smoke-docker: docker-build
	$(MAKE) fixtures-docker
	$(MAKE) bridge-docker DATES=$(SMOKE_DATES) TXN_DIR=$(FIXTURE_TXN) POP_DIR=$(FIXTURE_POP)
	$(MAKE) etl-clj DATES=$(SMOKE_DATES) POP_DIR=$(FIXTURE_POP)
	$(MAKE) parity-docker DATES=$(SMOKE_DATES) TXN_DIR=$(FIXTURE_TXN) POP_DIR=$(FIXTURE_POP)
	@echo "Smoke-docker (bridge + etl-clj + parity) OK."

pipeline: bridge etl-clj parity preprocess
	@echo "Hybrid pipeline finished."
