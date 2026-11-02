.PHONY: install schema seed reset-dev test test-db smoke

# Seed range for `make seed` / `make reset-dev`. Override on the command line:
#   make seed SEED_FROM=2026-01-01 SEED_TO=2026-12-31
SEED_FROM ?= 2019-01-01
SEED_TO   ?= 2027-12-31

install:
	pip install -r requirements.txt

# Empty database: tables, functions, indexes.
schema:
	@test -n "$(SPP_DSN)" || { echo "ERROR: SPP_DSN is empty. Set it, e.g. make $@ SPP_DSN=postgresql://..."; exit 1; }
	psql "$(SPP_DSN)" -v ON_ERROR_STOP=1 -f schema/01_schema.sql

# Spine rows and monthly partitions. Safe to run again.
seed:
	@test -n "$(SPP_DSN)" || { echo "ERROR: SPP_DSN is empty. Set it, e.g. make $@ SPP_DSN=postgresql://..."; exit 1; }
	psql "$(SPP_DSN)" -v ON_ERROR_STOP=1 -v seed_from=$(SEED_FROM) -v seed_to=$(SEED_TO) -f schema/02_seed.sql

# DEV ONLY: delete all pipeline tables and build them again from zero.
reset-dev:
	@test -n "$(SPP_DSN)" || { echo "ERROR: SPP_DSN is empty. Set it, e.g. make $@ SPP_DSN=postgresql://..."; exit 1; }
	psql "$(SPP_DSN)" -v ON_ERROR_STOP=1 -f schema/00_reset_dev.sql
	$(MAKE) schema seed

test:
	pytest -q

test-db:
	SPP_TEST_DSN="$(SPP_TEST_DSN)" pytest -q

smoke:
	python3 -m spp.run --feed rt_lmp --check-db
	python3 -m spp.run --feed rt_lmp --list-groups
