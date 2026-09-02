.PHONY: install schema test test-db lint smoke

install:
	pip install -r requirements.txt

schema:
	psql "$(SPP_DSN)" -v ON_ERROR_STOP=1 -f schema/010_schema.sql
	psql "$(SPP_DSN)" -v ON_ERROR_STOP=1 -f schema/011_partitions_and_seed.sql

test:
	pytest -q

test-db:
	SPP_TEST_DSN="$(SPP_TEST_DSN)" pytest -q

smoke:
	python3 -m spp.run --feed rt_lmp --check-db
	python3 -m spp.run --feed rt_lmp --list-groups
