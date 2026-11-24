"""Parse every DAG file and fail when one does not import.

Runs inside the Astro runtime image (the same Airflow and Python as the VPS),
with the dags/ folder mounted at /usr/local/airflow/dags:

    docker run --rm -v "$PWD/dags:/usr/local/airflow/dags:ro" <image> \
        python /usr/local/airflow/deploy/check_dag_imports.py

Used by CI (.github/workflows/ci.yml, job dag-import) and by the nightly
deploy before it restarts Airflow (deploy/vps/deploy.sh, pre-flight).
Exit 0 = all DAG files import and at least one DAG was found.
"""
from __future__ import annotations

import os
import sys

# No database and no .env in the check: the DAG files must import without them.
os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")

try:  # Airflow 3.1+
    from airflow.dag_processing.dagbag import DagBag
except ImportError:  # older Airflow
    from airflow.models.dagbag import DagBag


def main() -> int:
    folder = sys.argv[1] if len(sys.argv) > 1 else os.path.join(
        os.environ.get("AIRFLOW_HOME", "/usr/local/airflow"), "dags")
    bag = DagBag(dag_folder=folder, include_examples=False)
    for path, err in sorted(bag.import_errors.items()):
        print(f"IMPORT ERROR {path}:\n{err}", file=sys.stderr)
    print(f"dags found: {len(bag.dag_ids)}: {' '.join(sorted(bag.dag_ids))}")
    if bag.import_errors:
        print(f"FAIL: {len(bag.import_errors)} DAG file(s) do not import",
              file=sys.stderr)
        return 1
    if not bag.dag_ids:
        print(f"FAIL: no DAGs found in {folder}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
