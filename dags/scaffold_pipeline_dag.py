from datetime import datetime
from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator

from chem_utils.molecule_gen import generate_molecules
from chem_utils.properties import calculate_properties
from chem_utils.clustering import cluster_molecules
from chem_utils.discovery import discover_new_datasets

default_args = {
    "owner": "cheminformatics",
    "retries": 1,
    "email_on_failure": False,
}


def _discover_new_datasets(**context):
    """List scaffold files in S3, filter to only new ones since last successful
    run unless overwrite=True."""
    overwrite = context["params"]["overwrite"]
    prev_success = context.get("prev_start_date_success")
    return discover_new_datasets(overwrite, prev_success)


def _process_dataset(dataset_id, **context):
    """Run the full generate -> properties -> cluster chain for a single dataset."""
    generate_molecules(dataset_id)
    calculate_properties(dataset_id)
    cluster_molecules(dataset_id)


with DAG(
    dag_id="scaffold_pipeline",
    schedule="@weekly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    params={"overwrite": Param(type="boolean", default=False)},
    default_args=default_args,
) as dag:

    discover = PythonOperator(
        task_id="discover_new_datasets",
        python_callable=_discover_new_datasets,
    )

    process = PythonOperator.partial(
        task_id="process_dataset",
        python_callable=_process_dataset,
    ).expand(
        op_kwargs=discover.output.map(lambda dataset_id: {"dataset_id": dataset_id})
    )

    discover >> process
