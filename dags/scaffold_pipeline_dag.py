from datetime import datetime
from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator

from chem_utils.molecule_gen import generate_molecules
from chem_utils.properties import calculate_properties
from chem_utils.clustering import cluster_molecules
from chem_utils.discovery import discover_new_datasets
from chem_utils.quality_checks import run_quality_checks
from chem_utils.notifications import notify_teams


def _task_failure_alert(context):
    ti = context["task_instance"]
    notify_teams(
        f"Task `{ti.task_id}` failed in run `{ti.run_id}`.\nCheck Airflow logs for details.",
        is_error=True,
    )


default_args = {
    "owner": "cheminformatics",
    "retries": 1,
    "email_on_failure": False,
    "on_failure_callback": _task_failure_alert,
}


def _discover_new_datasets(**context):
    overwrite = context["params"]["overwrite"]
    prev_success = context.get("prev_start_date_success")
    return discover_new_datasets(overwrite, prev_success)


def _process_dataset(dataset_id, **context):
    """Run generate -> properties -> quality check -> cluster for one dataset.
    Notifies MS Teams on success, and on quality-check failure specifically
    (task-level failures are also caught by on_failure_callback above)."""
    generate_molecules(dataset_id)
    calculate_properties(dataset_id)

    issues = run_quality_checks(dataset_id)
    if issues:
        notify_teams(
            f"Dataset `{dataset_id}` failed data quality checks:\n- " + "\n- ".join(issues),
            is_error=True,
        )
        raise ValueError(f"Data quality check failed for {dataset_id}: {issues}")

    cluster_molecules(dataset_id)

    notify_teams(f"Dataset `{dataset_id}` processed successfully.", is_error=False)


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
