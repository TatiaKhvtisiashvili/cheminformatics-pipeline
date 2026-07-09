from datetime import datetime
from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator

from chem_utils.molecule_gen import generate_molecules
from chem_utils.properties import calculate_properties
from chem_utils.clustering import cluster_molecules

default_args = {
    "owner": "cheminformatics",
    "retries": 1,
    "email_on_failure": False,
}

with DAG(
    dag_id="scaffold_pipeline",
    schedule=None,  # manual trigger only for step 1
    start_date=datetime(2026, 1, 1),
    catchup=False,
    params={"dataset_id": Param(type="string", default="")},
    default_args=default_args,
) as dag:

    def _require_dataset_id(context):
        dataset_id = context["params"]["dataset_id"]
        if not dataset_id:
            raise ValueError("dataset_id parameter is strictly required")
        return dataset_id

    def _generate(**context):
        dataset_id = _require_dataset_id(context)
        generate_molecules(dataset_id)

    def _properties(**context):
        dataset_id = _require_dataset_id(context)
        calculate_properties(dataset_id)

    def _cluster(**context):
        dataset_id = _require_dataset_id(context)
        cluster_molecules(dataset_id)

    t1 = PythonOperator(task_id="generate_molecules", python_callable=_generate)
    t2 = PythonOperator(task_id="calculate_properties", python_callable=_properties)
    t3 = PythonOperator(task_id="cluster_molecules", python_callable=_cluster)

    t1 >> t2 >> t3
