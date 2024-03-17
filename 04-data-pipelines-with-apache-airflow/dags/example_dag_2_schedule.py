from airflow import DAG
from airflow.utils import timezone
from airflow.operators.empty import EmptyOperator

# DAG declaration
with DAG(
    dag_id="example_dag_2_schedule",
    start_date=timezone.datetime(2024,3,10),
    schedule="0 0 * * 1",
    tags=["DEB","Skoodio"],
    catchup=False,
    # mean you don't have to run retrospectively.
):
    t1 = EmptyOperator(task_id="t1")
    t2 = EmptyOperator(task_id="t2")

    t1 >> t2