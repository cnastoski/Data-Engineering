from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from datetime import datetime
from datetime import timedelta

default_args = {
    'owner': 'me',
    'start_date': datetime(2023, 2, 19),
    'retry_delay': timedelta(minutes=5),
    'email': 'abc.com',
    'email_on_failure': True
}

with DAG(
  dag_id="run_glue_job",
  start_date=days_ago(1),
  schedule_interval=None,
) as dag:
    glue_job = GlueJobOperator(
    task_id="run_glue_job",
    job_name="tran_fact_query_job",
    create_job_kwargs={"GlueVersion": "3.0", "NumberOfWorkers": 2, "WorkerType": "G.1X"},
)

    glue_job