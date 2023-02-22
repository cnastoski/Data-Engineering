from datetime import datetime
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow import DAG


default_args = {
    'owner': 'me',
    'start_date': days_ago(1),
    'retry_delay': timedelta(minutes=5),
    'email': 'nastoski@msu.edu',
    'email_on_failure': False
}

with DAG(dag_id='check_redshift_conn', default_args=default_args, schedule_interval=None) as dag:

    wait_cluster_available = RedshiftClusterSensor(
        task_id="wait_cluster_available",
        cluster_identifier="quintrix-aws-training",
        target_status="available",
        poke_interval=15,
        timeout=60 * 15,
    )