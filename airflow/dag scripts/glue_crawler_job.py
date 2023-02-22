from airflow import DAG
from airflow.operators.email_operator import EmailOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from datetime import datetime, timedelta
from airflow.sensors.s3_key_sensor import S3KeySensor


default_args = {
    'owner': 'me',
    'start_date': datetime(2023, 2, 19),
    'retry_delay': timedelta(minutes=5),
    'email': 'nastoski@msu.edu',
    'email_on_failure': True
}

with DAG(dag_id='glue_af_pipeline', default_args=default_args, schedule_interval=None) as dag:

    glue_cust_job = GlueJobOperator(
        task_id="glue_cust_job",
        job_name='glue_first_job',
    )

    s3_cust_file_chk = S3KeySensor(
        task_id='s3_cust_file_chk',
        bucket_key='cards_account_ingest_2022-01-02.csv',
        wildcard_match=True,
        bucket_name='cnastoski-clibucket-ohio',
        timeout=18 * 60 * 60,
        poke_interval=60 )
    glue_crawler_config = {
        "Name": 'customer_details_parquet_write_chris',
        # "Role": role_arn,
        # "DatabaseName": glue_db_name,
        # "Targets": {"S3Targets": [{"Path": f"{bucket_name}/input"}]},
    }
    crawl_s3 = GlueCrawlerOperator(
        task_id="crawl_s3",
        config=glue_crawler_config,
    )

    # submit_glue_job = GlueJobOperator(
    #     task_id="submit_glue_job",
    #     job_name='cust_transform',
    #     script_location=f"s3://{bucket_name}/etl_script.py",
    #     s3_bucket=bucket_name,
    #     create_job_kwargs={"GlueVersion": "3.0", "NumberOfWorkers": 2, "WorkerType": "G.1X"},
    # )

    s3_cust_file_chk >> crawl_s3 >> glue_cust_job