import sys
sys.path.append('C:/Users/cnast/Desktop/aws_learning-main/redshift/Projects/utils')
import boto3_connection as b3
import db_driver as db

bucket_name = "cnastoski-boto3bucket"
b3.createBucket(bucket_name)
query = "unload (%s) to %s iam_role %s HEADER FORMAT as CSV PARALLEL OFF"
args = ["select * from cards_ingest.detailed_view", f"s3://{bucket_name}/unload_files/",
        "arn:aws:iam::432167795286:role/service-role/AmazonRedshift-CommandsAccessRole-20230110T085953"]

db.do_query_string(query, args)