import boto3
import botocore
import cust_db_driver as db


def createBucket(bucketname: str, region='us-east-2'):
    s3 = boto3.client('s3', region_name=region)
    location = {'LocationConstraint': region}

    bucket_data = s3.list_buckets()['Buckets']
    bucket_names = []
    for name in bucket_data:
        bucket_names.append(name['Name'])

    if bucketname in bucket_names:
        return print("ERROR: bucket already exists")

    try:
        s3.create_bucket(Bucket=bucketname, CreateBucketConfiguration=location)

    except botocore.exceptions.ClientError as error:
        print(error)
        return

    bucket_data = s3.list_buckets()['Buckets']
    bucket_names = []
    for name in bucket_data:
        bucket_names.append(name['Name'])

    print(f"Bucket List: {bucket_names}")

    return 1


#createBucket('cnastoski-boto3test2')

query = "unload (%s) to %s iam_role %s HEADER FORMAT as CSV PARALLEL OFF"
args = ["select * from detailed_view", "s3://cnastoski-boto3test2/", "arn:aws:iam::432167795286:role/service-role/AmazonRedshift-CommandsAccessRole-20230110T085953" ]
db.do_query(query, args)