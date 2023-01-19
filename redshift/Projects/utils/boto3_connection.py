import boto3
import botocore
import db_driver as db


def createBucket(bucketname: str, region='us-east-2'):
    """
    :param bucketname : name of the bucket to create. Must be globally unique
    :param region : region that the bucket should be created in
    :returns : 1 if successful, prints error if fails
    """
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
        return 0

    bucket_data = s3.list_buckets()['Buckets']
    bucket_names = []
    for name in bucket_data:
        bucket_names.append(name['Name'])

    print(f"Bucket List: {bucket_names}")
    return 1
