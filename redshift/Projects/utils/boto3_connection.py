import boto3
import botocore


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


def add_file(file_path, bucket_name, object_name=None, region_name='us-east-2'):
    client = boto3.client('s3')

    if object_name is None:
        client.upload_file(file_path, bucket_name)
    else:
        with open(file_path, 'rb') as file:
            client.upload_fileobj(file, bucket_name, object_name)
    return 1


def delete_file(bucket_name, file_name):
    resource = boto3.resource('s3')
    resource.Object(bucket_name, file_name).delete()
    return 1


def delete_folder(bucket_name, folder_name):
    resource = boto3.resource('s3')
    bucket = resource.Bucket(bucket_name)
    bucket.objects.filter(Prefix=f"{folder_name}/").delete()


delete_folder('cnastoski-boto3bucket', 'test')
