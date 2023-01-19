from redshift.Projects.utils import boto3_connection as b3
from hadoop.Project.utils import transaction_create as tc
import os

# create all files for the test data
start_date, end_date = '2022-05-09', '2022-05-16'
tc.create_data(start_date,end_date)

#add files into s3
#add_file('C:/Users/cnast/Desktop/aws_learning-main/hadoop/Project/config/2022-01-01.csv', 'cnastoski-boto3bucket', 'test_data/2022-01-01.csv')


