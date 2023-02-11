from redshift.Projects.utils import boto3_connection as b3
from hadoop.Project.utils import transaction_create as tc
from redshift.Projects.utils import db_driver as db
import os

create_data = False
add_to_s3 = False
create_table = False
copy_to_table = False

# create all files for the test data
if create_data:
    start_date, end_date = '2022-05-09', '2022-05-16'
    tc.create_data(start_date, end_date)

# add all csv files into s3
if add_to_s3:
    # add_file('C:/Users/cnast/Desktop/aws_learning-main/hadoop/project/config/2022-01-01.csv',
    # 'cnastoski-boto3bucket', 'test_data/2022-01-01.csv')
    directory = "C:\\Users\\cnast\\Desktop\\aws_learning-main\\hadoop\\project\\config"

    for file in os.listdir(directory):
        # only upload csv files in the directory
        if file.split('.')[1] == "csv":
            s3_object_name = f"order_{file.split('.')[0]}/{file}"
            b3.add_file(f"{directory}/{file}", 'cnastoski-boto3bucket', object_name=s3_object_name)

# executes the sql query file to create the schema and table into redshift
if create_table:
    db.query_from_file("C:\\Users\\cnast\\Desktop\\aws_learning-main\\hadoop\\Project\\config\\create_table.sql")

# copy files from s3 to redshift
if copy_to_table:
    db.loadFromCSV("C:\\Users\\cnast\\Desktop\\aws_learning-main\\hadoop\\project\\config\\2022-05-09.csv", "tran_fact",
                   "hadoop")
