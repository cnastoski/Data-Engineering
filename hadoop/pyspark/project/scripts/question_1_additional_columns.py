import pyspark
from pyspark import SparkContext as sc
import os
from pyspark.sql import SparkSession
from functools import reduce
from pyspark.sql import DataFrame
from pyspark.sql.functions import concat_ws, lit, to_date, when
import pytz
from datetime import datetime

# create spark session
spark = SparkSession.builder \
    .master("local") \
    .appName("class_question1") \
    .getOrCreate()

# read every csv file one by one
df1 = spark.read.option("header", True).csv(
    "s3://cnastoski-pyspark/raw_data/dataprep/input_data/dataset_date=2022-01-11/")
df2 = spark.read.option("header", True).csv(
    "s3://cnastoski-pyspark/raw_data/dataprep/input_data/dataset_date=2022-01-12/")
df3 = spark.read.option("header", True).csv(
    "s3://cnastoski-pyspark/raw_data/dataprep/input_data/dataset_date=2022-01-13/")

df_list = [df1, df2, df3]
df_main = reduce(DataFrame.unionAll, df_list)

# likewise we can use below code to do it all in one line
# df = spark.read.option("header", True).load("s3://cnastoski-pyspark/raw_data/dataprep/input_data/*")

# create another column to add in current load_time for every record
time_zone= pytz.timezone('America/Detroit')
now = datetime.now(time_zone)
current_time = now.strftime("%Y-%m-%d")


loadtime_df = df_main.withColumn("load_time", lit(current_time))

# take first and last name columns and concat them into a new column and remove old ones
concat_df = loadtime_df.select(
    concat_ws(" ", loadtime_df.acct_hldr_first_name, loadtime_df.acct_hldr_last_name).alias("name"),
    loadtime_df.acct_hldr_first_name,
    loadtime_df.acct_hldr_last_name)

df11 = loadtime_df.join(concat_df, on=['acct_hldr_first_name', 'acct_hldr_last_name'], how='left_outer')
df12 = df11.drop("acct_hldr_first_name", "acct_hldr_last_name")

# filter out two first characters in dataframe and create a new column with values based on those characters
# then drop old account_id column and transition column

df13 = df12.withColumn("acct_id_type", df12.account_id.substr(0, 2))
df13 = df13.withColumn("account_type", when(df13.acct_id_type == "CK", "Checking")
                       .when(df13.acct_id_type == "PV", "Private")
                       .otherwise("Savings"))
final_df = df13.drop("account_id_type", "acct_id_type")

#switch around columns for final dataframe before uploading to s3
final_df = final_df.select("account_id",
                           "name",
                           "account_open_dt",
                           "account_type",
                           "acct_hldr_primary_addr_state",
                           "acct_hldr_primary_addr_zip_cd",
                           "dataset_date",
                           "load_time")

# create partitions and upload this file to the transformed_date s3 bucket
final_df.write.partitionBy("load_time", "dataset_date").mode("append").parquet("s3://cnastoski-pyspark/transform_data/question_1_parquet/")

#spark-submit s3://cnastoski-pyspark/scripts/class_question1/question_1_additional_columns.py