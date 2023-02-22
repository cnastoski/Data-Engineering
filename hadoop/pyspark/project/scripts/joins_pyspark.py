import pyspark
from pyspark import SparkContext as sc
import os
from pyspark.sql import SparkSession
from functools import reduce
from pyspark.sql import DataFrame
from pyspark.sql.functions import concat_ws, lit, to_date, when, col
import pytz
from datetime import datetime
import sys
import argparse

spark = SparkSession.builder \
    .master('local') \
    .appName("joins_pyspark") \
    .enableHiveSupport() \
    .getOrCreate()

df1 = spark.sql(""" select count(account_id) as accounts_opened, account_open_dt from pyspark.question_1_parquet 
                    where account_open_dt >= '2022-01-01' and account_open_dt <= '2022-01-07' 
                    group by account_open_dt """)

df2 = spark.sql(""" select count(account_id) as accounts_opened, account_type from pyspark.question_1_parquet
                    where account_open_dt >= '2022-01-01' and account_open_dt <= '2022-01-07' group by account_type""")




