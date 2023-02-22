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

spark = SparkSession.builder\
    .master('local')\
    .appName("etl_lite")\
    .enableHiveSupport()\
    .getOrCreate()

df1 = spark.sql("""select * from pyspark.question_1 limit 10""")
df1.show()
