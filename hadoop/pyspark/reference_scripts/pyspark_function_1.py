
import random
from random import randint
import string
from pyspark.sql.types import *
import pyspark.sql.functions as psf
from datetime import datetime, timedelta
from pyspark.sql import SparkSession


def getData(filePath):
    df=spark.read.csv(filePath)
    countChk=df.count()
    if countChk ==0:
        print(" There is no data !! Source need to be Contacted")



# df.withColumn('age2', df.age + 2)
if __name__ == '__main__':
    spark = SparkSession.builder.master("local").appName("sourcedata").getOrCreate()
    src_bucket='aws-train-nov-de'
    schema='cards_ingest'
    table='account_src'
    s3DataPath="s3://"+src_bucket+'/'+schema+'/'+table+'/'
# s3://aws-train-nov-de/cards_ingest/account_src
    fileKey='cards_account_ingest_2022-01-02.csv'
    filePath=s3DataPath+fileKey
    getData(filePath)
    # spar