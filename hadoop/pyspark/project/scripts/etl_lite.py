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
    .master("local") \
    .appName("question1_enhanced") \
    .getOrCreate()


def extract_data(base_path: str, file_list: list):
    """
    opens one or more files from a directory and creates a dataframe from those files
    :param base_path: base directory where those files are stored
    :param file_names: list of files to turn into dataframes
    :return: the combined pyspark dataframe
    """
    file_paths = ",".join(base_path + "/" + file for file in file_list)
    relisted_files = file_paths.split(',')
    df = spark.read.option("header", True).csv(base_path).where(col('dataset_date').between('2022-01-12', '2022-01-13'))

    return df


def transform_data(df: DataFrame):
    # create another column to add in current load_time for every record
    time_zone = pytz.timezone('America/Detroit')
    now = datetime.now(time_zone)
    current_time = now.strftime("%Y-%m-%d")
    loadtime_df = df.withColumn("load_time", lit(current_time))

    # add a column with first and last name concatenated and drop first and last name columns
    concat_df = loadtime_df.withColumn("name",
                                       concat_ws(" ", loadtime_df.acct_hldr_first_name, loadtime_df.acct_hldr_last_name)
                                       )
    trimmed_df = concat_df.drop("acct_hldr_first_name", "acct_hldr_last_name")

    # filter out two first characters in dataframe and create a new column with values based on those characters
    # then drop old account_id column and transition column

    transition_df = trimmed_df.withColumn("acct_id_type", trimmed_df.account_id.substr(0, 2))
    transition_df = transition_df.withColumn("account_type", when(transition_df.acct_id_type == "CK", "Checking")
                                             .when(transition_df.acct_id_type == "PV", "Private")
                                             .otherwise("Savings"))
    final_df = transition_df.drop("account_id_type", "acct_id_type")

    # switch around columns for final dataframe before uploading to s3
    final_df = final_df.select("account_id",
                               "name",
                               "account_open_dt",
                               "account_type",
                               "acct_hldr_primary_addr_state",
                               "acct_hldr_primary_addr_zip_cd",
                               "dataset_date",
                               "load_time")

    return final_df


def load_data(dataframe: DataFrame, filepath: str, partitions, mode, file_format):
    if file_format == 'parquet':
        if not partitions:
            dataframe.write.option('header', True).mode(mode).parquet(filepath)
        else:
            dataframe.write.partitionBy("load_time", "dataset_date").mode(mode).parquet(filepath)
    if file_format == 'csv':
        if not partitions:
            dataframe.write.mode(mode).csv(filepath)
        else:
            dataframe.write.option('header', True).partitionBy("load_time", "dataset_date").mode(mode).csv(filepath)

    return 0


def main():
    parser = argparse.ArgumentParser(description='Read and write files to S3 using pyspark')

    parser.add_argument('-dir', metavar='base_directory', help='file path where your data files for reading are stored',
                        type=str, required=True)

    parser.add_argument('-files', metavar='file_names',
                        help='list of file names that you want to be read into a dataframe',
                        required=True,
                        nargs='+',
                        type=str)

    parser.add_argument('-dest', metavar='destination_path',
                        help='destination path to where you want to store the finished data',
                        type=str, required=True)

    parser.add_argument('-m', metavar='mode',
                        help='mode of inserting data into the destination, Default=append. others: overwrite,ignore,'
                             'error',
                        type=str,
                        default='append')

    parser.add_argument('-format', metavar='file_format',
                        help='arguments: csv or parquet. Default=csv. Choose the file format in which to write the '
                             'pyspark data',
                        type=str,
                        default='csv')

    parser.add_argument('-p', metavar='partitions',
                        help='Choose whether to create the data using partitions or not. Default=True',
                        type=bool,
                        default=True)

    args = parser.parse_args()

    dataframe = extract_data(args.dir, args.files)
    final_df = transform_data(dataframe)
    load_data(final_df, args.dest, partitions=args.p, mode=args.m, file_format=args.format)


if __name__ == "__main__":
    main()

