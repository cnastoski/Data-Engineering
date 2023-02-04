UNLOAD ( 'select sum(tran_amount), country_cd from hadoop.tran_fact GROUP BY country_cd' )
TO 's3://cnastoski-unloadbucket/parquet_unload/'
IAM_ROLE 'arn:aws:iam::432167795286:role/service-role/AmazonRedshift-CommandsAccessRole-20230110T085953'
FORMAT PARQUET
ALLOWOVERWRITE


CREATE TABLE table_states(database_name varchar(20),table_name varchar(50),partition_key varchar(30),rec_count int,load_date date,execution_key varchar(100))
PARTITIONED BY (execution_key varchar(100))
STORED AS parquet
TBLPROPERTIES ("parquet.compression"="SNAPPY")

