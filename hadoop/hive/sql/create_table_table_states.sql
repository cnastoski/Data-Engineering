CREATE TABLE IF NOT EXISTS table_states(
    database_name varchar(20),
    table_name varchar(50),
    partition_key varchar(30),
    rec_count int,
    load_date date)
PARTITIONED BY (execution_key varchar(100))
STORED AS parquet
TBLPROPERTIES ("parquet.compression"="SNAPPY");


INSERT INTO table_states partition(execution_key)
SELECT 'src_customer','tran_fact',dataset_date,count(*),CURRENT_DATE,
       concat('src_customer','-','tran_fact','-',dataset_date) as execution_key
FROM tran_fact
WHERE dataset_date='2022-02-09'
group by dataset_date;