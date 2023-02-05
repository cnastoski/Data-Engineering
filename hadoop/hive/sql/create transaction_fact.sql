CREATE DATABASE cards_dw
location 's3://cnastoski-clibucket-ohio/cards_dw'; --creating a new folder in s3 bucket for this database


use cards_dw;


CREATE TABLE IF NOT EXISTS transaction_fact(
    tran_id int,
    cust_id varchar(20),
    tran_date date,
    tran_ammount decimal(10,2),
    tran_type varchar(1),
    load_time TIMESTAMP
)
PARTITIONED BY (dataset_date varchar(10), state_cd varchar(2))
STORED AS parquet
TBLPROPERTIES ("parquet.compression"="SNAPPY");


INSERT INTO transaction_fact PARTITION(dataset_date,state_cd)
SELECT
    tran_id,
    cust_id,
    tran_date,
    tran_ammount,
    tran_type,
    `current_timestamp`(),
    dataset_date,state_cd
FROM src_customer.tran_fact
WHERE dataset_date='2022-02-03'