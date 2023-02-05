use cards_dw;


set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;


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
WHERE dataset_date='${CURRENT_DATE}';


-- hive -hivevar CURRENT_DATE='2022-02-07' -f 's3://cnastoski-clibucket-ohio/sql_scripts/insert_transaction_fact.sql'

