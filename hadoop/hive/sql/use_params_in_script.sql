hive -f filename.hql
beeline -f file.hql --hivevar HDFSDIR=/tmp/folder
USE myhivedb;
-- a comment
LOAD DATA INPATH '${HDFSDIR}/browser.tsv' OVERWRITE INTO TABLE browser;
-- other queries

hive -e " show database"

ssh -i sanjeeb_ec2.pem  hadoop@ec2-44-211-208-21.compute-1.amazonaws.com
ds_date='2022-01-03'
beeline -u jdbc:hive2://ec2-44-211-208-21.compute-1.amazonaws.com:10000/default --hivevar dataset_date=${ds_date} -e\
 "select count(1),dataset_date from src_customer.customer_details_partition_dynamic where dataset_date='${ds_date}' group by dataset_date "

ds_date='2022-01-02'
schema_name='src_customer'
beeline -u jdbc:hive2://ec2-44-211-208-21.compute-1.amazonaws.com:10000/default --hivevar dataset_date=${ds_date}   -f abc.hql

abc.sql
select count(1),dataset_date from src_customer.customer_details_partition_dynamic where dataset_date='${ds_date}' group by dataset_date



beeline -u "${dbconection}" --hivevar load_id=${loadid} -f /etc/sql/hive_script.sql
INSERT into TABLE table-b
SELECT column1,
Column2,
Column3,
${loadid} as load_id,
Column5
From table-a;