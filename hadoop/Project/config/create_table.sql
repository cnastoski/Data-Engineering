CREATE SCHEMA IF NOT EXISTS hadoop;


CREATE TABLE IF NOT EXISTS hadoop.tran_fact(
    tran_id int,
    cust_id varchar(30),
    tran_amount decimal(10,2),
    tran_type varchar(1),
    country_cd varchar(3),
    tran_date date
);