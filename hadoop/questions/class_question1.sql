1.Create table :tran_fact
tran_id int, cust_id varchar(20),tran_date date,tran_ammount decimal(10,2), tran_type varchar(1)

102020,CA1001,2022-02-01,1200,C
102021,CA1002,2022-02-01,700,C
102022,CA1003,2022-02-01,500,C
102023,CA1004,2022-02-02,900,C
102020,CA1001,2022-02-02,200,D
102029,CA1001,2022-02-02,700,C
102024,CA1005,2022-02-03,12200,C
102025,CA1003,2022-02-03,200,D
102026,CA1004,2022-02-04,12200,C
102027,CA1007,2022-02-04,9200,C
102028,CA1007,2022-02-04,3200,D

1. Total unique customer per day.

    select count(distinct cust_id) as unique_customers, tran_date
    from hadoop.tran_fact
    group by tran_date

2. Total number of unique customer till date

    select count(distinct cust_id) as unique_customers
    from hadoop.tran_fact

3. Total transaction amount per customer per day ( if its C then add if D then subtract )

    select tran_fact.cust_id, sum(updated_tran) as daily_transaction, tran_fact.tran_date
    from hadoop.tran_fact
    join(select cust_id, tran_date,
    case
        when tran_type = 'D' then -tran_amount
        else tran_amount
    end as updated_tran
    from hadoop.tran_fact) trans
    on trans.cust_id = tran_fact.cust_id
    group by tran_fact.cust_id, tran_fact.tran_date
    order by tran_fact.tran_date

4. Find out duplicate transaction in total.

    SELECT tran_id, cust_id, tran_date, tran_amount, tran_type
    FROM hadoop.tran_fact
    GROUP BY tran_id, cust_id, tran_date, tran_amount, tran_type
    HAVING COUNT(*) > 1


5. show the transaction which has debit but never credit before.

    SELECT * FROM hadoop.tran_fact
    WHERE tran_type = 'D' AND cust_id NOT IN (SELECT cust_id FROM hadoop.tran_fact WHERE tran_type = 'C')