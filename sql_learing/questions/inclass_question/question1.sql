create table cards_ingest.tran_fact(
    tran_id int,
    cust_id varchar(10),
    stat_cd varchar(2),
    tran_ammt decimal(10,2),
    tran_date date
)

truncate table cards_ingest.tran_fact;
INSERT INTO cards_ingest.tran_fact
	(tran_id, cust_id, stat_cd, tran_ammt, tran_date) VALUES
	(102020, 'cust_101', 'NY', 125,to_date('2022-01-01','yyy-mm-dd')),
	(102021, 'cust_101', 'NY', 5125,to_date('2022-01-01','yyy-mm-dd')),
    (102022, 'cust_102', 'CA', 6125,to_date('2022-01-01','yyy-mm-dd')),
    (102023, 'cust_103', 'CA', 7145,to_date('2022-01-01','yyy-mm-dd')),
    (102024, 'cust_104', 'TX', 1023,to_date('2022-01-01','yyy-mm-dd')),
    (102025, 'cust_101', 'NY', 670,to_date('2022-01-03','yyy-mm-dd')),
	(102026, 'cust_101', 'NY', 5235,to_date('2022-01-03','yyy-mm-dd')),
    (102027, 'cust_102', 'CA', 61255,to_date('2022-01-04','yyy-mm-dd')),
    (102028, 'cust_103', 'CA', 7345,to_date('2022-01-04','yyy-mm-dd')),
    (102029, 'cust_104', 'TX', 1023,to_date('2022-01-05','yyy-mm-dd')),
    (102030, 'cust_109', NULL, 1023,to_date('2022-01-05','yyy-mm-dd')),
    (102031, 'cust_104',Null, 1023,to_date('2022-01-05','yyy-mm-dd')),
    (102031, 'cust_107',TX, 1023,to_date('2022-01-05','yyy-mm-dd')),
    (102031, 'cust_107',CA, 1023,to_date('2022-02-05','yyy-mm-dd'));


drop table cards_ingest.cust_dim_details;
create table cards_ingest.cust_dim_details (
    cust_id varchar(10),
    state_cd varchar(2),
    zip_cd varchar(5),
    cust_first_name  varchar(20),
    cust_last_name  varchar(20),
    start_date date,
    end_date date,
    active_flag varchar(1)
);
truncate table cards_ingest.cust_dim_details;

insert into cards_ingest.cust_dim_details
(cust_id,state_cd,zip_cd , cust_first_name, cust_last_name, start_date,end_date,active_flag)
VALUES
('cust_101','NY','08922', 'Mike', 'doge',to_date('2022-01-01','yyyy-mm-dd'),to_date('2029-01-01','yyyy-mm-dd'),'Y'),
('cust_102','CA','04922', 'sean', 'lan',to_date('2022-01-01','yyyy-mm-dd'),to_date('2029-01-01','yyyy-mm-dd'),'Y'),
('cust_103','CA','05922', 'sachin', 'ram',to_date('2022-01-01','yyyy-mm-dd'),to_date('2029-01-01','yyyy-mm-dd'),'Y'),
('cust_104','TX','08922', 'bill', 'kja',to_date('2022-01-01','yyyy-mm-dd'),to_date('2029-01-01','yyyy-mm-dd'),'Y'),
('cust_105','CA','08922', 'Douge', 'lilly',to_date('2022-01-01','yyyy-mm-dd'),to_date('2029-01-01','yyyy-mm-dd'),'Y'),
('cust_106','CA','08922', 'hence', 'crow',to_date('2022-01-01','yyyy-mm-dd'),to_date('2029-01-01','yyyy-mm-dd'),'Y'),
('cust_107','TX','08922', 'Mike', 'doge',to_date('2022-01-01','yyyy-mm-dd'),to_date('2029-02-01','yyyy-mm-dd'),'Y'),
('cust_107','NY','08922', 'Mike', 'doge',to_date('2022-02-03','yyyy-mm-dd'),to_date('2022-01-01','yyyy-mm-dd'),'N');

1. Calculate total tran_ammt (sum) for each state

    Select stat_cd, sum(tran_fact.tran_ammt) as "state sum"
    from cards_ingest.tran_fact
    group by stat_cd

2. Calculate maximum and minimum tran_ammt on each state and tran_date

    select stat_cd,tran_date max(tran_fact.tran_ammt) as "state Max", min(tran_ammt) as "state Min"
	from cards_ingest.tran_fact
    group by stat_cd, tran_date



3. Calculate total transaction which have tran_ammt more than 10000

    select sum(tran_fact.tran_ammt) as "transaction sum"
    from cards_ingest.tran_fact
    where tran_fact.tran_ammt > 10000

4. Show the state which have total (sum) tran_ammt more than 10000

    select stat_cd, sum(tran_fact.tran_ammt) as tran_sum
    from cards_ingest.tran_fact
	Group by stat_cd
    having sum(tran_fact.tran_ammt) > 10000

5. show me the states where total ammt is more than 10000

    select stat_cd, sum(tran_fact.tran_ammt) as tran_sum
    from cards_ingest.tran_fact
	Group by stat_cd
    having sum(tran_fact.tran_ammt) > 10000

6. show me the states where cust_id ='cust_104' and  total ammt is more than 10000

    Select tran_fact.cust_id, stat_cd, sum(tran_fact.tran_ammt) as tran_sum
    from cards_ingest.tran_fact
    where cust_id = 'cust_104'
    group by stat_cd, tran_fact.cust_id
    having sum(tran_fact.tran_ammt) > 10000


7. Calculate total transaction by state [ if state if NULL make it TX] where total transaction is more than 10000

    select coalesce(stat_cd, 'TX'), sum(tran_fact.tran_ammt) as tran_sum
    from cards_ingest.tran_fact
    group by stat_cd
    having sum(tran_fact.tran_ammt) > 10000
 


8. Show me a message col if state is null then "missing data" else "good data"

    select stat_cd,
    case 
    	when stat_cd is NULL then 'missing data'
    	else 'good data'
	end
	from cards_ingest.tran_fact


9. Show me sum of tran_ammt by state [ if state is null and cust_id='cust_104' then 'TX' else 'CA']

    select stat_cd, cust_id, sum(tran_fact.tran_ammt) as tran_sum,
    case
        when stat_cd is null and cust_id = 'cust_104' then stat_cd = 'TX'
        else stat_cd = 'CA'
    end
    from cards_ingest.tran_fact
    group by stat_cd, cust_id
    

Join Question:

1.Give me all details from transaction table and zip_cd from dimension table.

    select * , cards_ingest.cust_dim_details.zip_cd
    from cards_ingest.tran_fact
    inner join cards_ingest.cust_dim_details on cards_ingest.cust_dim_details.cust_id = cards_ingest.tran_fact.cust_id

2. Sum of tran_ammt by zip_cd

    select sum(tran_fact.tran_ammt) as tran_sum , cust_dim_details.zip_cd
    from cards_ingest.tran_fact
    inner join cards_ingest.cust_dim_details on cust_dim_details.cust_id = tran_fact.cust_id
    group by zip_cd








3. Give me top 5 customer [ (first name+ last name) is customer] by tran_ammt [highest is first] join on cust_id

    select cust_dim_details.cust_first_name, cust_dim_details.cust_last_name, tran_fact.tran_ammt
    from cards_ingest.tran_fact
    join cards_ingest.cust_dim_details on cust_dim_details.cust_id = tran_fact.cust_id
	where active_flag = 'Y'
    order by tran_ammt desc
    limit 5

4. Give me the all cols from tran_fact [ I dont need state_cd is null] first five records [ lower to highest]

    select * 
    from tran_fact
    where stat_cd is not NULL
    order by tran_ammt
    limit 5


