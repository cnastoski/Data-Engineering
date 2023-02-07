use cards_dw;


select make, num_registered,
rank() over(order by num_registered desc) as num_registered_rank
from (select make, count(*) as num_registered from ev_vehicle_info group by make) subq
limit ${NUM_LIMIT};

--hive -hivevar NUM_LIMIT='5' -f 's3://cnastoski-clibucket-ohio/sql_scripts/cards_dw/ev_vehicle_info/num_registered_rank.sql'
