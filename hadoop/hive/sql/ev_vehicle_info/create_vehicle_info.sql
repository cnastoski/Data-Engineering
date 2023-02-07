use cards_dw;

--create table that we can pull data from a csv in s3
create external table ev_vehicle_info_src(
    vin varchar(10),
    county varchar(50),
    city varchar(50),
    state varchar(2),
    postal_code varchar(5),
    model_year smallint,
    make varchar(15),
    model string,
    electric_vehicle_type string,
    cafv_eligibility varchar(50),
    electric_range smallint,
    base_msrp double precision,
    legislative_district smallint,
    DOL_vehicle_id varchar(10),
    vehicle_location string,
    electric_utility string,
    2020_census_tract varchar(15)
)
row format delimited fields terminated by ','
location 's3://cnastoski-clibucket-ohio/cards_dw/ev_vehicle_info_src'
tblproperties ("skip.header.line.count"="1");

--create parquet table that we move our data into from the original

create table ev_vehicle_info(
    vin varchar(10),
    county varchar(50),
    city varchar(50),
    state varchar(2),
    postal_code varchar(5),
    model_year smallint,
    model string,
    electric_vehicle_type string,
    cafv_eligibility varchar(50),
    electric_range smallint,
    base_msrp double precision,
    legislative_district smallint,
    DOL_vehicle_id varchar(10),
    vehicle_location string,
    electric_utility string,
    2020_census_tract varchar(15)
)
PARTITIONED BY (make varchar(15))
STORED AS parquet
TBLPROPERTIES ("parquet.compression"="SNAPPY");

--insert data from src table to parquet table

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE ev_vehicle_info partition(make)
select
vin,
county,
city,
state,
postal_code,
model_year,
model,
electric_vehicle_type,
cafv_eligibility,
electric_range,
base_msrp,
legislative_district,
DOL_vehicle_id,
vehicle_location,
electric_utility,
2020_census_tract,
make
FROM ev_vehicle_info_src;