SANDBOX.CLEAN_SCH.RESTAURANT_STMuse role sysadmin;
use warehouse compute_wh;
use database sandbox;
use schema stage_sch;

-- level 1 
create or replace table stage_sch.restaurant (
    restaurantid text,      
    name text ,                                         -- restaurant name, required field
    cuisinetype text,                                    -- type of cuisine offered
    pricing_for_2 text,                                  -- pricing for two people as text
    restaurant_phone text WITH TAG (common.pii_policy_tag = 'SENSITIVE'),                               -- phone number as text
    operatinghours text,                                 -- restaurant operating hours
    locationid text ,                                    -- location id, default as text
    activeflag text ,                                    -- active status
    openstatus text ,                                    -- open status
    locality text,                                       -- locality as text
    restaurant_address text,                             -- address as text
    latitude text,                                       -- latitude as text for precision
    longitude text,                                      -- longitude as text for precision
    createddate text,                                    -- record creation date
    modifieddate text,                                   -- last modified date
    -- audit columns for debugging
    _stg_file_name text,
    _stg_file_load_ts timestamp,
    _stg_file_md5 text,
    _copy_data_ts timestamp default current_timestamp
)
comment='This is the restaurant stage/raw table where data will be copied from internal stage using copy command. This is as-is data represetation from the source restaurant. All the columns are text data type except the audit columns that are added for traceability.';


create or replace stream stage_sch.restaurant_stm
on table stage_sch.restaurant
append_only=True 
comment='This is the append-only steam object on restaurant table that gets delta data based on chnages. It will track insert, update or delelte in the restaurant data ';

select * from stage_sch.restaurant;

CREATE OR REPLACE PIPE stage_sch.restaurant_pipes
AUTO_INGEST = TRUE
INTEGRATION = Azure_Notification
AS 
copy into stage_sch.restaurant (restaurantid, name, cuisinetype, pricing_for_2, restaurant_phone, 
                      operatinghours, locationid, activeflag, openstatus, 
                      locality, restaurant_address, latitude, longitude, 
                      createddate, modifieddate, 
                      _stg_file_name, _stg_file_load_ts, _stg_file_md5, _copy_data_ts)
from (
        select 
        t.$1::text as restaurantid,        -- restaurantid as the first column
        t.$2::text as name,
        t.$3::text as cuisinetype,
        t.$4::text as pricing_for_2,
        t.$5::text as restaurant_phone,
        t.$6::text as operatinghours,
        t.$7::text as locationid,
        t.$8::text as activeflag,
        t.$9::text as openstatus,
        t.$10::text as locality,
        t.$11::text as restaurant_address,
        t.$12::text as latitude,
        t.$13::text as longitude,
        t.$14::text as createddate,
        t.$15::text as modifieddate,
        -- audit columns for tracking & debugging
        metadata$filename as _stg_file_name,
        metadata$file_last_modified as _stg_file_load_ts,
        metadata$file_content_key as _stg_file_md5,
        current_timestamp() as _copy_data_ts

        from @azure_external_stage/restaurants t  
)
file_format=(format_name='stage_sch.csv_file_format')
ON_ERROR = 'CONTINUE'
PATTERN = '.*\.csv';

SHOW PIPES LIKE 'restaurant_pipe';

SHOW PIPES LIKE 'restaurant_pipes' IN SCHEMA stage_sch;
--using the next schema that is clean schema level 2 


create or replace table clean_sch.restaurant (
    restaurant_sk number autoincrement primary key,              -- primary key with auto-increment
    restaurant_id number unique,                                        -- restaurant id without auto-increment
    name string(100) not null,                                   -- restaurant name, required field
    cuisine_type string,                                         -- type of cuisine offered
    pricing_for_two number(10, 2),                               -- pricing for two people, up to 10 digits with 2 decimal places
    restaurant_phone string(15) WITH TAG (common.pii_policy_tag = 'SENSITIVE'),                                 -- phone number, supports 10-digit or international format
    operating_hours string(100),                                  -- restaurant operating hours
    location_id_fk number,                                       -- reference id for location, defaulted to 1
    active_flag string(10),                                      -- indicates if the restaurant is active
    open_status string(10),                                      -- indicates if the restaurant is currently open
    locality string(100),                                        -- locality of the restaurant
    restaurant_address string,                                   -- address of the restaurant, supports longer text
    latitude number(9, 6),                                       -- latitude with 6 decimal places for precision
    longitude number(9, 6),                                      -- longitude with 6 decimal places for precision
    created_dt timestamp_tz,                                     -- record creation date
    modified_dt timestamp_tz,                                    -- last modified date, allows null if not modified

    -- additional audit columns
    _stg_file_name string,                                       -- file name for audit
    _stg_file_load_ts timestamp_ntz,                             -- file load timestamp for audit
    _stg_file_md5 string,                                        -- md5 hash for file content for audit
    _copy_data_ts timestamp_ntz default current_timestamp        -- timestamp when data is copied, defaults to current timestamp
)
comment = 'Restaurant entity under clean schema with appropriate data type under clean schema layer, data is populated using merge statement from the stage layer location table. This table does not support SCD2'


create or replace stream clean_sch.restaurant_stm
on table clean_sch.restaurant
comment='This is a standard stream object on the clean restaurant table to track insert, update, and delete changes';
SHOW TASKS LIKE 'restauran_task2' IN SCHEMA clean_sch;

/*SELECT *
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START => DATEADD('hour', -24, CURRENT_TIMESTAMP()),  -- last 24 hours
    SCHEDULED_TIME_RANGE_END => CURRENT_TIMESTAMP(),
    RESULT_LIMIT => 100
))
WHERE SCHEMA_NAME IN ('CLEAN_SCH','CONSUMPTION_SCH')
ORDER BY SCHEDULED_TIME DESC;*/

select * from clean_sch.restaurant_stm;

SELECT *
FROM SNOWFLAKE.ACCOUNT_USAGE.TASK_HISTORY
WHERE NAME = 'CLEAN_SCH.restauran_task2'
ORDER BY SCHEDULED_TIME DESC
LIMIT 10;

select * from clean_sch.restaurant_location


CREATE OR REPLACE TASK clean_sch.restauran_task4
WAREHOUSE = compute_wh
SCHEDULE = 'USING CRON 30 20 * * * UTC' -- event-driven
AS
merge into clean_sch.restaurant as target
using (
            select 
            try_cast(restaurantid  as number) as restaurant_id,
            try_cast(name as string) as name,
            try_cast(cuisinetype as string ) as cuisine_type,
            try_cast(pricing_for_2 as number(10,2)) as pricing_for_two,
            try_cast(restaurant_phone as string) as restaurant_phone,
            try_cast(operatinghours  as string) as operating_hours,
            try_cast(locationid  as number) as location_id_fk,
            try_cast(activeflag  as string) as active_flag,
            try_cast(openstatus as string) as open_status,
            try_cast(locality  as string) as locality,
            try_cast(restaurant_address as string) as restaurant_address,
            try_cast(latitude as number(9,6)) as latitude,
            try_cast(longitude as number(9,6)) as longitude,
            TO_CHAR(COALESCE(TRY_TO_TIMESTAMP(createddate, 'MM/DD/YYYY HH24:MI'),TRY_TO_TIMESTAMP(createddate, 'MM/DD/YYYY')),'YYYY-MM-DD HH24:MI:SS.FF9') AS created_dt,
            TO_CHAR(COALESCE(TRY_TO_TIMESTAMP(modifieddate, 'MM/DD/YYYY HH24:MI'),TRY_TO_TIMESTAMP(createddate, 'MM/DD/YYYY')),'YYYY-MM-DD HH24:MI:SS.FF9') AS modified_dt,
            _stg_file_name,
            _stg_file_load_ts,
            _stg_file_md5,
            _copy_data_ts
            from stage_sch.restaurant_stm
            where restaurant_id is not null 
) as source
on target.restaurant_id=source.restaurant_id
when matched and  (
        target.name != source.name or
        target.cuisine_type != source.cuisine_type or
        target.pricing_for_two != source.pricing_for_two or
        target.restaurant_phone != source.restaurant_phone or
        target.operating_hours != source.operating_hours or
        target.location_id_fk != source.location_id_fk or
        target.active_flag != source.active_flag or
        target.open_status != source.open_status or
        target.locality != source.locality or
        target.restaurant_address != source.restaurant_address or
        target.latitude != source.latitude or
        target.longitude != source.longitude or
        target.created_dt != source.created_dt or
        target.modified_dt != source.modified_dt
) then 
    update set 
        target.name = source.name,
        target.cuisine_type = source.cuisine_type,
        target.pricing_for_two = source.pricing_for_two,
        target.restaurant_phone = source.restaurant_phone,
        target.operating_hours = source.operating_hours,
        target.location_id_fk = source.location_id_fk,
        target.active_flag = source.active_flag,
        target.open_status = source.open_status,
        target.locality = source.locality,
        target.restaurant_address = source.restaurant_address,
        target.latitude = source.latitude,
        target.longitude = source.longitude,
        target.created_dt = source.created_dt,
        target.modified_dt = source.modified_dt,
        target._stg_file_name = source._stg_file_name,
        target._stg_file_load_ts = source._stg_file_load_ts,
        target._stg_file_md5 = source._stg_file_md5,
        target._copy_data_ts = source._copy_data_ts
when not matched then 
    insert(
        restaurant_id,
        name,
        cuisine_type,
        pricing_for_two,
        restaurant_phone,
        operating_hours,
        location_id_fk,
        active_flag,
        open_status,
        locality,
        restaurant_address,
        latitude,
        longitude,
        created_dt,
        modified_dt,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5,
        _copy_data_ts
    )
    values(
        source.restaurant_id,
        source.name,
        source.cuisine_type,
        source.pricing_for_two,
        source.restaurant_phone,
        source.operating_hours,
        source.location_id_fk,
        source.active_flag,
        source.open_status,
        source.locality,
        source.restaurant_address,
        source.latitude,
        source.longitude,
        source.created_dt,
        source.modified_dt,
        source._stg_file_name,
        source._stg_file_load_ts,
        source._stg_file_md5,
        source._copy_data_ts
    );

ALTER TASK clean_sch.restauran_task4 RESUME;
 
SHOW TASKS LIKE 'restauran_task4' IN SCHEMA clean_sch;
SHOW TASKS LIKE 'restaurant_scd2_task4' IN SCHEMA consumption_sch;

LOCATION_SCD2_TASK, LOCATION_SCD2_TASK2


SELECT *
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    TASK_NAME => 'clean_sch.restauran_task4',
    RESULT_LIMIT => 10
));

EXECUTE TASK clean_sch.restauran_task4;

SHOW TASKS IN SCHEMA clean_sch;
SHOW TASKS IN SCHEMA consumption_sch;
select * from clean_sch.restaurant
-- level 3

select * from consumption_sch.restaurant_dim;


Create or replace table consumption_sch.restaurant_dim(
            restaurant_hk number primary key,
            restaurant_id number,
            name string(100),
            cuisine_type string,
            pricing_for_two number(10,2),
            restaurant_phone string(15) with tag (common.pii_policy_tag ='SENSITIVE'),
            operating_hours string(100),
            location_id_fk number,
            active_flag string(10),
            open_status string(10),
            locality string(100),
            restaurant_address string,
            latitude number(9, 6),
            longitude number(9, 6),
            Eff_start_date timestamp_tz,
            Eff_End_date timestamp_tz,
            current_flage boolean
        
)comment =' Restaurant dimensional table with scd enabled';

select * from consumption_sch.restaurant_dim;



CREATE OR REPLACE TASK consumption_sch.restaurant_scd2_task4
WAREHOUSE = compute_wh
SCHEDULE  = 'USING CRON 3 21 * * * UTC'
AS
merge into 
    consumption_sch.restaurant_dim as target
using 
    clean_sch.restaurant_stm as source
on 
    target.restaurant_id = source.restaurant_id and 
    target.name = source.name and 
    target.cuisine_type = source.cuisine_type and 
    target.pricing_for_two = source.pricing_for_two and 
    target.restaurant_phone = source.restaurant_phone and 
    target.operating_hours = source.operating_hours and 
    target.location_id_fk = source.location_id_fk and 
    target.active_flag = source.active_flag and 
    target.open_status = source.open_status and 
    target.locality = source.locality and 
    target.restaurant_address = source.restaurant_address and 
    target.latitude = source.latitude and 
    target.longitude = source.longitude
when matched 
    and source.METADATA$ACTION = 'DELETE' and source.METADATA$ISUPDATE = 'TRUE' then
    -- Update the existing record to close its validity period
    UPDATE SET 
        target.EFF_END_DATE = CURRENT_TIMESTAMP(),
        target.current_flage = FALSE
when not matched 
    and source.METADATA$ACTION = 'INSERT' and source.METADATA$ISUPDATE = 'TRUE' then
    -- Insert new record with current data and new effective start date
    INSERT (
            restaurant_hk,
            restaurant_id ,
            name,
            cuisine_type ,
            pricing_for_two,
            restaurant_phone,
            operating_hours ,
            location_id_fk ,
            active_flag,
            open_status,
            locality,
            restaurant_address,
            latitude,
            longitude,
            Eff_start_date,
            Eff_End_date,
            current_flage
    )
    values (
        hash(SHA1_hex(CONCAT(source.RESTAURANT_ID, source.NAME, source.CUISINE_TYPE, 
            source.PRICING_FOR_TWO, source.RESTAURANT_PHONE, source.OPERATING_HOURS, 
            source.LOCATION_ID_FK, source.ACTIVE_FLAG, source.OPEN_STATUS, source.LOCALITY, 
            source.RESTAURANT_ADDRESS, source.LATITUDE, source.LONGITUDE))),
        source.RESTAURANT_ID,
        source.NAME,
        source.CUISINE_TYPE,
        source.PRICING_FOR_TWO,
        source.RESTAURANT_PHONE,
        source.OPERATING_HOURS,
        source.LOCATION_ID_FK,
        source.ACTIVE_FLAG,
        source.OPEN_STATUS,
        source.LOCALITY,
        source.RESTAURANT_ADDRESS,
        source.LATITUDE,
        source.LONGITUDE,
        CURRENT_TIMESTAMP(),
        NULL,
        TRUE
    )
when not matched 
    and source.METADATA$ACTION = 'INSERT' and source.METADATA$ISUPDATE = 'FALSE' then
    -- Insert new record with current data and new effective start date
    INSERT (
        restaurant_hk,
            restaurant_id ,
            name,
            cuisine_type ,
            pricing_for_two,
            restaurant_phone,
            operating_hours ,
            location_id_fk ,
            active_flag,
            open_status,
            locality,
            restaurant_address,
            latitude,
            longitude,
            Eff_start_date,
            Eff_End_date,
            current_flage
    )
    values (
        hash(SHA1_hex(CONCAT(source.RESTAURANT_ID, source.NAME, source.CUISINE_TYPE, 
            source.PRICING_FOR_TWO, source.RESTAURANT_PHONE, source.OPERATING_HOURS, 
            source.LOCATION_ID_FK, source.ACTIVE_FLAG, source.OPEN_STATUS, source.LOCALITY, 
            source.RESTAURANT_ADDRESS, source.LATITUDE, source.LONGITUDE))),
        source.RESTAURANT_ID,
        source.NAME,
        source.CUISINE_TYPE,
        source.PRICING_FOR_TWO,
        source.RESTAURANT_PHONE,
        source.OPERATING_HOURS,
        source.LOCATION_ID_FK,
        source.ACTIVE_FLAG,
        source.OPEN_STATUS,
        source.LOCALITY,
        source.RESTAURANT_ADDRESS,
        source.LATITUDE,
        source.LONGITUDE,
        CURRENT_TIMESTAMP(),
        NULL,
        TRUE
    );

EXECUTE TASK consumption_sch.restaurant_scd2_task4;

ALTER TASK consumption_sch.restaurant_scd2_task4 RESUME;

select * from clean_sch.restaurant_stm

select * from consumption_sch.restaurant_dim;

list @stage_sch.cvs_stg/delta/restaurant;



copy into stage_sch.restaurant (restaurantid, name, cuisinetype, pricing_for_2, restaurant_phone, 
                      operatinghours, locationid, activeflag, openstatus, 
                      locality, restaurant_address, latitude, longitude, 
                      createddate, modifieddate, 
                      _stg_file_name, _stg_file_load_ts, _stg_file_md5, _copy_data_ts)
from (
        select 
        t.$1::text as restaurantid,        -- restaurantid as the first column
        t.$2::text as name,
        t.$3::text as cuisinetype,
        t.$4::text as pricing_for_2,
        t.$5::text as restaurant_phone,
        t.$6::text as operatinghours,
        t.$7::text as locationid,
        t.$8::text as activeflag,
        t.$9::text as openstatus,
        t.$10::text as locality,
        t.$11::text as restaurant_address,
        t.$12::text as latitude,
        t.$13::text as longitude,
        t.$14::text as createddate,
        t.$15::text as modifieddate,
        -- audit columns for tracking & debugging
        metadata$filename as _stg_file_name,
        metadata$file_last_modified as _stg_file_load_ts,
        metadata$file_content_key as _stg_file_md5,
        current_timestamp() as _copy_data_ts

        from @stage_sch.cvs_stg/delta/restaurant/day-02-upsert-restaurant-delhi+NCR.csv t
)
file_format=(format_name='stage_sch.csv_file_format')
on_error = abort_statement;


TRUNCATE table stage_sch.restaurant_stm
;
select * from stage_sch.restaurant_stm
select * from consumption_sch.restaurant_dim;