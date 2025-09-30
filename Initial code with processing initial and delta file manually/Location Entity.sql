use role sysadmin;
use warehouse compute_wh;
use database sandbox;
use schema stage_sch;

select 
    t.$1::text as locationid,
    t.$2::text as city,
    t.$3::text as state,
    t.$4::text as zipcode,
    t.$5::text as activeflag,
    t.$6::text as createddate,
    t.$7::text as modifieddate
    -- audit columns for tracking and debugging
    metadata$filename as _stg_file_name,
    metadata$file_last_modified as _stg_file_load_ts,
    metadata$file_content_key as _stg_file_md5,
    current_timestamp as _copy_data_ts
    
from @stage_sch.cvs_stg/initial/location (file_format => 'stage_sch.csv_file_format') t;

-- create table 

create table stage_sch.location (
    locationid text,
    city text,
    state text,
    zipcode text,
    activeflag text,
    createddate text,
    modifieddate text,
    -- audit columns for tracking and debugging
    _stg_file_name text,
    _stg_file_load_ts timestamp,
    _stg_file_md5 text,
    _copy_data_ts timestamp default current_timestamp  
)
comment='This is the location stage/raw table where data will be copied from internal stage using copy command. This is as-is data represetation from the source location. All the columns are text data type except the audit columns that are added for traceability.';



create or replace stream stage_sch.location_stm
on table stage_sch.location
append_only =true 
comment = 'This is the append-only steam object on location table that gets delta data based on chnages. It will track insert, update or delelte in the location data '


select * from stage_sch.location;
select * from stage_sch.location_stm
-- copy comment

copy into stage_sch.location (locationid, city, state, zipcode, activeflag, createddate,
                            modifieddate, _stg_file_name, _stg_file_load_ts, _stg_file_md5, _copy_data_ts)
from (
select 
    t.$1::text as locationid,
    t.$2::text as city,
    t.$3::text as state,
    t.$4::text as zipcode,
    t.$5::text as activeflag,
    t.$6::text as createddate,
    t.$7::text as modifieddate,
    -- audit columns for tracking and debugging
    metadata$filename as _stg_file_name,
    metadata$file_last_modified as _stg_file_load_ts,
    metadata$file_content_key as _stg_file_md5,
    current_timestamp as _copy_data_ts
    
from @stage_sch.cvs_stg/initial/location t
)
file_format = (format_name='stage_sch.csv_file_format')
on_error = abort_statement;

select * from stage_sch.location;
select * from stage_sch.location_stm;


--using the next schema that is clean schema level 2 

use schema clean_sch;

create or replace table clean_sch.restaurant_location(
        restaurant_location_sk number autoincrement primary key,
        location_id number  not null unique,
        city string(100) not null,
        state string(100) not null,
        state_code string(2) not null,
        is_union_territory boolean not null default false,
        capital_city_flag boolean not null default false,
        city_tier text(6),
        zip_code string(10) not null,
        active_flage string(10) not null,
        created_ts timestamp_tz not null,
        modified_ts timestamp_tz,
        --additional audit columns 

        _stg_file_name string,
        _stg_file_load_ts timestamp_ntz,
        _stg_file_md5 string,
        _copy_data_ts timestamp_ntz default current_timestamp
)
comment ='Location entity under clean schema with appropritte data type under clean schema layer, data is populated using merge statemnt from the the stage layer location table. This table does not support SCD2';



-- alter tabel becasue made a mistake in clean_sch.restaurant_location one column name 
ALTER TABLE clean_sch.restaurant_location
RENAME COLUMN active_flage TO active_flag;

create or replace stream clean_sch.restaurant_location_stm
on table clean_sch.restaurant_location
comment = ' this is standard stream object on the location table to track insert, delete and update changes';


Merge into clean_sch.restaurant_location as target 
using(
        select 
        Cast(LocationID as Number) as Location_ID,
        Cast(City as String) as City,
        case when cast(State as String) ='Delhi' then 'New Delhi'
        else cast(State as String)
        end as State,
        -- state code mapping
        Case 
                when State='Delhi' then 'DL'
                when State='Maharashtra' then 'MH'
                when State='Uttar Pradesh' then 'UP'
                when State='Gujarat' then 'GJ'
                when State='Rajasthan' then 'RJ'
                when State='Kerala' then 'KL'
                when State='Punjab' then 'PB'
                when State='Karnataka' then 'KA'
                when State='Madhya Pradesh' then 'MP'
                when State='Odisha' then 'OR'
                when State='Chandigarh' then 'CH'
                when State='West Bengal' then 'WB'
                when State='Sikkim' then 'SK'
                when State='Andhra Pradesh' then 'AP'
                when State='Assam' then 'AS'
                when State='Jammu and Kashmir' then 'JK'
                when State='Puducherry' then 'PY'
                when State='Uttarakhand' then 'UK'
                when State='Himachal Pradesh' then 'HP'
                when State='Tamil Nadu' then 'TN'
                when State='Goa' then 'GA'
                when State='Telangana' then 'TG'
                when State='Chhattisgarh' then 'CG'
                when State='Jharkhand' then 'JH'
                when State='Bihar' then 'BR'
                Else null
                end as state_code,
                case 
                    when State in('Delhi', 'Chandigarh', 'Puducherry', 'Jammu and Kashmir') then 'Y'
                    else 'N'
                    end as is_union_territory,
                case when (State='Delhi' or State='New Delhi' and City='Delhi') THEN True 
                when (State='Maharashtra' and City='Mumbai') Then True 
                else False 
                end as capital_city_flag,
                case when City in ('Mumbai','Delhi', 'Bengaluru', 'Hyderabad', 'Chennai', 'Kolkata', 'Pune', 'Ahmedabad') then 'Tier-1'
                when City in ('Jaipur', 'Lucknow', 'Kanpur', 'Nagpur', 'Indore', 'Bhopal', 'Patna','Vadodara', 'Coimbatore',
                'Ludhiana', 'Agra', 'Nashik', 'Ranchi', 'Meerut', 'Raipur', 'Guwahati', 'Chandigarh') then 'Tier-1'
                else 'Tier-3'
                end as city_tier,
                Cast(ZipCode as String) as Zip_Code,
                Cast(ActiveFlag as String) as Active_Flag,
                to_timestamp(CreatedDate, 'YYYY-MM-DD HH24:MI:SS') as created_ts,
                to_timestamp(ModifiedDate, 'YYYY-MM-DD HH24:MI:SS') as modified_ts,
                _stg_file_name,
                _stg_file_load_ts,
                _stg_file_md5,
                _copy_data_ts
        from stage_sch.location_stm
) as source
on target.Location_ID=source.Location_ID
when matched and (
    target.City!=source.City or 
    target.State!=source.State or 
    target.state_code!=source.state_code or 
    target.is_union_territory!=source.is_union_territory or 
    target.capital_city_flag !=source.capital_city_flag or 
    target.city_tier != source.city_tier  or 
    target.Zip_Code != source.Zip_Code or 
    target.Active_Flag != source.Active_Flag  or 
    target.modified_ts !=source.modified_ts 
) then
    update set
    target.City=source.City,
    target.State=source.State, 
    target.state_code=source.state_code, 
    target.is_union_territory=source.is_union_territory, 
    target.capital_city_flag=source.capital_city_flag ,
    target.city_tier = source.city_tier  ,
    target.Zip_Code = source.Zip_Code ,
    target.Active_Flag = source.Active_Flag, 
    target.modified_ts =source.modified_ts,
    target._stg_file_load_ts = source._stg_file_load_ts,
    target._stg_file_md5 = source._stg_file_md5,
    target._copy_data_ts = source._copy_data_ts
when not matched then 
    insert(
        Location_ID,
        City,
        State,
        state_code,
        is_union_territory,
        capital_city_flag,
        city_tier,
        Zip_Code,
        Active_Flag,
        created_ts,
        modified_ts,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5,
        _copy_data_ts
    )
    values(
        source.Location_ID,
        source.City,
        source.State,
        source.state_code,
        source.is_union_territory,
        source.capital_city_flag,
        source.city_tier,
        source.Zip_Code,
        source.Active_Flag,
        source.created_ts,
        source.modified_ts,
        source._stg_file_name,
        source._stg_file_load_ts,
        source._stg_file_md5,
        source._copy_data_ts
    );

select * from clean_sch.restaurant_location;
select * from clean_sch.restaurant_location_stm;


--level 3 
use schema consumption_sch;

create or replace table consumption_sch.restaurant_location_dim(
    restaurant_location_hk number primary key,-- hash key for the dimesntion
    location_id number(38,0) not null,
    city varchar(100) not null,
    state varchar(100) not null,
    state_code varchar(2) not null,
    is_union_territory boolean not null default false,
    capital_city_flag boolean not null default false,
    city_tier varchar(6),
    zip_code varchar(10) not null,
    active_flag varchar(10) not null,
    eff_start_dt timestamp_tz(9) not null,
    eff_end_dt timestamp_tz(9),
    current_flag boolean not null default true
)
comment='Dimesntion table for restaurant location with scd2 enabled and haskey as surrogate key';




Merge into consumption_sch.restaurant_location_dim as target 
using clean_sch.restaurant_location_stm as source 
on target.Location_ID=source.Location_ID and target.ACTIVE_FLAG=source.ACTIVE_FLAG
when matched 
    and source.METADATA$ACTION='DELETE' and source.METADATA$ISUPDATE ='True' then 
    -- update the existing record to close its validity period 
    update set 
        target.eff_end_dt= CURRENT_TIMESTAMP(),
        target.current_flag= False
    when not matched 
        and source.METADATA$ACTION='INSERT' and source.METADATA$ISUPDATE ='True'
    then 
    -- insert new record with current data and new effective start date
    insert(
        RESTAURANT_LOCATION_HK,
        LOCATION_ID,
        CITY,
        STATE,
        STATE_CODE,
        IS_UNION_TERRITORY,
        CAPITAL_CITY_FLAG,
        CITY_TIER,
        ZIP_CODE,
        ACTIVE_FLAG,
        EFF_START_DT,
        EFF_END_DT,
        CURRENT_FLAG
    )
    values(
        hash(Sha1_hex(CONCAT(source.CITY, source.STATE, source.STATE_CODE, source.ZIP_CODE))),
        source.LOCATION_ID,
        source.CITY,
        source.STATE,
        source.STATE_CODE,
        source.IS_UNION_TERRITORY,
        source.CAPITAL_CITY_FLAG,
        source.CITY_TIER,
        source.ZIP_CODE,
        source.ACTIVE_FLAG,
        CURRENT_TIMESTAMP(),
        NULL,
        TRUE  
    )
    when not matched 
        and source.METADATA$ACTION='INSERT' and source.METADATA$ISUPDATE ='False' 
    then 
        -- Insert new record with current data and new effective start date
    Insert(
        RESTAURANT_LOCATION_HK,
        LOCATION_ID,
        CITY,
        STATE,
        STATE_CODE,
        IS_UNION_TERRITORY,
        CAPITAL_CITY_FLAG,
        CITY_TIER,
        ZIP_CODE,
        ACTIVE_FLAG,
        EFF_START_DT,
        EFF_END_DT,
        CURRENT_FLAG
    )
    VALUES (
        hash(SHA1_hex(CONCAT(source.CITY, source.STATE, source.STATE_CODE, source.ZIP_CODE))),
        source.LOCATION_ID,
        source.CITY,
        source.STATE,
        source.STATE_CODE,
        source.IS_UNION_TERRITORY,
        source.CAPITAL_CITY_FLAG,
        source.CITY_TIER,
        source.ZIP_CODE,
        source.ACTIVE_FLAG,
        CURRENT_TIMESTAMP(),
        NULL,
        TRUE
    );

select * from consumption_sch.restaurant_location_dim;
    
SANDBOX.STAGE_SCH.CVS_STG
-- Adding delta table to test the entier process from stage to consumption schema.

list @stage_sch.cvs_stg/delta/location;



copy into stage_sch.location (locationid, city, state, zipcode, activeflag, createddate,
                            modifieddate, _stg_file_name, _stg_file_load_ts, _stg_file_md5, _copy_data_ts)
from (
select 
    t.$1::text as locationid,
    t.$2::text as city,
    t.$3::text as state,
    t.$4::text as zipcode,
    t.$5::text as activeflag,
    t.$6::text as createddate,
    t.$7::text as modifieddate,
    -- audit columns for tracking and debugging
    metadata$filename as _stg_file_name,
    metadata$file_last_modified as _stg_file_load_ts,
    metadata$file_content_key as _stg_file_md5,
    current_timestamp as _copy_data_ts
    
from @stage_sch.cvs_stg/delta/location/delta-day02-2rows-update.csv t
)
file_format = (format_name='stage_sch.csv_file_format')
on_error = abort_statement;



--- I tried uploading error files to make sure my process is working correctly so yes it is. So the cleaning schema didn't insert data because error in location id
-- Now as the data is loacted in the stage schema location table  and location stream table I will have to remove that data so my tables are clean when ever a new record comes in it process without error



delete from stage_sch.location where locationid like'%|%'-- remove the error file from location table as well of stage schema

delete from stage_sch.location where locationid like '%junk%';