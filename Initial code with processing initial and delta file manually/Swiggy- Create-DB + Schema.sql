-- use sysadmin role.
use role sysadmin;

-- create development sendbox database/schema if does not exist 
 create database if not exists sandbox;

 use database sandbox;

create schema if not exists stage_sch;
create schema if not exists clean_sch;
create schema if not exists consumption_sch;
create schema if not exists common;-- policy objects will be stored 


use schema stage_sch;
 -- create file format to process the cvs files
 create file format if not exists stage_sch.csv_file_format
        type='csv'
        compression='auto'
        field_delimiter =','
        record_delimiter= '\n'
        skip_header=1 
        field_optionally_enclosed_by = '\042'
        null_if=('\\N');

create stage stage_sch.cvs_stg
directory =(enable=True)
COMMENT= "this is snowflake internal stage";
        
--policy tag

create or replace tag
    common.pii_policy_tag
    allowed_values 'PII','PRICE','SENSITIVE', 'EMAIL'
    comment= 'This PII policy tag object';

create or replace masking policy
    common.pii_masking_policy as (pii_text string)
    returns string ->
    to_varchar('**PII**');

create or replace masking policy
    common.email_masking_policy as (email_text string)
    returns string ->
    to_varchar('** EMAIL**');

create or replace masking policy
    common.phone_masking_policy as (phone string)
    returns string ->
    to_varchar('**Phone**');

 -- to check the loading data and validating the data-- use the root location. When tables are not create and only files is loaded in the stage enviroment 
list @stage_sch.cvs_stg/initial/location;
list @stage_sch.cvs_stg/delta;SANDBOX.STAGE_SCH.CVS_STG



--- sql command to chech the data in stage location

select 
    t.$1::text as locationid,
    t.$2::text as city,
    t.$3::text as state,
    t.$4::text as zipcode,
    t.$5::text as activeflag,
    t.$6::text as createddate,
    t.$7::text as modifieddate
from @stage_sch.cvs_stg/initial/location (file_format => 'stage_sch.csv_file_format') t;