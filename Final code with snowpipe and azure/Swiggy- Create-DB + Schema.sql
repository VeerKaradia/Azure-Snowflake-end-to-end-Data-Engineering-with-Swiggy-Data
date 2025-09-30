SANDBOX.CLEAN_SCH.ORDERSSANDBOX.CLEAN_SCH.ORDERSSANDBOX.CLEAN_SCH.ORDER_ITEMSANDBOX.CLEAN_SCH.DELIVERYSANDBOX.CLEAN_SCH.DELIVERYSANDBOX.CLEAN_SCH.RESTAURANT-- use sysadmin role.
SANDBOX.STAGE_SCH.CVS_STG

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
        null_if=('\\N')
        SKIP_BLANK_LINES = TRUE;

-- Manually uplaoding the the file in Stage_sch 
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

 -- to check the loading data and validating the data use the root location. When tables are not create and only files is loaded in the stage environment 
list @stage_sch.cvs_stg/initial/location;
list @stage_sch.cvs_stg/delta;SANDBOX.STAGE_SCH.CVS_STG


-- External stage azure 

-- Granting permissions for sysadmin to access the external table, integration and notification integration.
GRANT USAGE ON stage stage_sch.azure_external_stage TO ROLE SYSADMIN;



--To connect Azure blob with snowflake I am using Azure integration and Notification Integration with Azure Tenant ID. This is to make my data pull automatically from the blob to snowflake.
CREATE OR REPLACE STAGE stage_sch.azure_external_stage
  URL = 'azure://swiggydata.blob.core.windows.net/swiggyrawdata/data/'
  STORAGE_INTEGRATION = AZURE_INTEGRATION;

--Integration and Notifcation Integration set up
CREATE STORAGE INTEGRATION AZURE_INTEGRATION
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'AZURE'
  ENABLED = TRUE
  AZURE_TENANT_ID = '6c6162c2-356b-4825-aa1f-3782a01f730d'  -- From Azure AD
  STORAGE_ALLOWED_LOCATIONS = ('azure://swiggydata.blob.core.windows.net/swiggyrawdata/data/');

desc  STORAGE INTEGRATION AZURE_INTEGRATION;

CREATE OR REPLACE  NOTIFICATION INTEGRATION Azure_Notification
  ENABLED = TRUE
  TYPE = QUEUE
  NOTIFICATION_PROVIDER = AZURE_STORAGE_QUEUE
  AZURE_STORAGE_QUEUE_PRIMARY_URI = 'https://swiggydata.queue.core.windows.net/notification'
  AZURE_TENANT_ID = '6c6162c2-356b-4825-aa1f-3782a01f730d'
  USE_PRIVATELINK_ENDPOINT = False
  COMMENT = 'notfication'

DESC  NOTIFICATION INTEGRATION Azure_Notification;

--Additional PII for Email, Phone.
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

LIST stage @azure_external_stage;



USE ROLE ACCOUNTADMIN;
SELECT SYSTEM$VALIDATE_STORAGE_INTEGRATION(
  'AZURE_INTEGRATION',
  '6c6162c2-356b-4825-aa1f-3782a01f730d',
  'azure://swiggydata.blob.core.windows.net/swiggyrawdata/data/',
  'eastus'
);

LIST @stage_sch.azure_external_stage;

list @azure_external_stage

--- sql command to chech the data in stage location

-- select 
    -- t.$1::text as locationid,
    -- t.$2::text as city,
    -- t.$3::text as state,
    -- t.$4::text as zipcode,
    -- t.$5::text as activeflag,
    -- t.$6::text as createddate,
    -- t.$7::text as modifieddate
-- from @stage_sch.cvs_stg/initial/location (file_format => 'stage_sch.csv_file_format') t;