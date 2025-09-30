use role sysadmin;
use warehouse compute_wh;
use database sandbox;
use schema stage_sch;

 select 
        t.$1::text as customerid,
        t.$2::text as name,
        t.$3::text as mobile,
        t.$4::text as email,
        t.$5::text as loginbyusing,
        t.$6::text as gender,
        t.$7::text as dob,
        t.$8::text as anniversary,
        t.$9::text as preferences,
        t.$10::text as createddate,
        t.$11::text as modifieddate,
        metadata$filename as _stg_file_name,
        metadata$file_last_modified as _stg_file_load_ts,
        metadata$file_content_key as _stg_file_md5,
        current_timestamp as _copy_data_ts
    from @stage_sch.cvs_stg/delta/customer/day-02-insert-update-Copy3.csv t


---level 1
create or replace table stage_sch.customer (
    customerid text,                    -- primary key as text
    name text,                          -- name as text
    mobile text WITH TAG (common.pii_policy_tag = 'PII'),                        -- mobile number as text
    email text WITH TAG (common.pii_policy_tag = 'EMAIL'),                         -- email as text
    loginbyusing text,                  -- login method as text
    gender text WITH TAG (common.pii_policy_tag = 'PII'),                        -- gender as text
    dob text WITH TAG (common.pii_policy_tag = 'PII'),                           -- date of birth as text
    anniversary text,                   -- anniversary as text
    preferences text,                   -- preferences as text
    createddate text,                   -- created date as text
    modifieddate text,                  -- modified date as text

    -- audit columns with appropriate data types
    _stg_file_name text,
    _stg_file_load_ts timestamp,
    _stg_file_md5 text,
    _copy_data_ts timestamp default current_timestamp
)
comment = 'This is the customer stage/raw table where data will be copied from internal stage using copy command. This is as-is data represetation from the source location. All the columns are text data type except the audit columns that are added for traceability.';

create or replace stream stage_sch.customer_stm
on table stage_sch.customer
append_only = true 
comment=' this is customer stream where we are just taking delta table details '

-- SNOWPIPE
CREATE OR REPLACE PIPE stage_sch.customer_pipes
AUTO_INGEST = TRUE
INTEGRATION = Azure_Notification
AS 
copy into stage_sch.customer (customerid, name, mobile, email, loginbyusing, gender, dob, anniversary, 
                    preferences, createddate, modifieddate, 
                    _stg_file_name, _stg_file_load_ts, _stg_file_md5, _copy_data_ts)
from (
    select 
        t.$1::text as customerid,
        t.$2::text as name,
        t.$3::text as mobile,
        t.$4::text as email,
        t.$5::text as loginbyusing,
        t.$6::text as gender,
        t.$7::text as dob,
        t.$8::text as anniversary,
        t.$9::text as preferences,
        t.$10::text as createddate,
        t.$11::text as modifieddate,
        metadata$filename as _stg_file_name,
        metadata$file_last_modified as _stg_file_load_ts,
        metadata$file_content_key as _stg_file_md5,
        current_timestamp as _copy_data_ts
        from @azure_external_stage/customers t  
)
file_format=(format_name='stage_sch.csv_file_format')
ON_ERROR = 'CONTINUE'
PATTERN = '.*\.csv';

select * from stage_sch.customer;

select * from stage_sch.customer_stm;



-- level 2 clean 

CREATE OR REPLACE TABLE CLEAN_SCH.CUSTOMER (
    
    CUSTOMER_SK NUMBER AUTOINCREMENT PRIMARY KEY,                -- Auto-incremented primary key
    CUSTOMER_ID STRING NOT NULL,                                 -- Customer ID
    First_Name STRING(100) NOT NULL,    -- Customer name
    Last_Name STRING(100) NOT NULL,      -- Customer name
    MOBILE STRING(15)  WITH TAG (common.pii_policy_tag = 'PII'),                                           -- Mobile number, accommodating international format
    EMAIL STRING(100) WITH TAG (common.pii_policy_tag = 'EMAIL'),                                           -- Email
    LOGIN_BY_USING STRING(50),                                   -- Method of login (e.g., Social, Google, etc.)
    GENDER STRING(10)  WITH TAG (common.pii_policy_tag = 'PII'),                                           -- Gender
    DOB DATE WITH TAG (common.pii_policy_tag = 'PII'),                                                    -- Date of birth in DATE format
    ANNIVERSARY DATE,                                            -- Anniversary in DATE format
    PREFERENCES STRING,                                          -- Customer preferences
    CUISINE_TYPES STRING,                                        -- Customer CUISINE_Types
    CREATED_DT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP,           -- Record creation timestamp
    MODIFIED_DT TIMESTAMP_TZ,                                    -- Record modification timestamp, allows NULL if not modified

    -- Additional audit columns
    _STG_FILE_NAME STRING,                                       -- File name for audit
    _STG_FILE_LOAD_TS TIMESTAMP_NTZ,                             -- File load timestamp
    _STG_FILE_MD5 STRING,                                        -- MD5 hash for file content
    _COPY_DATA_TS TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP        -- Copy data timestamp
)
comment = 'Customer entity under clean schema with appropriate data type under clean schema layer, data is populated using merge statement from the stage layer location table. This table does not support SCD2';

create or replace stream CLEAN_SCH.CUSTOMER_Stm
on table CLEAN_SCH.CUSTOMER
comment='This is the stream object on customer entity to track insert, update, and delete changes';




CREATE OR REPLACE TASK clean_sch.customer_task
WAREHOUSE = compute_wh
SCHEDULE = 'USING CRON 30 20 * * * UTC' -- event-driven
AS
merge into CLEAN_SCH.CUSTOMER AS target
USING (
    SELECT 
        CUSTOMERID::STRING AS CUSTOMER_ID,
        SPLIT_PART(Name, ' ', 1)::STRING AS First_Name,
        SPLIT_PART(Name, ' ', 2)::STRING AS Last_Name,
        MOBILE::STRING AS MOBILE,
        EMAIL::STRING AS EMAIL,
        LOGINBYUSING::STRING AS LOGIN_BY_USING,
        GENDER::STRING AS GENDER,
        COALESCE(TRY_TO_DATE(DOB, 'YYYY-MM-DD'), TRY_TO_DATE(DOB, 'MM/DD/YYYY')) AS DOB,
        COALESCE(TRY_TO_DATE(ANNIVERSARY, 'YYYY-MM-DD'), TRY_TO_DATE(ANNIVERSARY, 'MM/DD/YYYY')) AS ANNIVERSARY,
        
        -- Extract food preference and cuisine types from JSON
        PARSE_JSON(PREFERENCES):FoodPreference::STRING AS PREFERENCES, -- Customer preferences
        ARRAY_TO_STRING(PARSE_JSON(PREFERENCES):CuisineTypes, ', ') AS CUISINE_TYPES, --CuisineTypes
        TO_CHAR(COALESCE(TRY_TO_TIMESTAMP(CREATEDDATE, 'MM/DD/YYYY HH24:MI'),TRY_TO_TIMESTAMP(CREATEDDATE, 'MM/DD/YYYY')),'YYYY-MM-DD"T"HH24:MI:SS.FF6') AS CREATED_DT,
            TO_CHAR(COALESCE(TRY_TO_TIMESTAMP(MODIFIEDDATE, 'MM/DD/YYYY HH24:MI'),TRY_TO_TIMESTAMP(MODIFIEDDATE, 'MM/DD/YYYY')),'YYYY-MM-DD"T"HH24:MI:SS.FF6') AS MODIFIED_DT,
        _STG_FILE_NAME
        _STG_FILE_LOAD_TS,
        _STG_FILE_MD5,
        _COPY_DATA_TS
    FROM STAGE_SCH.CUSTOMER_STM
) AS source
ON target.CUSTOMER_ID = source.CUSTOMER_ID
WHEN MATCHED AND (
        target.FIRST_NAME != source.FIRST_NAME or
        target.LAST_NAME != source.LAST_NAME or
        target.MOBILE != source.MOBILE or
        target.EMAIL != source.EMAIL or
        target.LOGIN_BY_USING != source.LOGIN_BY_USING or
        target.GENDER != source.GENDER or
        target.DOB != source.DOB or
        target.ANNIVERSARY != source.ANNIVERSARY or
        target.PREFERENCES != source.PREFERENCES or
        target.CUISINE_TYPES != source.CUISINE_TYPES or
        target.CREATED_DT != source.CREATED_DT or
        target.MODIFIED_DT != source.MODIFIED_DT
) THEN 
    UPDATE SET 
        target.FIRST_NAME = source.FIRST_NAME,
        target.LAST_NAME = source.LAST_NAME,
        target.MOBILE = source.MOBILE,
        target.EMAIL = source.EMAIL,
        target.LOGIN_BY_USING = source.LOGIN_BY_USING,
        target.GENDER = source.GENDER,
        target.DOB = COALESCE(source.DOB, target.DOB),
        target.ANNIVERSARY = COALESCE(source.ANNIVERSARY, target.ANNIVERSARY),
        target.PREFERENCES = source.PREFERENCES,
        target.CUISINE_TYPES = source.CUISINE_TYPES,
        target.CREATED_DT = source.CREATED_DT,
        target.MODIFIED_DT = source.MODIFIED_DT,
        target._STG_FILE_NAME = source._STG_FILE_NAME,
        target._STG_FILE_LOAD_TS = source._STG_FILE_LOAD_TS,
        target._STG_FILE_MD5 = source._STG_FILE_MD5,
        target._COPY_DATA_TS = source._COPY_DATA_TS
WHEN NOT MATCHED THEN
    INSERT (
        CUSTOMER_ID,
        FIRST_NAME,
        LAST_NAME,
        MOBILE,
        EMAIL,
        LOGIN_BY_USING,
        GENDER,
        DOB,
        ANNIVERSARY,
        PREFERENCES, 
        CUISINE_TYPES,
        CREATED_DT,
        MODIFIED_DT,
        _STG_FILE_NAME,
        _STG_FILE_LOAD_TS,
        _STG_FILE_MD5,
        _COPY_DATA_TS
    )
    VALUES (
        source.CUSTOMER_ID,
        source.FIRST_NAME,
        source.LAST_NAME,
        source.MOBILE,
        source.EMAIL,
        source.LOGIN_BY_USING,
        source.GENDER,
        source.DOB,
        source.ANNIVERSARY,
        source.PREFERENCES, 
        source.CUISINE_TYPES,
        source.CREATED_DT,
        source.MODIFIED_DT,
        source._STG_FILE_NAME,
        source._STG_FILE_LOAD_TS,
        source._STG_FILE_MD5,
        source._COPY_DATA_TS
    );

 
ALTER TASK clean_sch.customer_task RESUME;

 SHOW TASKS LIKE 'customer_task' IN SCHEMA clean_sch;  

SELECT *
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
  TASK_NAME => 'clean_sch.customer_task',
  RESULT_LIMIT => 10
))
ORDER BY SCHEDULED_TIME DESC;
 
   SELECT * FROM CLEAN_SCH.CUSTOMER;
-- LEVEL 3 
CREATE OR REPLACE TABLE CONSUMPTION_SCH.CUSTOMER_DIM (
    CUSTOMER_HK NUMBER PRIMARY KEY,               -- Surrogate key for the customer
    CUSTOMER_ID STRING NOT NULL,                                 -- Natural key for the customer
    FIRST_NAME STRING(100) NOT NULL,                                   -- Customer FIRST name
    LAST_NAME STRING(100) NOT NULL,                             -- Customer LAST name
    MOBILE STRING(15) WITH TAG (common.pii_policy_tag = 'PII'),                                           -- Mobile number
    EMAIL STRING(100) WITH TAG (common.pii_policy_tag = 'EMAIL'),                                           -- Email
    LOGIN_BY_USING STRING(50),                                   -- Method of login
    GENDER STRING(10) WITH TAG (common.pii_policy_tag = 'PII'),                                           -- Gender
    DOB DATE WITH TAG (common.pii_policy_tag = 'PII'),                                                    -- Date of birth
    ANNIVERSARY DATE,                                            -- Anniversary
    PREFERENCES STRING,                                          -- Preferences
    CUISINE_TYPES String,                                         --CUISINE_TYPES
    EFF_START_DATE TIMESTAMP_TZ,                                 -- Effective start date
    EFF_END_DATE TIMESTAMP_TZ,                                   -- Effective end date (NULL if active)
    IS_CURRENT BOOLEAN                                           -- Flag to indicate the current record
)
COMMENT = 'Customer Dimension table with SCD Type 2 handling for historical tracking.';


SANDBOX.STAGE_SCH.CVS_STGSANDBOX.STAGE_SCH.CVS_STG


CREATE OR REPLACE TASK consumption_sch.customer_scd2_task
WAREHOUSE = compute_wh
SCHEDULE  = 'USING CRON 3 21 * * * UTC'
AS
MERGE INTO 
    CONSUMPTION_SCH.CUSTOMER_DIM AS target
USING 
    CLEAN_SCH.CUSTOMER_STM AS source
ON 
    target.CUSTOMER_ID = source.CUSTOMER_ID AND
    target.FIRST_NAME = source.FIRST_NAME AND
    target.LAST_NAME = source.LAST_NAME AND
    target.MOBILE = source.MOBILE AND
    target.EMAIL = source.EMAIL AND
    target.LOGIN_BY_USING = source.LOGIN_BY_USING AND
    target.GENDER = source.GENDER AND
    target.DOB = source.DOB AND
    target.ANNIVERSARY = source.ANNIVERSARY AND
    target.PREFERENCES = source.PREFERENCES AND
    target.CUISINE_TYPES = source.CUISINE_TYPES
WHEN MATCHED 
    AND source.METADATA$ACTION = 'DELETE' AND source.METADATA$ISUPDATE = 'TRUE' THEN
    -- Update the existing record to close its validity period
    UPDATE SET 
        target.EFF_END_DATE = CURRENT_TIMESTAMP(),
        target.IS_CURRENT = FALSE
WHEN NOT MATCHED 
    AND source.METADATA$ACTION = 'INSERT' AND source.METADATA$ISUPDATE = 'TRUE' THEN
    -- Insert new record with current data and new effective start date
    INSERT (
        CUSTOMER_HK,
        CUSTOMER_ID,
        FIRST_NAME,
        LAST_NAME,
        MOBILE,
        EMAIL,
        LOGIN_BY_USING,
        GENDER,
        DOB,
        ANNIVERSARY,
        PREFERENCES,
        CUISINE_TYPES,
        EFF_START_DATE,
        EFF_END_DATE,
        IS_CURRENT
    )
    VALUES (
        hash(SHA1_hex(CONCAT(source.CUSTOMER_ID, source.FIRST_NAME,source.LAST_NAME, source.MOBILE, 
            source.EMAIL, source.LOGIN_BY_USING, source.GENDER, source.DOB, 
            source.ANNIVERSARY, source.PREFERENCES, source.CUISINE_TYPES))),
        source.CUSTOMER_ID,
        source.FIRST_NAME,
        source.LAST_NAME,
        source.MOBILE,
        source.EMAIL,
        source.LOGIN_BY_USING,
        source.GENDER,
        source.DOB,
        source.ANNIVERSARY,
        source.PREFERENCES,
        source.CUISINE_TYPES,
        CURRENT_TIMESTAMP(),
        NULL,
        TRUE
    )
WHEN NOT MATCHED 
    AND source.METADATA$ACTION = 'INSERT' AND source.METADATA$ISUPDATE = 'FALSE' THEN
    -- Insert new record with current data and new effective start date
    INSERT (
        CUSTOMER_HK,
        CUSTOMER_ID,
        FIRST_NAME,
        LAST_NAME,
        MOBILE,
        EMAIL,
        LOGIN_BY_USING,
        GENDER,
        DOB,
        ANNIVERSARY,
        PREFERENCES,
        CUISINE_TYPES,
        EFF_START_DATE,
        EFF_END_DATE,
        IS_CURRENT
    )
    VALUES (
        hash(SHA1_hex(CONCAT(source.CUSTOMER_ID, source.FIRST_NAME, source.LAST_NAME, source.MOBILE, 
            source.EMAIL, source.LOGIN_BY_USING, source.GENDER, source.DOB, 
            source.ANNIVERSARY, source.PREFERENCES, source.CUISINE_TYPES))),
        source.CUSTOMER_ID,
        source.FIRST_NAME,
        source.LAST_NAME,
        source.MOBILE,
        source.EMAIL,
        source.LOGIN_BY_USING,
        source.GENDER,
        source.DOB,
        source.ANNIVERSARY,
        source.PREFERENCES,
        source.CUISINE_TYPES,
        CURRENT_TIMESTAMP(),
        NULL,
        TRUE
    );

 ALTER TASK consumption_sch.customer_scd2_task RESUME;


 
select * from stage_Sch.customer;
select * from clean_sch.customer;
select * from  CONSUMPTION_SCH.CUSTOMER_DIM;

list @stage_sch.cvs_stg/delta/customer/;

copy into stage_sch.customer (customerid, name, mobile, email, loginbyusing, gender, dob, anniversary, 
                    preferences, createddate, modifieddate, 
                    _stg_file_name, _stg_file_load_ts, _stg_file_md5, _copy_data_ts)
from (
    select 
        t.$1::text as customerid,
        t.$2::text as name,
        t.$3::text as mobile,
        t.$4::text as email,
        t.$5::text as loginbyusing,
        t.$6::text as gender,
        t.$7::text as dob,
        t.$8::text as anniversary,
        t.$9::text as preferences,
        t.$10::text as createddate,
        t.$11::text as modifieddate,
        metadata$filename as _stg_file_name,
        metadata$file_last_modified as _stg_file_load_ts,
        metadata$file_content_key as _stg_file_md5,
        current_timestamp as _copy_data_ts
    from @stage_sch.cvs_stg/delta/customer/day-02-insert-update-Copy3.csv t
)
file_format = (format_name = 'stage_sch.csv_file_format')
on_error = abort_statement;