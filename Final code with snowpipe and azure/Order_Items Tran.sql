use role sysadmin;
use database sandbox;
use schema stage_sch;
use warehouse compute_wh;



create or replace table stage_sch.orderitem (
    orderitemid text comment 'Primary Key (Source System)',              -- primary key as text
    orderid text comment 'Order FK(Source System)',                  -- foreign key reference as text (no constraint in snowflake)
    menuid text comment 'Menu FK(Source System)',                   -- foreign key reference as text (no constraint in snowflake)
    quantity text,                 -- quantity as text
    price text,                    -- price as text (no decimal constraint)
    subtotal text,                 -- subtotal as text (no decimal constraint)
    createddate text,              -- created date as text
    modifieddate text,             -- modified date as text

    -- audit columns with appropriate data types
    _stg_file_name text,
    _stg_file_load_ts timestamp,
    _stg_file_md5 text,
    _copy_data_ts timestamp default current_timestamp
)
comment = 'This is the order item stage/raw table where data will be copied from internal stage using copy command. This is as-is data represetation from the source location. All the columns are text data type except the audit columns that are added for traceability.';

create or replace stream stage_sch.orderitem_stm 
on table stage_sch.orderitem
append_only = true
comment = 'This is the append-only stream object on order item table that only gets delta data';

select * from  stage_sch.orderitem_stm
select * from  stage_sch.orderitem

truncate table  stage_sch.orderitem

list @stage_sch.cvs_stg/initial/order-item;

CREATE OR REPLACE PIPE stage_sch.order_items_pipes
AUTO_INGEST = TRUE
INTEGRATION = Azure_Notification
AS 
copy into stage_sch.orderitem (orderitemid, orderid, menuid, quantity, price, 
                     subtotal, createddate, modifieddate,
                     _stg_file_name, _stg_file_load_ts, _stg_file_md5, _copy_data_ts)
from (
    select 
        t.$1::text as orderitemid,
        t.$2::text as orderid,
        t.$3::text as menuid,
        t.$4::text as quantity,
        t.$5::text as price,
        t.$6::text as subtotal,
        t.$7::text as createddate,
        t.$8::text as modifieddate,
        metadata$filename as _stg_file_name,
        metadata$file_last_modified as _stg_file_load_ts,
        metadata$file_content_key as _stg_file_md5,
        current_timestamp as _copy_data_ts
         from @azure_external_stage/order_items t  
)
file_format=(format_name='stage_sch.csv_file_format')
ON_ERROR = 'CONTINUE'
PATTERN = '.*\.csv';


select * from stage_sch.orderitem




CREATE OR REPLACE TABLE clean_sch.order_item (
    order_item_sk NUMBER AUTOINCREMENT primary key comment 'Surrogate Key (EDW)',    -- Auto-incremented unique identifier for each order item
    order_item_id NUMBER  NOT NULL UNIQUE comment 'Primary Key (Source System)',
    order_id_fk NUMBER  NOT NULL comment 'Order FK(Source System)',                  -- Foreign key reference for Order ID
    menu_id_fk NUMBER  NOT NULL comment 'Menu FK(Source System)',                   -- Foreign key reference for Menu ID
    quantity NUMBER(10, 2),                 -- Quantity as a decimal number
    price NUMBER(10, 2),                    -- Price as a decimal number
    subtotal NUMBER(10, 2),                 -- Subtotal as a decimal number
    created_dt TIMESTAMP,                 -- Created date of the order item
    modified_dt TIMESTAMP,                -- Modified date of the order item

    -- Audit columns
    _stg_file_name VARCHAR(255),            -- File name of the staging file
    _stg_file_load_ts TIMESTAMP,            -- Timestamp when the file was loaded
    _stg_file_md5 VARCHAR(255),             -- MD5 hash of the file for integrity check
    _copy_data_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP -- Timestamp when data is copied into the clean layer
)
comment = 'Order item entity under clean schema with appropriate data type under clean schema layer, data is populated using merge statement from the stage layer location table. This table does not support SCD2';

create or replace stream CLEAN_SCH.order_item_stm 
on table CLEAN_SCH.order_item
comment = 'This is the stream object on order_item table table to track insert, update, and delete changes';


select * from clean_sch.order_item_stm;
select * from clean_sch.order_item;

truncate table clean_sch.order_item;

CREATE OR REPLACE TASK clean_sch.order_items
WAREHOUSE = compute_wh
SCHEDULE = 'USING CRON 30 20 * * * UTC' -- event-driven
AS
MERGE INTO clean_sch.order_item AS target
USING (
    SELECT 
        TRY_CAST(orderitemid AS Int) AS order_item_id,  
        TRY_CAST(orderid as NUMBER) as order_id_fk,
        TRY_CAST(menuid  AS NUMBER) as menu_id_fk,
        TRY_CAST(quantity AS NUMBER(10, 2)) AS quantity,
        TRY_CAST(price AS NUMBER(10, 2)) as price,
        Try_Cast(subtotal as NUMBER(10, 2)) AS subtotal,
        TRY_CAST(createddate AS TIMESTAMP_NTZ) AS created_dt,  -- Renamed column
        TRY_CAST(modifieddate AS TIMESTAMP_NTZ) AS modified_dt, -- Renamed column
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5,
        _copy_data_ts
    FROM stage_sch.orderitem_stm
    where orderitemid >=40
) AS source
ON target.order_item_id = source.order_item_id and 
target.order_id_fk = source.order_id_fk and
target.menu_id_fk = source.menu_id_fk
WHEN MATCHED AND (
        target.quantity != source.quantity or
        target.price != source.price or
        target.subtotal != source.subtotal or
        target.created_dt != source.created_dt or
        target.modified_dt != source.modified_dt
) THEN 
    UPDATE SET
        target.quantity = source.quantity,
        target.price = source.price,
        target.subtotal = source.subtotal,
        target.created_dt = source.created_dt,
        target.modified_dt = source.modified_dt,
        target._stg_file_name = source._stg_file_name,
        target._stg_file_load_ts = source._stg_file_load_ts,
        target._stg_file_md5 = source._stg_file_md5,
        target._copy_data_ts = source._copy_data_ts
WHEN NOT MATCHED THEN
    INSERT (
       order_item_id,
        order_id_fk,
        menu_id_fk,
        quantity,
        price,
        subtotal,
        created_dt,
        modified_dt,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5,
        _copy_data_ts
    )
    VALUES (
       source.order_item_id,
        source.order_id_fk,
        source.menu_id_fk,
        source.quantity,
        source.price,
        source.subtotal,
        source.created_dt ,
        source.modified_dt,
        source._stg_file_name,
        source._stg_file_load_ts,
        source._stg_file_md5,
        source._copy_data_ts
    );
ALTER TASK clean_sch.order_items RESUME;
-- part-2
list @stage_sch.cvs_stg/delta/order-item/;

copy into stage_sch.orderitem (orderitemid, orderid, menuid, quantity, price, 
                     subtotal, createddate, modifieddate,
                     _stg_file_name, _stg_file_load_ts, _stg_file_md5, _copy_data_ts)
from (
    select 
        t.$1::text as orderitemid,
        t.$2::text as orderid,
        t.$3::text as menuid,
        t.$4::text as quantity,
        t.$5::text as price,
        t.$6::text as subtotal,
        t.$7::text as createddate,
        t.$8::text as modifieddate,
        metadata$filename as _stg_file_name,
        metadata$file_last_modified as _stg_file_load_ts,
        metadata$file_content_key as _stg_file_md5,
        current_timestamp as _copy_data_ts
    from @stage_sch.cvs_stg/delta/order-item/day-02-order-item.csv t
)
file_format = (format_name = 'stage_sch.csv_file_format')
on_error = abort_statement;