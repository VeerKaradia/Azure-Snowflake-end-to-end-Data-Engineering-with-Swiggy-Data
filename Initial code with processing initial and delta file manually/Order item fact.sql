use role sysadmin;
use warehouse compute_wh;
use database sandbox;
use schema CONSUMPTION_SCH;

select * from CONSUMPTION_SCH.order_item_fact
where order_id =14;

Create or replace table CONSUMPTION_SCH.order_item_fact (
    order_item_fact_hk number autoincrement comment 'Hash key for order item',
    order_item_id  number not null comment 'Order item FK',
    order_id number not null comment 'order FK',
    customer_dim_key  number not null comment 'Customer dim hash key',
    customer_address_dim_key  not null number,
    restaurant_dim_key  not null  number,
    restaurant_location_dim_key  not null  number,
    menu_dim_key  not null  number,
    delivery_agent_dim_key  not null number,
    order_date_dim_key  not null number,
    quantity  number,
    price  number(10,2),
    subtotal  number(10,2),
    delivery_status  varchar,
    estimated_time  varchar

)
comment='The item order fact table that has item level price, quantity, and other details'





MERGE INTO CONSUMPTION_SCH.order_item_fact AS target
USING (
    SELECT 
        oi.Order_Item_ID AS order_item_id,
        oi.Order_ID_fk AS order_id,
        c.CUSTOMER_HK AS customer_dim_key,
        ca.CUSTOMER_ADDRESS_HK AS customer_address_dim_key,
        r.RESTAURANT_HK AS restaurant_dim_key, 
        rl.restaurant_location_hk as restaurant_location_dim_key,
        m.Menu_Dim_HK AS menu_dim_key,
        da.DELIVERY_AGENT_HK AS delivery_agent_dim_key,
        dd.DATE_DIM_HK AS order_date_dim_key,
        oi.Quantity::number(2) AS quantity,
        oi.Price AS price,
        oi.Subtotal AS subtotal,
        o.PAYMENT_METHOD,
        d.delivery_status AS delivery_status,
        d.estimated_time AS estimated_time,
    FROM 
        clean_sch.order_item_stm oi
    JOIN 
        clean_sch.orders_stm o ON oi.Order_ID_fk = o.Order_ID
    JOIN 
        clean_sch.delivery_stm d ON o.Order_ID = d.Order_ID_fk
    JOIN 
        consumption_sch.CUSTOMER_DIM c on o.Customer_ID_fk = c.customer_id
    JOIN 
        consumption_sch.CUSTOMER_ADDRESS_DIM ca on c.Customer_ID = ca.CUSTOMER_ID_fk
    JOIN 
        consumption_sch.restaurant_dim r on o.Restaurant_ID_fk = r.restaurant_id
    JOIN 
        consumption_sch.menu_dim m ON oi.MENU_ID_fk = m.menu_id
    JOIN 
        consumption_sch.delivery_agent_dim da ON d.Delivery_Agent_ID_fk = da.delivery_agent_id
    JOIN 
        consumption_sch.restaurant_location_dim rl on r.LOCATION_ID_FK = rl.location_id
    JOIN 
        CONSUMPTION_SCH.DATE_DIM dd on dd.calendar_date = date(o.order_date)
) AS source_stm
ON 
    target.order_item_id = source_stm.order_item_id and 
    target.order_id = source_stm.order_id
WHEN MATCHED THEN
    -- Update existing fact record
    UPDATE SET
        target.customer_dim_key = source_stm.customer_dim_key,
        target.customer_address_dim_key = source_stm.customer_address_dim_key,
        target.restaurant_dim_key = source_stm.restaurant_dim_key,
        target.restaurant_location_dim_key = source_stm.restaurant_location_dim_key,
        target.menu_dim_key = source_stm.menu_dim_key,
        target.delivery_agent_dim_key = source_stm.delivery_agent_dim_key,
        target.order_date_dim_key = source_stm.order_date_dim_key,
        target.quantity = source_stm.quantity,
        target.price = source_stm.price,
        target.subtotal = source_stm.subtotal,
        target.delivery_status = source_stm.delivery_status,
        target.estimated_time = source_stm.estimated_time
WHEN NOT MATCHED THEN
    -- Insert new fact record
    INSERT (
        order_item_id,
        order_id,
        customer_dim_key,
        customer_address_dim_key,
        restaurant_dim_key,
        restaurant_location_dim_key,
        menu_dim_key,
        delivery_agent_dim_key,
        order_date_dim_key,
        quantity,
        price,
        subtotal,
        delivery_status,
        estimated_time
    )
    VALUES (
        source_stm.order_item_id,
        source_stm.order_id,
        source_stm.customer_dim_key,
        source_stm.customer_address_dim_key,
        source_stm.restaurant_dim_key,
        source_stm.restaurant_location_dim_key,
        source_stm.menu_dim_key,
        source_stm.delivery_agent_dim_key,
        source_stm.order_date_dim_key,
        source_stm.quantity,
        source_stm.price,
        source_stm.subtotal,
        source_stm.delivery_status,
        source_stm.estimated_time
    );

/*merge into CONSUMPTION_SCH.order_item_fact as target
using( 
    select 
TRY_CAST(o.order_item_id as Number) AS order_item_id,
TRY_CAST(o.order_id_fk AS NUMBER) AS order_id,         
TRY_CAST(c.CUSTOMER_HK AS NUMBER) AS customer_dim_key,
TRY_CAST(c1.CUSTOMER_ADDRESS_HK AS NUMBER) AS customer_address_dim_key,
TRY_CAST(r.RESTAURANT_HK AS NUMBER) AS restaurant_dim_key,
TRY_CAST(r1.restaurant_location_hk AS NUMBER) AS restaurant_location_dim_key,
TRY_CAST(m.Menu_Dim_HK AS NUMBER) AS menu_dim_key,
TRY_CAST(da.DELIVERY_AGENT_HK AS NUMBER) AS delivery_agent_dim_key,
TRY_CAST(d3.Date_Dim_HK AS NUMBER) AS order_date_dim_key,
TRY_CAST(o.quantity AS NUMBER(10, 2)) AS quantity,
TRY_CAST(o.price AS NUMBER(10, 2)) as price,
Try_Cast(o.subtotal as NUMBER(10, 2)) AS subtotal,
TRY_CAST(o1.Payment_method AS VARCHAR) AS payment_method,
TRY_CAST(d.delivery_status AS VARCHAR) AS delivery_status,
TRY_CAST(d.estimated_time AS VARCHAR) AS estimated_time
    
    from clean_sch.order_item_stm as o

     join clean_sch.orders_stm as o1 on o1.Order_ID=o.order_id_fk 
     join clean_sch.delivery_stm as d on d.order_id_fk=O1.Order_ID
     join consumption_sch.customer_dim as c on o1.CUSTOMER_ID_FK= c.CUSTOMER_ID
     join consumption_sch.customer_address_dim as c1 on c1.CUSTOMER_ID_FK=c.CUSTOMER_ID
     join consumption_sch.restaurant_dim as r on o1.RESTAURANT_ID_FK=r.RESTAURANT_ID
     join consumption_sch.menu_dim as m on m.menu_id = o.menu_id_fk
     join consumption_sch.delivery_agent_dim as da on da.delivery_agent_id=d.delivery_agent_id_fk
     join consumption_sch.restaurant_location_dim r1 on r.location_id_fk=r1.LOCATION_ID
     join consumption_sch.date_dim as d3 on d3.calendar_date= date(o1.order_date)) as source 
on target.order_item_id=source.order_item_id and 
target.order_id = source.order_id 
WHEN MATCHED AND (
        target.customer_dim_key != source.customer_dim_key or
        target.customer_address_dim_key != source.customer_address_dim_key or 
        target.restaurant_dim_key != source.restaurant_dim_key or
        target.restaurant_location_dim_key != source.restaurant_location_dim_key or
        target.menu_dim_key != source.menu_dim_key or
        target.delivery_agent_dim_key != source.delivery_agent_dim_key or
        target.order_date_dim_key != source.order_date_dim_key or
        target.quantity != source.quantity or
        target.price != source.price or
        target.subtotal != source.subtotal or
        target.delivery_status != source.delivery_status or
        target.estimated_time != source.estimated_time
) THEN 
    UPDATE SET
         target.customer_dim_key = source.customer_dim_key,
        target.customer_address_dim_key = source.customer_address_dim_key,
        target.restaurant_dim_key = source.restaurant_dim_key,
        target.restaurant_location_dim_key = source.restaurant_location_dim_key,
        target.menu_dim_key = source.menu_dim_key,
        target.delivery_agent_dim_key = source.delivery_agent_dim_key,
        target.order_date_dim_key = source.order_date_dim_key,
        target.quantity = source.quantity,
        target.price = source.price,
        target.subtotal = source.subtotal,
        target.delivery_status = source.delivery_status,
        target.estimated_time = source.estimated_time
WHEN NOT MATCHED THEN
    -- Insert new fact record
    INSERT (
    order_item_id,
        order_id,
        customer_dim_key,
        customer_address_dim_key,
        restaurant_dim_key,
        restaurant_location_dim_key,
        menu_dim_key,
        delivery_agent_dim_key,
        order_date_dim_key,
        quantity,
        price,
        subtotal,
        delivery_status,
        estimated_time
    )
    VALUES (
        source.order_item_id,
        source.order_id,
        source.customer_dim_key,
        source.customer_address_dim_key,
        source.restaurant_dim_key,
        source.restaurant_location_dim_key,
        source.menu_dim_key,
        source.delivery_agent_dim_key,
        source.order_date_dim_key,
        source.quantity,
        source.price,
        source.subtotal,
        source.delivery_status,
        source.estimated_time

);*/

select * from CONSUMPTION_SCH.order_item_fact
where order_id=6 
