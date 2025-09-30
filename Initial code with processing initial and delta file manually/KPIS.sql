use role sysadmin;
use warehouse adhoc_wh;
use database sandbox;
use schema consumption_sch;

select * from consumption_sch.order_item_fact
where DELIVERY_STATUS='Delivered';

create or replace view consumption_sch.vw_yearly_revenue_kpis as
select
    d.year as year, -- fetch year from date_dim
    sum(fact.subtotal) as total_revenue,
    count(distinct fact.order_id) as total_orders,
    round(sum(fact.subtotal) / count(distinct fact.order_id), 2) as avg_revenue_per_order,
    round(sum(fact.subtotal) / count(fact.order_item_id), 2) as avg_revenue_per_item,
    max(fact.subtotal) as max_order_value
from
    consumption_sch.order_item_fact fact
join
    consumption_sch.date_dim d
on
    fact.order_date_dim_key = d.date_dim_hk -- join fact table with date_dim table
where DELIVERY_STATUS = 'Delivered'
group by
    d.year
order by
    d.year;

select * from  consumption_sch.vw_yearly_revenue_kpis;

-- scd2 safe
CREATE OR REPLACE VIEW consumption_sch.vw_yearly_revenue_kpis2 AS
WITH dedup_order_items AS (
    SELECT 
        order_item_id,
        order_id,
        order_date_dim_key,
        MAX(subtotal) AS item_subtotal  -- pick one version per item
    FROM consumption_sch.order_item_fact
    WHERE DELIVERY_STATUS = 'Delivered'
    GROUP BY order_item_id, order_id, order_date_dim_key
)
SELECT
    d.year AS year,
    SUM(o.item_subtotal) AS total_revenue,
    COUNT(DISTINCT o.order_id) AS total_orders,
    ROUND(SUM(o.item_subtotal) / COUNT(DISTINCT o.order_id), 2) AS avg_revenue_per_order,
    ROUND(SUM(o.item_subtotal) / COUNT(*), 2) AS avg_revenue_per_item,
    MAX(o.item_subtotal) AS max_order_value
FROM dedup_order_items o
JOIN consumption_sch.date_dim d
    ON o.order_date_dim_key = d.date_dim_hk
GROUP BY d.year
ORDER BY d.year;



/*WITH dedup_order_items AS (
    SELECT 
        order_item_id,
        order_id,
        order_date_dim_key,
        MAX(subtotal) AS item_subtotal  -- pick one version per item
    FROM consumption_sch.order_item_fact
    WHERE DELIVERY_STATUS = 'Delivered'
    GROUP BY order_item_id, order_id, order_date_dim_key
)
SELECT
    d.year AS year,
    SUM(o.item_subtotal) AS total_revenue
FROM dedup_order_items o
JOIN consumption_sch.date_dim d
    ON o.order_date_dim_key = d.date_dim_hk
GROUP BY d.year
ORDER BY d.year;*/



/*SELECT
    d.year AS year,
    SUM(fact.subtotal) AS total_revenue
FROM consumption_sch.order_item_fact fact
JOIN consumption_sch.date_dim d
    ON fact.order_date_dim_key = d.date_dim_hk
WHERE DELIVERY_STATUS = 'Delivered'
GROUP BY d.year
ORDER BY d.year;*/





select * from consumption_sch.vw_yearly_revenue_kpis2;

CREATE OR REPLACE VIEW consumption_sch.vw_monthly_revenue_kpis AS
SELECT
    d.YEAR AS year,                       -- Fetch year from DATE_DIM
    d.MONTH AS month,                     -- Fetch month from DATE_DIM
    SUM(fact.subtotal) AS total_revenue,
    COUNT(DISTINCT fact.order_id) AS total_orders,
    ROUND(SUM(fact.subtotal) / COUNT(DISTINCT fact.order_id), 2) AS avg_revenue_per_order,
    ROUND(SUM(fact.subtotal) / COUNT(fact.order_item_id), 2) AS avg_revenue_per_item,
    MAX(fact.subtotal) AS max_order_value
FROM
    consumption_sch.order_item_fact fact
JOIN
    consumption_sch.DATE_DIM d
ON
    fact.order_date_dim_key = d.DATE_DIM_HK -- Join fact table with DATE_DIM table
where DELIVERY_STATUS = 'Delivered'
GROUP BY
    d.YEAR, d.MONTH
ORDER BY
    d.YEAR, d.MONTH;

/*WITH dedup_order_items AS (
    SELECT 
        order_item_id,
        order_id,
        order_date_dim_key,
        MAX(subtotal) AS item_subtotal
    FROM consumption_sch.order_item_fact
    WHERE DELIVERY_STATUS = 'Delivered' AND order_id = 14
    GROUP BY order_item_id, order_id, order_date_dim_key
)
SELECT
    d.year AS year,
    SUM(o.item_subtotal) AS total_revenue,
    COUNT(DISTINCT o.order_id) AS total_orders,
    ROUND(SUM(o.item_subtotal) / COUNT(DISTINCT o.order_id), 2) AS avg_revenue_per_order,
    ROUND(SUM(o.item_subtotal) / COUNT(*), 2) AS avg_revenue_per_item,
    MAX(o.item_subtotal) AS max_order_value
FROM dedup_order_items o
JOIN consumption_sch.date_dim d
    ON o.order_date_dim_key = d.date_dim_hk
GROUP BY d.year
ORDER BY d.year;*/


select * from consumption_sch.vw_monthly_revenue_kpis;

CREATE OR REPLACE VIEW consumption_sch.vw_daily_revenue_kpis AS
SELECT
    d.YEAR AS year,                       -- Fetch year from DATE_DIM
    d.MONTH AS month,                     -- Fetch month from DATE_DIM
    d.DAY_OF_THE_MONTH AS day,            -- Fetch day from DATE_DIM
    SUM(fact.subtotal) AS total_revenue,
    COUNT(DISTINCT fact.order_id) AS total_orders,
    ROUND(SUM(fact.subtotal) / COUNT(DISTINCT fact.order_id), 2) AS avg_revenue_per_order,
    ROUND(SUM(fact.subtotal) / COUNT(fact.order_item_id), 2) AS avg_revenue_per_item,
    MAX(fact.subtotal) AS max_order_value
FROM
    consumption_sch.order_item_fact fact
JOIN
    consumption_sch.DATE_DIM d
ON
    fact.order_date_dim_key = d.DATE_DIM_HK -- Join fact table with DATE_DIM table
    where DELIVERY_STATUS = 'Delivered'
GROUP BY
    d.YEAR, d.MONTH, d.DAY_OF_THE_MONTH     -- Group by year, month, and day
ORDER BY
    d.YEAR, d.MONTH, d.DAY_OF_THE_MONTH;    -- Order by year, month, and day



select * from consumption_sch.vw_daily_revenue_kpis;


CREATE OR REPLACE VIEW consumption_sch.vw_day_revenue_kpis AS
SELECT
    d.YEAR AS year,                       -- Fetch year from DATE_DIM
    d.MONTH AS month,                     -- Fetch month from DATE_DIM
    d.DAY_NAME AS DAY_NAME,                -- Fetch day from DATE_DIM-DAY_NAME
    SUM(fact.subtotal) AS total_revenue,
    COUNT(DISTINCT fact.order_id) AS total_orders,
    ROUND(SUM(fact.subtotal) / COUNT(DISTINCT fact.order_id), 2) AS avg_revenue_per_order,
    ROUND(SUM(fact.subtotal) / COUNT(fact.order_item_id), 2) AS avg_revenue_per_item,
    MAX(fact.subtotal) AS max_order_value
FROM
    consumption_sch.order_item_fact fact
JOIN
    consumption_sch.DATE_DIM d
ON
    fact.order_date_dim_key = d.DATE_DIM_HK -- Join fact table with DATE_DIM table
GROUP BY
    d.YEAR, d.MONTH, d.DAY_NAME     -- Group by year, month, and day
ORDER BY
    d.YEAR, d.MONTH, d.DAY_NAME;    -- Order by year, month, and day

select * from consumption_sch.vw_day_revenue_kpis;

CREATE OR REPLACE VIEW consumption_sch.vw_monthly_revenue_by_restaurant AS
SELECT
    d.YEAR AS year,                       -- Fetch year from DATE_DIM
    d.MONTH AS month,                     -- Fetch month from DATE_DIM
    fact.DELIVERY_STATUS,
    r.name as restaurant_name,
    SUM(fact.subtotal) AS total_revenue,
    COUNT(DISTINCT fact.order_id) AS total_orders,
    ROUND(SUM(fact.subtotal) / COUNT(DISTINCT fact.order_id), 2) AS avg_revenue_per_order,
    ROUND(SUM(fact.subtotal) / COUNT(fact.order_item_id), 2) AS avg_revenue_per_item,
    MAX(fact.subtotal) AS max_order_value
FROM
    consumption_sch.order_item_fact fact
JOIN
    consumption_sch.DATE_DIM d
ON
    fact.order_date_dim_key = d.DATE_DIM_HK 
JOIN
    consumption_sch.restaurant_dim r
ON
    fact.restaurant_dim_key = r.RESTAURANT_HK 
GROUP BY
    d.YEAR, d.MONTH,fact.DELIVERY_STATUS,restaurant_name
ORDER BY
    d.YEAR, d.MONTH;

select * from consumption_sch.vw_monthly_revenue_by_restaurant;




select * from consumption_sch.customer_address_dim;


CREATE OR REPLACE VIEW consumption_sch.vv_revenue_by_state as 
select cd.CITY as city,
        d.YEAR as  year,-- fetch year from date_dim
    sum(fact.subtotal) as total_revenue,
    count(distinct fact.order_id) as total_orders,
    round(sum(fact.subtotal) / count(distinct fact.order_id), 2) as avg_revenue_per_order,
    round(sum(fact.subtotal) / count(fact.order_item_id), 2) as avg_revenue_per_item,
    max(fact.subtotal) as max_order_value
FROM CONSUMPTION_SCH.order_item_fact fact
join consumption_sch.date_dim d 
on fact.order_date_dim_key = d.DATE_DIM_HK
join consumption_sch.customer_address_dim cd
on fact.customer_address_dim_key=cd.CUSTOMER_ADDRESS_HK
GROUP BY cd.CITY, d.YEAR
ORDER BY d.YEAR, cd.CITY;


select * from consumption_sch.order_item_fact;


/*select m.category, count(distinct o.order_id) as order_count, SUM(o.subtotal) AS total_revenue  from consumption_sch.menu_dim as m
join consumption_sch.order_item_fact as o
on m.menu_dim_hk=o.menu_dim_key
where o.DELIVERY_STATUS = 'Delivered'
group by m.category
order by order_count desc*/




/*WITH order_totals AS (
    SELECT order_id, menu_dim_key, SUM(subtotal) AS order_total
    FROM consumption_sch.order_item_fact
    WHERE DELIVERY_STATUS = 'Delivered'
    GROUP BY order_id, menu_dim_key
)
SELECT m.category,
       COUNT(DISTINCT o.order_id) AS order_count,
       SUM(o.order_total) AS total_revenue
FROM consumption_sch.menu_dim m
JOIN order_totals o
    ON m.menu_dim_hk = o.menu_dim_key
GROUP BY m.category
ORDER BY order_count DESC;*/
