/*
That's the logic of dataset for its further visualization. 
Please find some examples of diagrams in the repository attachment. 

The issue: we need to understand the inflation rate in aggregation by:
- items;
- brands;
- regions.
The ratio from start of the year is required as well as the week over week dynamics. 

The schema logic is provided below.

1) table schema_name.orders: table with all orders 
____________________________________________
order_id                        | int PK    |
brand_id                        | int       |
brand_name                      | string    |
region_name                     | string    |
country_name                    | string    |
order_latest_revision_id        | int       |
brand_type                      | string    |
brand_business_type             | string    |
currency_rate                   | double    |
order_place_cost                | double    |
utc_confirmed_dttm              | string    |
utc_cancelled_dttm              | string    |
utc_created_dttm                | string    |
____________________________________________

2) table schema_name.brands_logic: key brands segmentation 
____________________________________________
brand_id                        | int PK    |
brand_segment                   | string    |
____________________________________________

3) table schema_name.order_revision: each order has at least one revesion before it finally would be confirmed or declined
____________________________________________
id                              | int PK    |
composition_id                  | int       |
____________________________________________

4) table schema_name.order_revision_item: each order consists of several items (at least one)
____________________________________________
composition_id                  | int PK    |
name                            | string    |
item_price_rational             | string    |
____________________________________________

*/

$datetime_parse = DateTime::Parse("%Y-%m-%d %H:%M:%S");

with orders as 
(
    select  
    brand_id
    , brand_name
    , region_name
    , country_name
    , order_id
    , order_latest_revision_id
    , brand_type 
    , brand_business_type
    , currency_rate
    , coalesce(order_place_cost, 0) * currency_rate as order_payment
    , DateTime::MakeDate( $datetime_parse ( coalesce(utc_confirmed_dttm, utc_cancelled_dttm, utc_created_dttm) ) ) as order_date

    from schema_name.orders
    where order_status = True 
), 

brands as 
(
    select 
    o.brand_id as brand_id  
    , o.brand_name as brand_name
    , o.region_name as region_name
    , o.brand_type as brand_type
    , o.brand_business_type as brand_business_type
    , bl.brand_segment as brand_segment
    , count(o.order_id) as overall_orders_per_brand_in_region
    , Math::Round( sum(coalesce(o.order_payment, 0)) ) as overall_gmv_per_brand_in_region

    from orders as o 
    left join schema_name.brands_logic as bl 
    on o.brand_id = bl.brand_id

    group by 
    o.brand_id 
    , o.brand_name 
    , bl.brand_segment
    , o.brand_type
    , o.brand_business_type
    , o.region_name 
), 

items as 
(
    select distinct 
    o.brand_id as brand_id
    , o.brand_name as brand_name
    , o.region_name as region_name
    , o.country_name as country_name
    , o.order_id as order_id
    , o.order_payment as order_payment
    , o.order_date as order_date

    , coalesce(rev.item_price_rational, 0) * o.currency_rate as item_price 
    , rev.name as item_name 

    from $orders as o 

    left join schema_name.order_revision as orev 
    on o.order_latest_revision_id = orev.id

    left join schema_name.order_revision_item as rev 
    on rev.composition_id = orev.composition_id
), 

items_price as 
(
    select 
    brand_id 
    , brand_name 
    , region_name 
    , country_name
    , order_date 
    , item_name
    , Math::Round(sum(item_price) / count(item_name)) as w_avg_item_price
    , avg(item_price) as avg_item_price 
    , count (item_name) as items_cnt 

    from items 

    group by 
    brand_id 
    , brand_name 
    , region_name 
    , country_name
    , order_date 
    , item_name
),

--here we define which items should be a part of final segment due to the condition of more than half of total brand GMV in region per all observation time
items_sample as 
(
    select distinct 
    brand_id
    , brand_name
    , brand_type
    , brand_business_type
    , brand_segment
    , region_name
    , item_name
    , items_rate
    , sum (items_rate) over w as window_sum 

    from 
    (
        select distinct 
        brand_id
        , brand_name
        , brand_type
        , brand_business_type
        , brand_segment
        , region_name
        , item_name
        , Math::Round( (item_price / overall_gmv_per_brand_in_region), -4) as items_rate
        from 
        (
            select 
            b.brand_id as brand_id
            , b.brand_name as brand_name
            , b.brand_type as brand_type
            , b.brand_business_type as brand_business_type
            , b.brand_segment as brand_segment
            , b.region_name as region_name
            , b.overall_gmv_per_brand_in_region as overall_gmv_per_brand_in_region

            , i.item_name as item_name 
            , sum(i.item_price) as item_price

            from brands as b 
            left join items as i 
            on b.brand_id = i.brand_id and b.region_name = i.region_name
            group by 
            b.brand_id 
            , b.brand_name
            , b.brand_type
            , b.brand_business_type
            , b.brand_segment
            , b.region_name
            , b.overall_gmv_per_brand_in_region 
            , i.item_name 
        )
    )
    window w as 
    (
        partition by 
        region_name
        , brand_id 
        , brand_name 
        , brand_type
        , brand_business_type
        , brand_segment
        order by 
        region_name
        , items_rate desc
    )
),

items_dynamics as 
(
    select distinct 
    ip.brand_id as brand_id
    , ip.brand_name as brand_name
    , i.brand_business_type as brand_business_type
    , i.brand_type as brand_type
    , i.brand_segment as brand_segment
    , ip.region_name as region_name
    , ip.country_name as country_name 
    , ip.order_date as order_date
    , ip.item_name as item_name

    , ip.avg_item_price as avg_item_price
    , ip.w_avg_item_price as w_avg_item_price
    , ip.items_cnt as items_cnt

    , i.items_rate as items_rate
    , i.window_sum as window_sum

    , b.overall_orders_per_brand_in_region as overall_orders_per_brand_in_region
    , b.overall_gmv_per_brand_in_region as overall_gmv_per_brand_in_region

    from items_price as ip 
    inner join items_sample as i
    on ip.brand_id = i.brand_id 
    and ip.brand_name = i.brand_name
    and ip.region_name = i.region_name
    and ip.item_name = i.item_name 

    left join brands as b 
    on ip.brand_id = b.brand_id 
    and ip.brand_name = b.brand_name
    and ip.region_name = b.region_name

    where window_sum <= 0.5
)

/*
Finally we are going to use via BI tool the equivalent of the following logic based on this dataset:

    , sum (infl_from_start * items_rate_sample) as w_infl_from_start
    from
    ...
        , (items_cnt * w_avg_item_price) / (sum (items_cnt * w_avg_item_price) over (partition by brand_id, brand_name, week_number) ) as items_rate_sample
*/

select 
brand_id
, brand_name
, brand_business_type
, brand_type
, brand_segment
, region_name
, country_name
, items_rate
, overall_gmv_per_brand_in_region
, overall_orders_per_brand_in_region
, item_name
, week_number
, items_cnt
, w_avg_item_price
, price_start
, price_prev_week
, Math::Round ( ( (w_avg_item_price - price_start) / price_start ), -4) as infl_from_start 
, Math::Round ( ( (w_avg_item_price - price_prev_week) / price_prev_week ), -4) as infl_WoW 
from 
(
    select
    brand_id
    , brand_name
    , region_name
    , country_name
    , brand_business_type
    , brand_type
    , brand_segment
    , items_rate
    , overall_gmv_per_brand_in_region
    , overall_orders_per_brand_in_region
    , item_name
    , week_number
    , items_cnt
    , w_avg_item_price
    , max ( case when week_number = min_order_week then w_avg_item_price else null end ) over (partition by brand_id, brand_name, region_name, item_name) as price_start
    , lag (w_avg_item_price, -1) over (partition by brand_id, brand_name, region_name, item_name order by week_number desc) as price_prev_week
    from 
    (
        select 
        brand_id
        , brand_name
        , region_name
        , country_name
        , brand_business_type
        , brand_type
        , brand_segment        
        , items_rate
        , overall_gmv_per_brand_in_region
        , overall_orders_per_brand_in_region


        , item_name
        , week_number
        , min_order_week

        , sum (items_cnt) as items_cnt
        , sum ( w_avg_item_price * items_cnt ) / sum(items_cnt) as w_avg_item_price
        from 
        (
            select 
            brand_id
            , brand_name
            , region_name
            , country_name
            , brand_business_type
            , brand_type
            , brand_segment

            , order_date
            , DateTime::MakeDate( DateTime::StartOfWeek(order_date) )  as week_number
            , max(DateTime::MakeDate( DateTime::StartOfWeek(order_date) ) ) over (partition by brand_id, brand_name, region_name, item_name) as max_order_week
            , min(DateTime::MakeDate( DateTime::StartOfWeek(order_date) ) ) over (partition by brand_id, brand_name, region_name, item_name) as min_order_week

            , w_avg_item_price
            , item_name
            , items_cnt

            , items_rate
            , overall_gmv_per_brand_in_region
            , overall_orders_per_brand_in_region

            from items_dynamics
        )
        group by 
        brand_id
        , brand_name
        , brand_business_type
        , brand_type
        , brand_segment
        , region_name
        , country_name
        , items_rate
        , overall_gmv_per_brand_in_region
        , overall_orders_per_brand_in_region
        , item_name
        , week_number
        , min_order_week
    )
)

