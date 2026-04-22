{{ config (
    alias = target.database + '_blended'
)}}

WITH spend AS 
    (SELECT region, date, COALESCE(SUM(spend),0) as spend, COALESCE(SUM(paid_spend),0) as paid_spend, COALESCE(SUM(paid_revenue),0) as paid_revenue, COALESCE(SUM(paid_purchases),0) as paid_purchases
    FROM 
        (SELECT date::date as date,spend, 
        case when  campaign_name !~* 'amazon' and campaign_name !~* 'view content'
        and campaign_name not in (
            '[SB] Prospecting - AU - BFCM Early Access - Lead Generation - Evergreen - 2024',
            '[SB] Prospecting - AU - Reminder Ads - All Products - BFCM 2025',
            '[SB] Prospecting - US - BFCM Early Access - Lead Generation - Evergreen - 2024',
            '[SB] Prospecting - US - Reminder Ads - All Products - BFCM 2024'
        )
        then spend else 0 end as paid_spend,
        revenue as paid_revenue, purchases as paid_purchases, campaign_type_custom as region
        FROM  reporting.happiestbaby_facebook_ad_performance
        WHERE date_granularity = 'day' 
        and campaign_name !~* 'corporate rental'
        and (campaign_type_custom = 'US' or campaign_type_custom = 'AUS')
        UNION ALL 
        SELECT date::date as date, spend,
        case when campaign_name !~* 'demand gen' then spend else 0 end as paid_spend,
        revenue as paid_revenue, purchases as paid_purchases, campaign_type_custom as region
        FROM reporting.happiestbaby_googleads_campaign_performance
        WHERE date_granularity = 'day'
        and campaign_name !~* 'corporate rental'
        and (campaign_type_custom = 'US' or campaign_type_custom = 'AUS')
        UNION ALL 
        SELECT date::date as date, spend, 
        case when campaign_name !~* 'traffic' then spend else 0 end as paid_spend,
        revenue as paid_revenue, purchases as paid_purchases, campaign_type_custom as region
        FROM reporting.happiestbaby_tiktok_ad_performance 
        WHERE date_granularity = 'day'
        and campaign_name !~* 'corporate rental'
        and (campaign_type_custom = 'US' or campaign_type_custom = 'AUS')
        UNION ALL
        SELECT date::date as date,spend, spend as paid_spend, revenue as paid_revenue, purchases as paid_purchases, campaign_type_custom as region
        FROM  reporting.happiestbaby_facebook_ad_performance
        WHERE date_granularity = 'day' 
        and (campaign_type_custom = 'EU' or campaign_type_custom = 'UK')
        UNION ALL 
        SELECT date::date as date, spend, spend as paid_spend, revenue as paid_revenue, purchases as paid_purchases, campaign_type_custom as region
        FROM reporting.happiestbaby_googleads_campaign_performance
        WHERE date_granularity = 'day'
        and (campaign_type_custom = 'EU' or campaign_type_custom = 'UK')
        UNION ALL 
        SELECT date::date as date, spend, spend as paid_spend, revenue as paid_revenue, purchases as paid_purchases, campaign_type_custom as region
        FROM reporting.happiestbaby_tiktok_ad_performance 
        WHERE date_granularity = 'day'
        and (campaign_type_custom = 'EU' or campaign_type_custom = 'UK')
        UNION ALL
        SELECT date::date as date, spend, spend as paid_spend, revenue as paid_revenue, purchases as paid_purchases, campaign_type_custom as region
        FROM reporting.happiestbaby_pinterest_ad_group_performance 
        WHERE date_granularity = 'day'
        and (campaign_type_custom = 'US' or campaign_type_custom = 'AUS' or campaign_type_custom = 'UK')
        UNION ALL
        SELECT date::date as date, spend, spend as paid_spend, revenue as paid_revenue, purchases as paid_purchases, campaign_type_custom as region
        FROM reporting.happiestbaby_bingads_campaign_performance
        WHERE date_granularity = 'day'
        AND campaign_type_custom = 'US'
        UNION ALL
        SELECT date::date as date, spend, spend as paid_spend, revenue as paid_revenue, purchases as paid_purchases, campaign_type_custom as region
        FROM reporting.happiestbaby_bingads_campaign_performance
        WHERE date_granularity = 'day'
        AND campaign_type_custom = 'AUS'
        UNION ALL
        SELECT date::date as date, spend::float/1000000 as spend, spend::float/1000000 as paid_spend, 0 as paid_revenue, 0 as paid_purchases, 'US' as region
        FROM reddit_raw.campaign_report)
    WHERE region != 'Other'
    GROUP BY 1,2),
    
    sho_data AS (
    SELECT region,
           date::date as date,
           COALESCE(SUM(orders), 0) as orders,
           COALESCE(SUM(first_orders), 0) as first_orders,
           COALESCE(SUM(revenue), 0) AS revenue,
           COALESCE(SUM(snoo_orders), 0) as snoo_orders,
           COALESCE(SUM(rental_orders), 0) AS rental_orders,
           COALESCE(SUM(pre_loved_orders), 0) AS pre_loved_orders,
           COALESCE(SUM(sleepea_orders), 0) AS sleepea_orders,
           COALESCE(SUM(snoobear_orders), 0) AS snoobear_orders,
           COALESCE(SUM(snoobie_orders), 0) AS snoobie_orders
    FROM (
        SELECT data_1.region AS region,
       data_1.date AS date,
       COALESCE(orders, 0) AS orders,
       COALESCE(first_orders, 0) AS first_orders,
       COALESCE(gross_revenue-total_discounts, 0) AS revenue,
       COALESCE(snoo_orders, 0) AS snoo_orders,
       COALESCE(rental_orders, 0) AS rental_orders,
       COALESCE(pre_loved_orders, 0) AS pre_loved_orders,
       COALESCE(sleepea_orders, 0) AS sleepea_orders,
       COALESCE(snoobear_orders, 0) AS snoobear_orders,
       COALESCE(snoobie_orders, 0) AS snoobie_orders
        FROM (SELECT 'EU' AS region, date::date AS date,
                   COUNT(DISTINCT(order_id)) AS orders,
                   COUNT(DISTINCT CASE WHEN customer_order_index = 1 then order_id else null end) AS first_orders,
                   COALESCE(SUM(gross_revenue*(1-coalesce(total_tax/nullif(total_revenue,0),0))), 0) AS gross_revenue,
                   COALESCE(SUM(subtotal_discount*(1-coalesce(total_tax/nullif(total_revenue,0),0))), 0) AS total_discounts
            FROM reporting.happiestbaby_shopify_eu_daily_sales_by_order
            
            GROUP BY 1, 2
        ) AS data_1
        LEFT JOIN (SELECT 'EU' AS region,date,
                        sum(CASE WHEN product_title = 'SNOO Smart Sleeper Baby Cot' THEN quantity ELSE 0 END) AS snoo_orders,
                        sum(CASE WHEN product_title = 'SNOO Smart Sleeper—Rental' THEN quantity ELSE 0 END) AS rental_orders,
                        sum(CASE WHEN product_title = 'SNOO Smart Sleeper—Certified Pre-Loved' THEN quantity ELSE 0 END) AS pre_loved_orders,
                        sum(CASE 
                            WHEN product_title IN ('Sleepea® 5-Second Baby Swaddle','Sleepea® Comforter Swaddle','Sleepea® 5-Second Baby Swaddle') THEN quantity 
                            WHEN product_title = 'Sleepea® Swaddle Sack 3 Pack Bundle' THEN quantity*3
                        ELSE 0 END) AS sleepea_orders,
                        sum(CASE WHEN product_title ~* 'SNOObear' THEN quantity ELSE 0 END) AS snoobear_orders,
                        sum(CASE WHEN product_title ~* 'SNOObie' THEN quantity ELSE 0 END) AS snoobie_orders 
                FROM reporting.happiestbaby_shopify_eu_daily_sales_by_order_line_item
            GROUP BY 1, 2
        ) using(date,region)

        UNION ALL

        SELECT data_2.region AS region,
       data_2.date AS date,
       COALESCE(orders, 0) AS orders,
       COALESCE(first_orders, 0) AS first_orders,
       COALESCE(gross_revenue-total_discounts, 0) AS revenue,
       COALESCE(snoo_orders, 0) AS snoo_orders,
       COALESCE(rental_orders, 0) AS rental_orders,
       COALESCE(pre_loved_orders, 0) AS pre_loved_orders,
       COALESCE(sleepea_orders, 0) AS sleepea_orders,
       COALESCE(snoobear_orders, 0) AS snoobear_orders,
       COALESCE(snoobie_orders, 0) AS snoobie_orders
        FROM (SELECT 'UK' AS region, date::date AS date,
                   COUNT(DISTINCT(order_id)) AS orders,
                   COUNT(DISTINCT CASE WHEN customer_order_index = 1 then order_id else null end) AS first_orders,
                   COALESCE(SUM(gross_revenue*(1-coalesce(total_tax/nullif(total_revenue,0),0))), 0) AS gross_revenue,
                   COALESCE(SUM(subtotal_discount*(1-coalesce(total_tax/nullif(total_revenue,0),0))), 0) AS total_discounts
            FROM reporting.happiestbaby_shopify_uk_daily_sales_by_order
            
            GROUP BY 1, 2
        ) AS data_2
        LEFT JOIN (SELECT 'UK' AS region,date,
                        sum(CASE WHEN product_title = 'SNOO Smart Sleeper Baby Cot' OR product_title = 'SNOO Smart Baby Sleeper' THEN quantity ELSE 0 END) AS snoo_orders,
                        sum(CASE WHEN product_title = 'SNOO Smart Sleeper—Rental' THEN quantity ELSE 0 END) AS rental_orders,
                        sum(CASE WHEN product_title = 'SNOO Smart Sleeper—Certified Pre-Loved' THEN quantity ELSE 0 END) AS pre_loved_orders,
                        sum(CASE 
                            WHEN product_title IN ('Sleepea® 5-Second Baby Swaddle','Sleepea® Comforter Swaddle','Sleepea® 5-Second Baby Swaddle') THEN quantity 
                            WHEN product_title = 'Sleepea® Swaddle Sack 3 Pack Bundle' THEN quantity*3
                        ELSE 0 END) AS sleepea_orders,
                        sum(CASE WHEN product_title ~* 'SNOObear' THEN quantity ELSE 0 END) AS snoobear_orders,
                        sum(CASE WHEN product_title ~* 'SNOObie' THEN quantity ELSE 0 END) AS snoobie_orders 
                FROM reporting.happiestbaby_shopify_uk_daily_sales_by_order_line_item
            GROUP BY 1, 2
        ) using(date,region)

        UNION ALL 

        SELECT data_3.region AS region,
       data_3.date AS date,
       COALESCE(orders, 0) AS orders,
       COALESCE(first_orders, 0) AS first_orders,
       COALESCE(gross_revenue-total_discounts, 0) AS revenue,
       COALESCE(snoo_orders, 0) AS snoo_orders,
       COALESCE(rental_orders, 0) AS rental_orders,
       COALESCE(pre_loved_orders, 0) AS pre_loved_orders,
       COALESCE(sleepea_orders, 0) AS sleepea_orders,
       COALESCE(snoobear_orders, 0) AS snoobear_orders,
       COALESCE(snoobie_orders, 0) AS snoobie_orders
        FROM (SELECT 'US' AS region, date::date AS date,
                   COUNT(DISTINCT(order_id)) AS orders,
                   COUNT(DISTINCT CASE WHEN customer_order_index = 1 then order_id else null end) AS first_orders,
                   COALESCE(SUM(gross_sales), 0) AS gross_revenue,
                   COALESCE(SUM(gross_sales-subtotal_sales), 0) AS total_discounts
            FROM reporting.happiestbaby_shopify_us_daily_sales_by_order_line_item
            
            GROUP BY 1, 2
        ) AS data_3
        LEFT JOIN (SELECT 'US' AS region, date,
                        sum(CASE WHEN product_title = 'SNOO Smart Sleeper Bassinet' THEN quantity ELSE 0 END) AS snoo_orders,
                        sum(CASE WHEN product_title = 'SNOO Smart Sleeper—Rental' THEN quantity ELSE 0 END) AS rental_orders,
                        sum(CASE WHEN product_title = 'SNOO Smart Sleeper—Certified Pre-Loved' THEN quantity ELSE 0 END) AS pre_loved_orders,
                        sum(CASE 
                            WHEN product_title IN ('Sleepea® 5-Second Baby Swaddle','Sleepea® Comforter Swaddle') THEN quantity 
                            WHEN product_title = 'Sleepea® Swaddle Sack 3 Pack Bundle' THEN quantity*3
                        ELSE 0 END) AS sleepea_orders,
                        sum(CASE WHEN product_title ~* 'SNOObear' THEN quantity ELSE 0 END) AS snoobear_orders,
                        sum(CASE WHEN product_title ~* 'SNOObie' THEN quantity ELSE 0 END) AS snoobie_orders
                FROM reporting.happiestbaby_shopify_us_daily_sales_by_order_line_item
                GROUP BY 1, 2
        ) using(date,region)

        UNION ALL 

        SELECT data_4.region AS region,
       data_4.date AS date,
       COALESCE(orders, 0) AS orders,
       COALESCE(first_orders, 0) AS first_orders,
       COALESCE(gross_revenue-total_discounts, 0) AS revenue,
       COALESCE(snoo_orders, 0) AS snoo_orders,
       COALESCE(rental_orders, 0) AS rental_orders,
       COALESCE(pre_loved_orders, 0) AS pre_loved_orders,
       COALESCE(sleepea_orders, 0) AS sleepea_orders,
       COALESCE(snoobear_orders, 0) AS snoobear_orders,
       COALESCE(snoobie_orders, 0) AS snoobie_orders
        FROM (SELECT 'AUS' AS region, date::date AS date,
                   COUNT(DISTINCT(order_id)) AS orders,
                   COUNT(DISTINCT CASE WHEN customer_order_index = 1 then order_id else null end) AS first_orders,
                   COALESCE(SUM(gross_revenue*(1-coalesce(total_tax/nullif(total_revenue,0),0))), 0) AS gross_revenue,
                   COALESCE(SUM(subtotal_discount*(1-coalesce(total_tax/nullif(total_revenue,0),0))), 0) AS total_discounts
            FROM reporting.happiestbaby_shopify_aus_daily_sales_by_order
            
            GROUP BY 1, 2
        ) AS data_4
        LEFT JOIN (SELECT 'AUS' AS region,date,
                        sum(CASE WHEN product_title = 'SNOO Smart Sleeper Bassinet' OR product_title = 'SNOO Smart Baby Sleeper' THEN quantity ELSE 0 END) AS snoo_orders,
                        sum(CASE WHEN product_title = 'SNOO Smart Sleeper—Rental' THEN quantity ELSE 0 END) AS rental_orders,
                        sum(CASE WHEN product_title = 'SNOO Smart Sleeper—Certified Pre-Loved' THEN quantity ELSE 0 END) AS pre_loved_orders,
                        sum(CASE 
                            WHEN product_title IN ('Sleepea® 5-Second Baby Swaddle','Sleepea® Comforter Swaddle') THEN quantity 
                            WHEN product_title = 'Sleepea® Swaddle Sack 3 Pack Bundle' THEN quantity*3
                        ELSE 0 END) AS sleepea_orders,
                        sum(CASE WHEN product_title ~* 'SNOObear' THEN quantity ELSE 0 END) AS snoobear_orders,
                        sum(CASE WHEN product_title ~* 'SNOObie' THEN quantity ELSE 0 END) AS snoobie_orders 
                FROM reporting.happiestbaby_shopify_aus_daily_sales_by_order_line_item
                GROUP BY 1, 2
        ) using(date,region)
    ) AS subquery -- added alias for the subquery
    GROUP BY 1, 2
),

actual_data as (SELECT 'Actual' as type, region, date, spend, paid_spend, paid_revenue, paid_purchases, orders, first_orders, revenue, snoo_orders, rental_orders, pre_loved_orders,
sleepea_orders, snoobear_orders, snoobie_orders
FROM utilities.dates 
LEFT JOIN spend USING(date)
LEFT JOIN sho_data USING(date, region)
WHERE date between '2024-01-01' and current_date),

forecast_data as (
SELECT 'Forecasted' AS type, 'US' AS region, date::date as date, sum(us_spend::float) as spend, sum(us_spend::float) as paid_spend, sum(0) as paid_revenue, sum(0) as paid_purchases, sum(0) as orders, 
    sum(0) as first_orders, sum(us_revenue::float) as revenue, sum(us_snoo::float) as snoo_orders, sum(us_rental::float) as rental_orders, sum(us_pre_loved) as pre_loved_orders, 
    sum(us_sleepea::float) as sleepea_orders, sum(us_white_noise::float/2) as snoobear_orders, sum(us_white_noise::float/2) as snoobie_orders
from gsheet_raw.shopify_forecast
group by 1,2,3
UNION ALL
SELECT 'Forecasted' AS type, 'AUS' AS region, date::date as date, sum(aus_spend::float) as spend, sum(aus_spend::float) as paid_spend, sum(0) as paid_revenue, sum(0) as paid_purchases, 
    sum(0) as orders, sum(0) as first_orders, sum(aus_revenue::float) as revenue, sum(aus_snoo::float) as snoo_orders, sum(us_rental::float) as rental_orders, sum(0) as pre_loved_orders, 
    sum(aus_sleepea::float) as sleepea_orders, sum(aus_snoobear::float) as snoobear_orders, sum(aus_snoobie::float) as snoobie_orders
from gsheet_raw.shopify_forecast
group by 1,2,3
UNION ALL
SELECT 'Forecasted' AS type, 'UK' AS region, date::date as date, sum(uk_spend::float) as spend, sum(uk_spend::float) as paid_spend, sum(0) as paid_revenue, sum(0) as paid_purchases, 
    sum(0) as orders, sum(0) as first_orders, sum(uk_revenue::float) as revenue, sum(uk_snoo::float) as snoo_orders, sum(0) as rental_orders, sum(0) as pre_loved_orders,
    sum(0) as sleepea_orders, sum(0) as snoobear_orders, sum(0) as snoobie_orders
from gsheet_raw.shopify_forecast_eu_uk
group by 1,2,3
UNION ALL
SELECT 'Forecasted' AS type, 'EU' AS region, date::date as date, sum(eu_spend::float) as spend, sum(eu_spend::float) as paid_spend, sum(0) as paid_revenue, sum(0) as paid_purchases, 
    sum(0) as orders, sum(0) as first_orders, sum(eu_revenue::float) as revenue, sum(eu_snoo::float) as snoo_orders, sum(0) as rental_orders, sum(0) as pre_loved_orders, 
    sum(0) as sleepea_orders, sum(0) as snoobear_orders, sum(0) as snoobie_orders
from gsheet_raw.shopify_forecast_eu_uk
group by 1,2,3
)

select * from actual_data 
union all 
select * from forecast_data
order by date desc
