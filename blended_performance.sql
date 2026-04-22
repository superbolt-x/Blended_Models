{{ config (
    alias = target.database + '_blended_performance'
)}}

WITH 
sho_data AS (
	SELECT 
	date_granularity,
    date,
	'Actual' as data_type,
	0 as spend,
	orders as shopify_orders, 
	first_orders as shopify_first_orders,
	total_net_sales as shopify_sales,
	first_order_total_net_sales as shopify_first_sales,
	subtotal_refund - shipping_refunds + tax_refunds as shopify_refund,
	first_order_subtotal_refund - first_order_shipping_refunds + first_order_tax_refunds as shopify_first_refund
    FROM {{ source('reporting','shopify_sales') }}
           

, actual_data as (select 
    date,
    date_granularity,
    data_type,
    sum(spend) as original_spend,
    sum(spend) as spend,
    sum(first_orders) as first_orders,
    sum(returning_orders) as returning_orders,
    sum(subtotal_sales) as total_sales,
	sum(first_subtotal_sales) as first_total_sales
from 
(select date, date_granularity, 'Actual' as data_type, sum(spend) as spend, 0 as first_orders, 0 as returning_orders, 0 as subtotal_sales, 0 as first_subtotal_sales 
from {{ source('reporting', 'facebook_ad_performance') }}
where campaign_name != '[SB] Prospecting - Advantage+ - Video Testing'
group by 1,2,3
union all 
select date, date_granularity, 'Actual' as data_type, sum(spend) as spend, 0 as first_orders, 0 as returning_orders, 0 as subtotal_sales, 0 as first_subtotal_sales 
from {{ source('reporting', 'googleads_campaign_performance') }}
group by 1,2,3
union all 
select date, date_granularity, data_type, spend, shopify_first_orders as first_orders, 
    coalesce(shopify_orders,0) - coalesce(shopify_first_orders,0) as returning_orders, 
    coalesce(shopify_sales,0) - coalesce(shopify_refund,0)  as subtotal_sales,
	coalesce(shopify_first_sales,0) - coalesce(shopify_first_refund,0)  as first_subtotal_sales
from sho_data)
group by 1,2,3)

, forcasted_data as (
{%- for date_granularity in date_granularity_list %}
    select date_trunc('{{date_granularity}}',date)::date as date, '{{date_granularity}}' as date_granularity, 'Forecasted' as data_type, 
    sum(original_spend) as original_spend, sum(original_spend) as spend, sum(acquisitions) as first_orders, sum(returning) as returning, sum(revenue) as total_sales,
	sum(0) as first_total_sales
    from {{ source('gsheet_raw', 'forecast_data') }}
    group by 1,2,3
    {%- if not loop.last %}union all{%- endif %}
{% endfor %}
)

, adjusted_forcasted_data as (
{%- for date_granularity in date_granularity_list %}
    select date_trunc('{{date_granularity}}',date)::date as date, '{{date_granularity}}' as date_granularity, 'Adjusted Forecasted' as data_type, 
    sum(adjusted_spend) as original_spend, sum(adjusted_spend) as spend, sum(adjusted_acquisitions) as first_orders, sum(adjusted_returning) as returning, 
	sum(adjusted_revenue) as total_sales, sum(0) as first_total_sales
    from {{ source('gsheet_raw', 'forecast_data') }}
    group by 1,2,3
    {%- if not loop.last %}union all{%- endif %}
{% endfor %}
)

select * from actual_data 
union all
select * from forcasted_data 
union all
select * from adjusted_forcasted_data 
order by date desc
