{{ config (
    alias = target.database + '_blended'
)}}

with
paid_data as
    (SELECT channel, campaign_id::varchar as campaign_id, campaign_name, date::date, date_granularity, COALESCE(SUM(spend),0) as spend, COALESCE(SUM(clicks),0) as clicks, 
        COALESCE(SUM(impressions),0) as impressions, COALESCE(SUM(add_to_cart),0) as add_to_cart, COALESCE(SUM(paid_purchases),0) as paid_purchases, COALESCE(SUM(paid_revenue),0) as paid_revenue, 
        0 as shopify_first_orders, 0 as shopify_orders, 0 as shopify_first_sales, 0 as shopify_sales, 0 as shopify_first_net_sales, 0 as shopify_net_sales
    FROM
        (SELECT 'Meta' as channel, campaign_id, campaign_name, date, date_granularity, 
            spend, link_clicks as clicks, impressions, add_to_cart, purchases as paid_purchases, revenue as paid_revenue
        FROM {{ source('reporting','facebook_ad_performance') }}
        UNION ALL
        SELECT 'Google Ads' as channel, campaign_id, campaign_name, date, date_granularity,
            spend, clicks, impressions, add_to_cart, purchases as paid_purchases, revenue as paid_revenue
        FROM {{ source('reporting','googleads_campaign_performance') }}
		UNION ALL
		SELECT 'Tiktok Ads' as channel, campaign_id, campaign_name, date, date_granularity, 
			spend, clicks, impressions, atc as add_to_cart, purchases as paid_purchases, revenue as paid_revenue
        FROM {{ source('reporting','tiktok_campaign_performance') }}
		UNION ALL
		SELECT 'Tiktok Ads' as channel, campaign_id, campaign_name, date_trunc('day',stat_time_day::date)::date as date, 'day' as date_granularity, 
			spend, 0 as clicks, 0 as impressions, 0 as add_to_cart, 0 as paid_purchases, 0 as paid_revenue
        FROM {{ source('tiktok_raw','tiktok_gmv_campaign_performance') }}
        UNION ALL
		SELECT 'Tiktok Ads' as channel, campaign_id, campaign_name, date_trunc('week',stat_time_day::date)::date as date, 'week' as date_granularity, 
			spend, 0 as clicks, 0 as impressions, 0 as add_to_cart, 0 as paid_purchases, 0 as paid_revenue
        FROM {{ source('tiktok_raw','tiktok_gmv_campaign_performance') }}
        UNION ALL
		SELECT 'Tiktok Ads' as channel, campaign_id, campaign_name, date_trunc('month',stat_time_day::date)::date as date, 'month' as date_granularity, 
			spend, 0 as clicks, 0 as impressions, 0 as add_to_cart, 0 as paid_purchases, 0 as paid_revenue
        FROM {{ source('tiktok_raw','tiktok_gmv_campaign_performance') }}
        UNION ALL
		SELECT 'Tiktok Ads' as channel, campaign_id, campaign_name, date_trunc('quarter',stat_time_day::date)::date as date, 'quarter' as date_granularity, 
			spend, 0 as clicks, 0 as impressions, 0 as add_to_cart, 0 as paid_purchases, 0 as paid_revenue
        FROM {{ source('tiktok_raw','tiktok_gmv_campaign_performance') }}
        UNION ALL
		SELECT 'Tiktok Ads' as channel, campaign_id, campaign_name, date_trunc('year',stat_time_day::date)::date as date, 'year' as date_granularity, 
			spend, 0 as clicks, 0 as impressions, 0 as add_to_cart, 0 as paid_purchases, 0 as paid_revenue
        FROM {{ source('tiktok_raw','tiktok_gmv_campaign_performance') }}
        UNION ALL
        SELECT 'Bing' as channel, campaign_id, campaign_name, date, date_granularity, 
            spend, clicks, impressions, 0 as add_to_cart, purchases as paid_purchases, revenue as paid_revenue
        FROM {{ source('reporting','bingads_campaign_performance') }}
        )
    GROUP BY channel, campaign_id, campaign_name, date, date_granularity),

ga4_data as 
    (SELECT campaign_id::varchar as campaign_id, date, date_granularity, 
    sum(sessions) as sessions, sum(engaged_sessions) as engaged_sessions, sum(purchase) as ga4_purchases, sum(purchase_value) as ga4_revenue
    FROM {{ source('reporting','ga4_campaign_performance') }}
    GROUP BY 1,2,3),

paid_ga4_data as (
  SELECT 
    case when campaign_name is null then 'Not Paid' else channel end as channel, campaign_name, date::date, date_granularity,
    SUM(COALESCE(spend, 0)) AS spend,
    SUM(COALESCE(clicks, 0)) AS clicks,
    SUM(COALESCE(impressions, 0)) AS impressions,
	SUM(COALESCE(add_to_cart, 0)) AS add_to_cart,
    SUM(COALESCE(paid_purchases, 0)) AS paid_purchases,
    SUM(COALESCE(paid_revenue, 0)) AS paid_revenue,
    SUM(COALESCE(shopify_first_orders, 0)) AS shopify_first_orders,
    SUM(COALESCE(shopify_orders, 0)) AS shopify_orders,
    SUM(COALESCE(shopify_first_sales, 0)) AS shopify_first_sales,
    SUM(COALESCE(shopify_sales, 0)) AS shopify_sales,
    SUM(COALESCE(shopify_first_net_sales, 0)) AS shopify_first_net_sales,
    SUM(COALESCE(shopify_net_sales, 0)) AS shopify_net_sales,
    SUM(COALESCE(sessions, 0)) AS sessions,
    SUM(COALESCE(engaged_sessions, 0)) AS engaged_sessions,
    SUM(COALESCE(ga4_purchases, 0)) AS ga4_purchases,
    SUM(COALESCE(ga4_revenue, 0)) AS ga4_revenue
  FROM paid_data FULL OUTER JOIN ga4_data USING(date,date_granularity,campaign_id)
  GROUP BY 1,2,3,4),

sho_data as
    (SELECT
            'Shopify' as channel,
            '(not set)' as campaign_name,
            date,
            date_granularity,
            0 as spend,
            0 as clicks,
            0 as impressions,
			0 as add_to_cart,
            0 as paid_purchases,
            0 as paid_revenue, 
            first_orders as shopify_first_orders, 
            orders as shopify_orders, 
            first_order_gross_sales as shopify_first_sales, 
            gross_sales as shopify_sales,
            first_order_net_sales as shopify_first_net_sales,
            net_sales as shopify_net_sales,
            0 as sessions,
            0 as engaged_sessions,
            0 as ga4_purchases,
            0 as ga4_revenue 
    	FROM {{ source('reporting','shopify_sales') }}
        GROUP BY channel, date, campaign_name, date_granularity
    )
    
SELECT 
    channel,
    campaign_name,
    date,
    date_granularity,
    spend,
    clicks,
    impressions,
	add_to_cart,
    paid_purchases,
    paid_revenue,
    shopify_first_orders,
    shopify_orders,
    shopify_first_sales,
    shopify_sales,
    shopify_first_net_sales,
    shopify_net_sales,
    sessions,
    engaged_sessions,
    ga4_purchases,
    ga4_revenue 
FROM (
    SELECT * FROM paid_ga4_data
    UNION ALL 
    SELECT * FROM sho_data
)
