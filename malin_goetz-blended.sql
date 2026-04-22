{{ config (
    alias = target.database + '_blended'
)}}

WITH spend_data as
    (SELECT date::date, date_granularity, COALESCE(SUM(acquisition_spend),0) as acquisition_spend, COALESCE(SUM(spend),0) as spend, COALESCE(SUM(clicks),0) as clicks, COALESCE(SUM(impressions),0) as impressions, 
        COALESCE(SUM(revenue),0) as revenue, COALESCE(SUM(purchases),0) as paid_purchases
    FROM
        (SELECT date, date_granularity, 
            case when (campaign_type_default ~* 'prospecting' or campaign_type_default ~* 'retargeting') then spend else 0 end as acquisition_spend,
            spend, link_clicks as clicks, impressions, revenue, purchases
        FROM {{ source('reporting','facebook_ad_performance') }}
        WHERE campaign_country = 'US'
        UNION ALL
        SELECT date, date_granularity,
            0 as acquisition_spend,
            spend, clicks, impressions, revenue, purchases
        FROM {{ source('reporting','googleads_campaign_performance') }}
        WHERE campaign_country = 'US'
        UNION ALL
        SELECT date, date_granularity, 
            0 as acquisition_spend,
            spend, clicks, impressions, revenue, purchases
        FROM {{ source('reporting','bingads_campaign_performance') }})
    GROUP BY date, date_granularity),

    ga_data as 
    (SELECT date::date, date_granularity, sum(sessions) as traffic
    FROM {{ source('reporting','googleanalytics_performance_by_sourcemedium') }}
    WHERE profile = '103426173'
    GROUP BY date, date_granularity),

    gcs_data as 
    (SELECT dg::date as date, date_granularity,
    sum(case when order_type = 'new' then revenue end) as new_revenue,
    sum(case when order_type = 'recurring' then revenue end) as returning_revenue,
    sum(case when order_type = 'new' then orders end) as new_purchases,
    sum(case when order_type = 'recurring' then orders end) as returning_purchases
    from {{ source('reporting','gcs_daily_performance_us') }}
    group by date, date_granularity)
    
SELECT date,
    date_granularity,
    spend,
    acquisition_spend,
    clicks,
    impressions,
    revenue,
    paid_purchases,
    traffic,
    new_revenue,
    returning_revenue,
    new_purchases,
    returning_purchases
FROM {{ source('utilities','dates') }}
LEFT JOIN spend_data USING (date)
LEFT JOIN ga_data USING (date,date_granularity)
LEFT JOIN gcs_data USING (date,date_granularity)
WHERE date <= current_date
