{{ config (
    alias = target.database + '_blended'
)}}

{%- set date_granularity_list = ['day','week','month','quarter','year'] -%}

WITH 
    shareasale_initial AS (
        SELECT *, {{ get_date_parts('date') }}
        FROM {{ source('gsheet_raw', 'shareasale_spend') }}
    ),
    looker_initial AS (
        SELECT *, {{ get_date_parts('date') }}
        FROM {{ source('gsheet_raw', 'looker_data_updated') }}
    ),
    stretch_initial AS (
        SELECT *, {{ get_date_parts('date') }}
        FROM {{ source('gsheet_raw', 'stretch_forecasted_data') }}
    ),
    forecast_initial AS (
        SELECT *, {{ get_date_parts('date') }}
        FROM {{ source('gsheet_raw', 'forecast_data_updated') }}
    ),
    spend AS (
    SELECT 
        date, 
        date_granularity, 
        'Actual'::text AS data_type, 
        SUM(spend) AS spend, 
        SUM(acq_spend) AS acq_spend, 
        SUM(ret_spend) AS ret_spend,
        SUM(impressions) AS impressions,
        SUM(clicks) AS clicks,
        SUM(purchases) AS purchases,
        SUM(revenue) AS revenue
    FROM (
        SELECT 
            date,
            date_granularity,
            spend,
            case when campaign_name !~* 'react' then spend else 0 end as acq_spend,
            case when campaign_name ~* 'react' then spend else 0 end as ret_spend,
            impressions,
            link_clicks AS clicks,
            purchases,
            revenue
        FROM {{ source('reporting', 'facebook_campaign_performance') }}
        
        UNION ALL

        SELECT 
            date,
            date_granularity,
            spend,
            case when campaign_name !~* 'brand' then spend else (case when campaign_name ~* 'brand' then spend end)*0.34 end as acq_spend, 
            case when campaign_name ~* 'brand' then spend*0.66 else 0 end as ret_spend,
            impressions,
            clicks,
            purchases,
            revenue
        FROM {{ source('reporting', 'googleads_campaign_performance') }}
        
        UNION ALL

        SELECT 
            date,
            date_granularity,
            spend,
            spend AS acq_spend,
            0 as ret_spend,
            impressions,
            clicks,
            purchases,
            revenue
        FROM {{ source('reporting', 'pinterest_ad_group_performance') }}
        
        UNION ALL
        
        -- Shareasale data for dates before or on April 28, 2025
        {% for date_granularity in date_granularity_list %}
        SELECT 
            {{ date_granularity }}::date AS date, 
            '{{ date_granularity }}' AS date_granularity, 
            SUM(ft_spend + ret_spend) AS spend, 
            SUM(ft_spend) AS acq_spend, 
            SUM(ret_spend) AS ret_spend,
            0 as impressions,
            0 as clicks,
            0 as purchases,
            0 as revenue
        FROM shareasale_initial
        WHERE {{ date_granularity }}::date <= '2025-04-28'
        GROUP BY 1, 2
        
        {% if not loop.last %}UNION ALL{% endif %}
        {% endfor %}
        
        UNION ALL
        
        -- Impact data for dates after April 28, 2025
        SELECT 
            date, 
            date_granularity, 
            SUM(action_cost) AS spend, 
            SUM(CASE WHEN customer_status = 'New' THEN action_cost ELSE 0 END) AS acq_spend, 
            SUM(CASE WHEN customer_status = 'Existing' THEN action_cost ELSE 0 END) AS ret_spend,
            0 as impressions,
            0 as clicks,
            SUM(actions) as purchases,
            SUM(revenue) as revenue
        FROM {{ source('reporting', 'impact_performance') }}
        WHERE date > '2025-04-28'
        GROUP BY 1, 2
    ) AS combined_spend
    GROUP BY 1, 2, 3
),

purchases AS (
    SELECT 
        date, 
        date_granularity, 
        'Actual'::text AS data_type, 
        SUM(acquisitions) AS acquisitions, 
        SUM(first_time_sales) AS first_time_sales, 
        SUM(repurchase_sales) AS repurchase_sales, 
        SUM(total_sales) AS total_sales, 
        SUM(repurchase_orders) AS repurchase_orders
    FROM (
        {% for date_granularity in date_granularity_list %}
        SELECT 
            {{ date_granularity }}::date AS date, 
            '{{ date_granularity }}' AS date_granularity, 
            SUM(ft_orders) AS acquisitions, 
            SUM(ft_sales) AS first_time_sales, 
            SUM(ret_sales) AS repurchase_sales, 
            SUM(total_sales) AS total_sales, 
            SUM(ret_orders) AS repurchase_orders
        FROM looker_initial
        GROUP BY 1, 2
        
        {% if not loop.last %}UNION ALL{% endif %}
        {% endfor %}
    )
    GROUP BY 1, 2, 3
),

actual_data AS (
    SELECT * 
    FROM spend 
    LEFT JOIN purchases USING (date, date_granularity, data_type)
    ORDER BY date DESC
),

forecast_data AS (
    {% for date_granularity in date_granularity_list %}
    SELECT
        {{ date_granularity }}::date AS date, 
        '{{ date_granularity }}' AS date_granularity, 
        'Goal' AS data_type,
        SUM(spend) AS spend, 
        SUM(acq_spend) AS acq_spend, 
        SUM(spend) - SUM(acq_spend) AS ret_spend, 
        NULL::bigint AS impressions,
        NULL::bigint AS clicks,
        NULL::bigint AS purchases,
        NULL::bigint AS revenue,
        SUM(ft_orders) AS acquisitions, 
        SUM(ft_revenue) AS first_time_sales,
        SUM(ret_revenue) AS repurchase_sales, 
        SUM(ft_revenue + ret_revenue) AS total_sales, 
        SUM(ret_orders) AS repurchase_orders
    FROM forecast_initial
    GROUP BY 1, 2, 3

    {% if not loop.last %}UNION ALL{% endif %}
    {% endfor %}
),

stretch_data AS (
    {% for date_granularity in date_granularity_list %}
    SELECT
        {{ date_granularity }}::date AS date, 
        '{{ date_granularity }}' AS date_granularity, 
        'Stretch' AS data_type,
        SUM(spend) AS spend, 
        SUM(acq_spend) AS acq_spend, 
        SUM(spend) - SUM(acq_spend) AS ret_spend, 
        NULL::bigint AS impressions,
        NULL::bigint AS clicks,
        NULL::bigint AS purchases,
        NULL::bigint AS revenue,
        SUM(ft_orders) AS acquisitions, 
        SUM(ft_revenue) AS first_time_sales,
        SUM(ret_revenue) AS repurchase_sales, 
        SUM(ft_revenue + ret_revenue) AS total_sales, 
        0 AS repurchase_orders
    FROM stretch_initial
    GROUP BY 1, 2, 3

    {% if not loop.last %}UNION ALL{% endif %}
    {% endfor %}
)

SELECT * 
FROM actual_data
WHERE date <= CURRENT_DATE

UNION ALL

SELECT * 
FROM forecast_data
WHERE date <= CURRENT_DATE

UNION ALL

SELECT * 
FROM stretch_data
WHERE date <= CURRENT_DATE

ORDER BY date DESC
