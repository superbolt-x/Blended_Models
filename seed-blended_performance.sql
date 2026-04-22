{{ config (
    alias = target.database + '_blended_performance'
)}}

{% set date_granularity_list = ['day', 'week', 'month', 'quarter', 'year'] %}
  
WITH initial_s3_data as 
    (SELECT *, {{ get_date_parts('activation_date') }} FROM {{ source('s3_raw','lasttouch_performance') }}  WHERE utm_campaign IS NOT NULL),
  
    s3_data as
    ({%- for date_granularity in date_granularity_list %}    
        SELECT '{{date_granularity}}' as date_granularity, {{date_granularity}} as date,
            case when utm_campaign::varchar = 'DS01 - Winback - Manual Campaign - 7DC1DV' then 'DS01 - Winback - Manual Campaign - 7DC' else utm_campaign::varchar end as utm_campaign, 
			google_campaign, bing_campaign,
            utm_content, utm_term,
            CASE WHEN channel ~* 'meta' THEN 'Meta' 
                WHEN channel ~* 'google' THEN 'Google Ads' 
                WHEN channel ~* 'youtube' THEN 'Youtube' 
                WHEN channel ~* 'bing' THEN 'Bing' 
            END as channel_adj,
            CASE WHEN utm_campaign ~* 'INTL' THEN 'INTL' ELSE 'US' END as market,
            CASE WHEN utm_campaign ~* 'DS01' THEN 'DS01'
                WHEN utm_campaign ~* 'VS01' THEN 'VS01'
                WHEN utm_campaign ~* 'PDS08' THEN 'PDS08'
                WHEN utm_campaign ~* 'HCP' THEN 'HCP'
				WHEN utm_campaign ~* 'DM02' THEN 'DM02'
				WHEN utm_campaign ~* 'AM02' THEN 'AM02'
				WHEN utm_campaign ~* 'PM02' THEN 'PM02'
                ELSE 'Other'
            END as product,    
            CASE 
                WHEN google_campaign ~* 'amazon' OR bing_campaign ~* 'amazon' THEN 'Amazon'
                WHEN google_campaign ~* 'demand' OR utm_campaign ~* 'demand' THEN 'Demand Gen'
                WHEN google_campaign ~* 'YT' OR channel ~* 'youtube' THEN 'Youtube'
                WHEN (google_campaign ~* 'Shopping' AND google_campaign ~* 'Brand') OR (bing_campaign ~* 'Shopping' AND bing_campaign ~* 'Brand') THEN 'Shopping - Brand'
                WHEN google_campaign ~* 'Shopping' AND google_campaign !~* 'Brand' THEN 'Shopping - Non Brand'
                WHEN google_campaign ~* 'Performance Max' OR google_campaign ~* 'PMax' THEN 'PMax'
                WHEN google_campaign ~* 'NB' OR bing_campaign ~* 'NB' THEN 'Non Brand'
                WHEN google_campaign ~* 'Brand' OR bing_campaign ~* 'Brand' THEN 'Branded'
                WHEN utm_campaign ~* 'Prospect' OR utm_campaign ~* 'Interest' THEN 'Prospecting'
                WHEN utm_campaign ~* 'Retarget' THEN 'Retargeting'
                ELSE 'Other'
            END as campaign_type,
            COALESCE(SUM(fta_subs),0) as ft_orders, COALESCE(SUM(lta_subs),0) as lt_orders
        FROM initial_s3_data
        LEFT JOIN 
            (SELECT CASE WHEN utm_campaign ~* 'demandgen' THEN 'YOUTUBE' ELSE 'GOOGLE' END as channel, campaign_name::varchar as google_campaign, utm_campaign::varchar
            FROM {{ source('gsheet_raw','utm_campaign_list') }} 
            WHERE channel = 'google' AND utm_campaign IS NOT NULL) USING(channel, utm_campaign)
        LEFT JOIN 
            (SELECT 'BING' as channel, campaign_name::varchar as bing_campaign, utm_campaign::varchar
            FROM {{ source('gsheet_raw','utm_campaign_list') }} 
            WHERE channel = 'bing' AND utm_campaign IS NOT NULL) USING(channel, utm_campaign)
        GROUP BY 1,2,3,4,5,6,7,8,9,10,11
        {% if not loop.last %}UNION ALL
        {% endif %}
    {% endfor %}),
  
    final_data as
    (SELECT channel, date::date, date_granularity, market, product, google_campaign, bing_campaign, utm_campaign, campaign_type, utm_content, utm_term,
        COALESCE(SUM(spend),0) as spend, COALESCE(SUM(impressions),0) as impressions, COALESCE(SUM(clicks),0) as clicks, COALESCE(SUM(checkout_initiated),0) as checkout_initiated, 
        COALESCE(SUM(add_to_cart),0) as add_to_cart, COALESCE(SUM(leads),0) as leads, COALESCE(SUM(purchases),0) as purchases, COALESCE(SUM("VS-01 WK"),0) as "VS-01 WK",
        COALESCE(SUM(revenue),0) as revenue, COALESCE(SUM(ft_orders),0) as ft_orders, COALESCE(SUM(lt_orders),0) as lt_orders
    FROM
        (SELECT 'Meta' as channel, date, date_granularity, null as market, product, null as google_campaign, null as bing_campaign, campaign_name::varchar as utm_campaign, 
            CASE WHEN campaign_name ~* 'Prospect' OR campaign_name ~* 'Interest' THEN 'Prospecting' WHEN campaign_name ~* 'Retarget' THEN 'Retargeting' END as campaign_type,
            adset_name as utm_content, ad_name as utm_term,
            spend, impressions, link_clicks as clicks, 0 as checkout_initiated, add_to_cart, leads, purchases, "VS-01 WK", 0 as revenue, 0 as ft_orders, 0 as lt_orders
        FROM {{ source('reporting','facebook_ad_performance') }}
        UNION ALL
        SELECT 'Google Ads' as channel, gc.date, gc.date_granularity, country as market, product, 
            CASE WHEN campaign_type_custom = 'Amazon' THEN campaign_name::varchar ELSE COALESCE(google_campaign::varchar,campaign_name::varchar) END as google_campaign, null as bing_campaign, utm_campaign::varchar, 
            campaign_type_custom as campaign_type, null as utm_content, null as utm_term,
            COALESCE(SUM(spend),0) as spend, COALESCE(SUM(impressions),0) as impressions, COALESCE(SUM(clicks),0) as clicks, COALESCE(SUM(checkout_initiated),0) as checkout_initiated,
            COALESCE(SUM(add_to_cart),0) as add_to_cart, 0 as leads, COALESCE(SUM(purchases),0) as purchases, 0 as "VS-01 WK", COALESCE(SUM(revenue),0) as revenue, 0 as ft_orders, 0 as lt_orders
        FROM {{ source('reporting','googleads_campaign_performance') }} gc
        LEFT JOIN (SELECT utm_campaign::varchar, google_campaign, COUNT(*) FROM s3_data GROUP BY 1,2) utm ON gc.campaign_name = utm.google_campaign 
        WHERE campaign_type_custom != 'Youtube'
        GROUP BY 1,2,3,4,5,6,7,8,9,10,11
        UNION ALL
        SELECT 'Google Ads' as channel, yt.date, yt.date_granularity, country as market, product, campaign_name::varchar as google_campaign, null as bing_campaign, utm_campaign::varchar, 
            campaign_type_custom as campaign_type, null as utm_content, null as utm_term,
            COALESCE(SUM(spend),0) as spend, COALESCE(SUM(impressions),0) as impressions, COALESCE(SUM(clicks),0) as clicks, COALESCE(SUM(checkout_initiated),0) as checkout_initiated,
            COALESCE(SUM(add_to_cart),0) as add_to_cart, 0 as leads, COALESCE(SUM(purchases),0) as purchases, 0 as "VS-01 WK", COALESCE(SUM(revenue),0) as revenue, 0 as ft_orders, 0 as lt_orders
        FROM {{ source('reporting','googleads_campaign_performance') }} yt
        LEFT JOIN (SELECT utm_campaign::varchar, COUNT(*) FROM s3_data GROUP BY 1) utm ON yt.campaign_id = utm.utm_campaign 
        WHERE campaign_type_custom = 'Youtube'
        GROUP BY 1,2,3,4,5,6,7,8,9,10,11
        UNION ALL
        SELECT 'Bing' as channel, bc.date::date, bc.date_granularity, country::varchar as market, product::varchar, null as google_campaign, bing_campaign::varchar, utm_campaign::varchar, 
            campaign_type_custom::varchar as campaign_type, 
            null as utm_content, null as utm_term,
            COALESCE(SUM(spend),0) as spend, COALESCE(SUM(impressions),0) as impressions, COALESCE(SUM(clicks),0) as clicks, 0 as checkout_initiated,
            0 as add_to_cart, 0 as leads, COALESCE(SUM(purchases),0) as purchases, 0 as "VS-01 WK", COALESCE(SUM(revenue),0) as revenue, 0 as ft_orders, 0 as lt_orders
        FROM {{ source('reporting','bingads_campaign_performance') }} bc
        LEFT JOIN (SELECT utm_campaign, bing_campaign, COUNT(*) FROM s3_data GROUP BY 1,2) utm ON bc.campaign_name = utm.bing_campaign 
        GROUP BY 1,2,3,4,5,6,7,8,9,10,11
        UNION ALL
        SELECT CASE WHEN channel_adj::varchar = 'Google Ads' OR channel_adj::varchar = 'Youtube' THEN 'Google Ads' ELSE channel_adj::varchar END as channel, date, date_granularity, market, product, 
            CASE WHEN utm_content::varchar = 'DS01_DG_US CAN_AllGenders_18-65_Cooking_AllPlacements_TopicTargeting_Cooking' 
                    AND google_campaign::varchar = 'DS01 - Demand Gen - Video - US - All Placements - Audience Segmentation Test'
		            THEN 'DS01 - Demand Gen - Video - US - All Placements - Topic Targeting' 
		        ELSE google_campaign::varchar
	        END as google_campaign,
            bing_campaign::varchar, 
	    CASE WHEN utm_campaign ~* 'Amplified Budget' AND utm_campaign !~* 'Mid-Performing' AND utm_campaign !~* 'Consolidated' THEN 'DS01 - Prospect - A+SC Campaign - 7DC - KOL - Top-Performing Creators - Amplified Budget Strategy'
		ELSE REPLACE(utm_campaign,'A SC','A+SC') 
	    END as utm_campaign, 
	    campaign_type::varchar, 
            CASE WHEN channel = 'Google Ads' OR channel = 'Bing' THEN null ELSE REPLACE(REPLACE(REPLACE(utm_content,'A SC','A+SC'),'USA CAN','USA+CAN'),'UnsubscribedEmail SMS','UnsubscribedEmail+SMS') END as utm_content, 
            CASE WHEN channel = 'Google Ads' OR channel = 'Bing' THEN null ELSE SPLIT_PART(utm_term,'- Copy',1) END as utm_term,
            0 as spend, 0 as impressions, 0 as clicks, 0 as checkout_initiated, 0 as add_to_cart, 0 as leads, 0 as purchases, 0 as "VS-01 WK", 0 as revenue, ft_orders, lt_orders
        FROM s3_data)
    GROUP BY channel, date, date_granularity, market, product, google_campaign, bing_campaign, utm_campaign, campaign_type, utm_content, utm_term),
    
	base as (
	SELECT channel, 
	  date, 
	  date_granularity, 
	  market, 
	  product, 
	  google_campaign,
	  bing_campaign,
	  utm_campaign, 
	  campaign_type, 
	  utm_content, 
	  utm_term,
	  spend,
	  impressions,
	  clicks,
	  add_to_cart,
	  checkout_initiated,
	  leads,
	  purchases,
	  "VS-01 WK",
	  revenue,
	  ft_orders,
	  lt_orders,
      DATE_TRUNC('month', date) AS month_start,
      EXTRACT(day FROM DATEADD(day, -1, DATEADD(month, 1, DATE_TRUNC('month', date)))) AS days_in_month,
	  DATEDIFF(day, DATE_TRUNC('quarter', date), DATEADD(quarter, 1, DATE_TRUNC('quarter', date))) AS days_in_quarter,
      DATEDIFF(day, DATE_TRUNC('year', date), DATEADD(year, 1, DATE_TRUNC('year', date)))  AS days_in_year,
	  CASE 
        WHEN date < '2025-08-01' THEN 10000.0
        ELSE 17000.0
      END AS monthly_fee_pool,
	  CASE 
		WHEN channel = 'Google Ads'
			AND market = 'US'
			AND campaign_type IN ('Demand Gen', 'Youtube') 
			AND google_campaign !~* 'image' 
		THEN 'DG Video' 
		ELSE 'Other' 
		END AS dg_breakdown
	FROM final_data),

	with_totals AS (
	  SELECT
	    b.*,
	    SUM(spend) OVER (PARTITION BY channel, market, dg_breakdown, DATE_TRUNC('month', date)) AS total_spend
	  FROM base b
	),
	
	final AS (
	  SELECT
	    *,
	    CASE 
	      WHEN channel = 'Google Ads'
			AND market = 'US'
			AND campaign_type IN ('Demand Gen', 'Youtube') 
			AND google_campaign !~* 'image' 
	      THEN 
	        -- uplift stays at row level
	        spend * 1.02565
	        +
	        -- fee allocation scales by granularity
	        (spend / NULLIF(total_spend,0)) *
	        CASE 
	          WHEN date_granularity = 'day'     THEN monthly_fee_pool / days_in_month
	          WHEN date_granularity = 'week'    THEN (monthly_fee_pool / days_in_month) * 7
	          WHEN date_granularity = 'month'   THEN monthly_fee_pool
	          WHEN date_granularity = 'quarter' THEN monthly_fee_pool * 3
	          WHEN date_granularity = 'year'    THEN monthly_fee_pool * 12
	          ELSE monthly_fee_pool / days_in_month
	        END
		  WHEN channel = 'Google Ads'
			AND market = 'US'
			AND campaign_type IN ('Demand Gen', 'Youtube') 
			AND google_campaign ~* 'image' 
	      THEN 
	        -- uplift stays at row level
	        spend * 1.02565
	      ELSE 0
	    END AS spend_with_fees
	  FROM with_totals
	)
	
SELECT *
FROM final
