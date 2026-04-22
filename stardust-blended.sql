{{ config (
    alias = target.database + '_blended'
)}}

{%- set date_granularity_list = ['day','week','month','quarter','year'] -%}


WITH appsflyer_data AS (
        {% for granularity in date_granularity_list %}
        SELECT 
            '{{granularity}}' as date_granularity,
            case when '{{granularity}}' = 'week' then date_trunc('{{granularity}}',date::date+1)-1 else date_trunc('{{granularity}}',date::date) end as date,
			app,
            CASE 
                WHEN source = 'Facebook Ads' THEN 'Meta'
                WHEN source = 'googleadwords_int' THEN 'Google Ads'
                WHEN source = 'tiktokglobal_int' THEN 'Tiktok Ads'
                ELSE source
            END AS channel,
            campaign_name,
            sum(sessions) as sessions,
			sum(CASE  WHEN source IN ('Facebook Ads','googleadwords_int','tiktokglobal_int') THEN 0 ELSE installs END) as apps_installs,
			sum(rc_trial_started_users) as apps_trial_started,
			sum(rc_trial_converted_users) as apps_trial_converted,
			sum(rc_initial_purchase_users) as apps_initial_purchase,
			sum(revenue) as apps_revenue
        FROM {{ source('gsheet_raw','appsflyer_insights') }}
        GROUP BY 1,2,3,4,5
        {% if not loop.last %}UNION ALL{% endif %}
        {% endfor %}
    ),

appsflyer_skan_data AS (
        {% for granularity in date_granularity_list %}
        SELECT 
            '{{granularity}}' as date_granularity,
            case when '{{granularity}}' = 'week' then date_trunc('{{granularity}}',date::date+1)-1 else date_trunc('{{granularity}}',date::date) end as date,
			'iOS' AS app,
            CASE 
                WHEN source = 'Facebook Ads' THEN 'Meta'
                WHEN source = 'googleadwords_int' THEN 'Google Ads'
                WHEN source = 'tiktokglobal_int' THEN 'Tiktok Ads'
                ELSE source
            END AS channel,
            case when source = 'af_app_invites' then 'not set' else campaign_name end as campaign_name,
            sum(0) as sessions,
			sum(installs) as apps_installs,
			sum(0) as apps_trial_started,
			sum(0) as apps_trial_converted,
			sum(0) as apps_initial_purchase,
			sum(0) as apps_revenue
        FROM {{ source('gsheet_raw','appsflyer_skan_insights') }}
        GROUP BY 1,2,3,4,5
        {% if not loop.last %}UNION ALL{% endif %}
        {% endfor %}
    ),

 appsflyer_skan_total_data AS (
        {% for granularity in date_granularity_list %}
        SELECT 
            '{{granularity}}' as date_granularity,
            case when '{{granularity}}' = 'week' then date_trunc('{{granularity}}',date::date+1)-1 else date_trunc('{{granularity}}',date::date) end as date,
			'iOS' AS app,
            'af_app_invites' AS channel,
            'not set' AS campaign_name,
            sum(0) as sessions,
			sum(-installs) as apps_installs,
			sum(0) as apps_trial_started,
			sum(0) as apps_trial_converted,
			sum(0) as apps_initial_purchase,
			sum(0) as apps_revenue
        FROM {{ source('gsheet_raw','appsflyer_skan_insights') }}
        GROUP BY 1,2,3,4,5
        {% if not loop.last %}UNION ALL{% endif %}
        {% endfor %}
    ),

final_appsflyer_data AS (
	SELECT date_granularity, date, app, channel, campaign_name, 
	sum(coalesce(sessions,0)) as sessions,
	sum(coalesce(apps_installs,0)) as apps_installs,
	sum(coalesce(apps_trial_started,0)) as apps_trial_started,
	sum(coalesce(apps_trial_converted,0)) as apps_trial_converted,
	sum(coalesce(apps_initial_purchase,0)) as apps_initial_purchase,
	sum(coalesce(apps_revenue,0)) as apps_revenue
	FROM
	(SELECT * FROM appsflyer_data
	UNION ALL
	SELECT * FROM appsflyer_skan_data
	UNION ALL
	SELECT * FROM appsflyer_skan_total_data)
	GROUP BY date_granularity, date, app, channel, campaign_name),
    
paid_data as
    (SELECT channel, campaign_id::varchar as campaign_id, campaign_name, date::date, date_granularity, app, COALESCE(SUM(spend),0) as spend, COALESCE(SUM(clicks),0) as clicks, 
        COALESCE(SUM(impressions),0) as impressions, COALESCE(SUM(app_install),0) as installs, COALESCE(SUM(trial_started),0) as trial_started, COALESCE(SUM(trial_converted),0) as trial_converted,
		COALESCE(SUM(initial_purchase),0) as initial_purchase, COALESCE(SUM(trial_converted_value),0)+COALESCE(SUM(initial_purchase_value),0) as revenue
    FROM
        (SELECT 'Meta' as channel, campaign_id::varchar as campaign_id, campaign_name, date, date_granularity, 
            case 
				when campaign_name ~* '_ios_' then 'iOS'
				when campaign_name ~* '_and_' then 'Android'
				when campaign_name ~* '_web_' then 'Web'
				-- New campaign naming
				when campaign_name ~* 'SB' then 'iOS'
				else 'Other'
			end as app,
			spend, link_clicks as clicks, impressions, app_install, trial_started, trial_converted, initial_purchase, trial_converted_value, initial_purchase_value 
        FROM {{ source('reporting','facebook_campaign_performance') }}
        UNION ALL
        SELECT 'Google Ads' as channel, campaign_id::varchar as campaign_id, campaign_name, date, date_granularity,
			case 
				when campaign_name ~* '_ios_' then 'iOS'
				when campaign_name ~* '_and_' then 'Android'
				when campaign_name ~* '_web_' then 'Web'
				-- New campaign naming
				when campaign_name ~* 'SB' then 'iOS'
				else 'Other'
			end as app,
            spend, clicks, impressions, app_install, trial_started, trial_converted, initial_purchase, trial_converted_value, initial_purchase_value  
        FROM {{ source('reporting','googleads_campaign_performance') }}
		UNION ALL
		{% for granularity in date_granularity_list %}
        SELECT 
			'Tiktok Ads' as channel, campaign_id::varchar as campaign_id, campaign_name,
			case when '{{granularity}}' = 'week' then date_trunc('{{granularity}}',stat_time_day::date+1)-1 else date_trunc('{{granularity}}',stat_time_day::date) end as date,
			'{{granularity}}' as date_granularity,
			case 
				when campaign_name ~* '_ios_' then 'iOS'
				when campaign_name ~* '_and_' then 'Android'
				when campaign_name ~* '_web_' then 'Web'
				-- New campaign naming
				when campaign_name ~* 'SB' then 'iOS'
				else 'Other'
			end as app,
            COALESCE(SUM(spend),0) as spend, 
			COALESCE(SUM(clicks),0) as clicks, 
        	COALESCE(SUM(impressions),0) as impressions, 
			COALESCE(SUM(skan_app_install),0) as installs, 
			COALESCE(SUM(skan_start_trial),0) as trial_started, 
			COALESCE(SUM(0),0) as trial_converted,
			COALESCE(SUM(0),0) as initial_purchase,
			COALESCE(SUM(0),0) as trial_converted_value,
			COALESCE(SUM(0),0) as initial_purchase_value
        FROM {{ source('tiktok_raw','campaign_performance_report') }}
        GROUP BY 1,2,3,4,5,6
        {% if not loop.last %}UNION ALL{% endif %}
        {% endfor %}
        )
    GROUP BY channel, campaign_id, campaign_name, date, date_granularity, app),

paid_appsflyer_data as (
  SELECT 
    channel, campaign_name, coalesce(campaign_id::varchar,'(not set)') as campaign_id, date::date, date_granularity, app,
    SUM(COALESCE(spend, 0)) AS spend,
    SUM(COALESCE(clicks, 0)) AS clicks,
    SUM(COALESCE(impressions, 0)) AS impressions,
    SUM(COALESCE(installs, 0)) AS installs,
    SUM(COALESCE(sessions, 0)) AS sessions,
	SUM(COALESCE(revenue, 0)) AS revenue,
	SUM(COALESCE(apps_revenue, 0)) AS apps_revenue,
    SUM(COALESCE(initial_purchase, 0)) AS initial_purchase,
    SUM(COALESCE(trial_converted, 0)) AS trial_converted,
    SUM(COALESCE(trial_started, 0)) AS trial_started,
	SUM(COALESCE(apps_installs, 0)) AS apps_installs,
    SUM(COALESCE(apps_initial_purchase, 0)) AS apps_initial_purchase,
    SUM(COALESCE(apps_trial_converted, 0)) AS apps_trial_converted,
    SUM(COALESCE(apps_trial_started, 0)) AS apps_trial_started
  FROM paid_data FULL OUTER JOIN final_appsflyer_data USING(channel,date,date_granularity,campaign_name,app)
  GROUP BY 1,2,3,4,5,6)
    
SELECT 
    channel,
	app,
    campaign_name,
	campaign_id,
    date,
    date_granularity,
    spend,
    clicks,
    impressions,
    installs,
    sessions,
	revenue,
	apps_revenue,
    initial_purchase,
    trial_converted,
    trial_started,
	apps_installs,
	apps_initial_purchase,
	apps_trial_converted,
	apps_trial_started
FROM paid_appsflyer_data
