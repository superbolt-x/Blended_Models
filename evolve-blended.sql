{{ config (
    alias = target.database + '_blended'
)}}

WITH leads_data as
    (SELECT 'day' as date_granularity, DATE_TRUNC('day',created_on::date) as date,
        CASE WHEN medium ~* 'facebook' THEN 'Facebook' ELSE medium END as channel, 
        assigned_user as location,
        utm_campaign as campaign_name, 
        utm_medium as adset_name,
        utm_content as ad_name,
        CASE WHEN utm_content ~* 'DiamondGlow' THEN 'DiamondGlow'
            WHEN utm_content ~* 'Hydrafacial' THEN 'Hydrafacial'
            WHEN utm_content ~* 'Botox' THEN 'Botox'
            WHEN utm_content ~* 'Xeomin' THEN 'Xeomin'
            WHEN utm_content ~* 'Facials' THEN 'Facials - General'
            WHEN utm_content ~* 'Lip Filler' THEN 'Lip Filler'
            WHEN utm_content ~* 'Filler' THEN 'Filler - General'
            WHEN utm_content ~* 'LHR' THEN 'LHR'
            WHEN utm_content ~* 'Microneedling' THEN 'Microneedling'
            WHEN utm_content ~* 'Traptox' THEN 'Traptox'
            WHEN utm_content ~* 'BBL' THEN 'BBL'
            WHEN utm_content ~* 'Lip Flip' THEN 'Lip Flip'
            WHEN utm_content ~* 'PRP Undereye' THEN 'PRP Undereye'
            WHEN utm_content ~* 'Clear and Brilliant' THEN 'Clear + Brilliant'
            WHEN utm_content ~* 'Wedding' THEN 'Wedding Prep'
            WHEN utm_content ~* 'General evergreen' THEN 'General evergreen'
            ELSE 'Others'
        END AS service,
        COUNT(opportunity_name) as leads,
        SUM(CASE WHEN stage = 'Replied' THEN 1 ELSE 0 END) as replied,
        SUM(CASE WHEN stage = 'Booked' OR stage = 'Patient' THEN 1 ELSE 0 END) as appointments
    FROM {{ source('gsheet_raw','crm_leads') }}
    GROUP BY 1,2,3,4,5,6,7,8
    UNION ALL
    SELECT 'week' as date_granularity, DATE_TRUNC('week',created_on::date) as date,
        CASE WHEN medium ~* 'facebook' THEN 'Facebook' ELSE medium END as channel, 
        assigned_user as location,
        utm_campaign as campaign_name, 
        utm_medium as adset_name,
        utm_content as ad_name,
        CASE WHEN utm_content ~* 'DiamondGlow' THEN 'DiamondGlow'
            WHEN utm_content ~* 'Hydrafacial' THEN 'Hydrafacial'
            WHEN utm_content ~* 'Botox' THEN 'Botox'
            WHEN utm_content ~* 'Xeomin' THEN 'Xeomin'
            WHEN utm_content ~* 'Facials' THEN 'Facials - General'
            WHEN utm_content ~* 'Lip Filler' THEN 'Lip Filler'
            WHEN utm_content ~* 'Filler' THEN 'Filler - General'
            WHEN utm_content ~* 'LHR' THEN 'LHR'
            WHEN utm_content ~* 'Microneedling' THEN 'Microneedling'
            WHEN utm_content ~* 'Traptox' THEN 'Traptox'
            WHEN utm_content ~* 'BBL' THEN 'BBL'
            WHEN utm_content ~* 'Lip Flip' THEN 'Lip Flip'
            WHEN utm_content ~* 'PRP Undereye' THEN 'PRP Undereye'
            WHEN utm_content ~* 'Clear and Brilliant' THEN 'Clear + Brilliant'
            WHEN utm_content ~* 'Wedding' THEN 'Wedding Prep'
            WHEN utm_content ~* 'General evergreen' THEN 'General evergreen'
            ELSE 'Others'
        END AS service,
        COUNT(opportunity_name) as leads,
        SUM(CASE WHEN stage = 'Replied' THEN 1 ELSE 0 END) as replied,
        SUM(CASE WHEN stage = 'Booked' OR stage = 'Patient' THEN 1 ELSE 0 END) as appointments
    FROM {{ source('gsheet_raw','crm_leads') }}
    GROUP BY 1,2,3,4,5,6,7,8
    UNION ALL
    SELECT 'month' as date_granularity, DATE_TRUNC('month',created_on::date) as date,
        CASE WHEN medium ~* 'facebook' THEN 'Facebook' ELSE medium END as channel, 
        assigned_user as location,
        utm_campaign as campaign_name, 
        utm_medium as adset_name,
        utm_content as ad_name,
        CASE WHEN utm_content ~* 'DiamondGlow' THEN 'DiamondGlow'
            WHEN utm_content ~* 'Hydrafacial' THEN 'Hydrafacial'
            WHEN utm_content ~* 'Botox' THEN 'Botox'
            WHEN utm_content ~* 'Xeomin' THEN 'Xeomin'
            WHEN utm_content ~* 'Facials' THEN 'Facials - General'
            WHEN utm_content ~* 'Lip Filler' THEN 'Lip Filler'
            WHEN utm_content ~* 'Filler' THEN 'Filler - General'
            WHEN utm_content ~* 'LHR' THEN 'LHR'
            WHEN utm_content ~* 'Microneedling' THEN 'Microneedling'
            WHEN utm_content ~* 'Traptox' THEN 'Traptox'
            WHEN utm_content ~* 'BBL' THEN 'BBL'
            WHEN utm_content ~* 'Lip Flip' THEN 'Lip Flip'
            WHEN utm_content ~* 'PRP Undereye' THEN 'PRP Undereye'
            WHEN utm_content ~* 'Clear and Brilliant' THEN 'Clear + Brilliant'
            WHEN utm_content ~* 'Wedding' THEN 'Wedding Prep'
            WHEN utm_content ~* 'General evergreen' THEN 'General evergreen'
            ELSE 'Others'
        END AS service,
        COUNT(opportunity_name) as leads,
        SUM(CASE WHEN stage = 'Replied' THEN 1 ELSE 0 END) as replied,
        SUM(CASE WHEN stage = 'Booked' OR stage = 'Patient' THEN 1 ELSE 0 END) as appointments
    FROM {{ source('gsheet_raw','crm_leads') }}
    GROUP BY 1,2,3,4,5,6,7,8
    UNION ALL
    SELECT 'quarter' as date_granularity, DATE_TRUNC('quarter',created_on::date) as date,
        CASE WHEN medium ~* 'facebook' THEN 'Facebook' ELSE medium END as channel, 
        assigned_user as location,
        utm_campaign as campaign_name, 
        utm_medium as adset_name,
        utm_content as ad_name,
        CASE WHEN utm_content ~* 'DiamondGlow' THEN 'DiamondGlow'
            WHEN utm_content ~* 'Hydrafacial' THEN 'Hydrafacial'
            WHEN utm_content ~* 'Botox' THEN 'Botox'
            WHEN utm_content ~* 'Xeomin' THEN 'Xeomin'
            WHEN utm_content ~* 'Facials' THEN 'Facials - General'
            WHEN utm_content ~* 'Lip Filler' THEN 'Lip Filler'
            WHEN utm_content ~* 'Filler' THEN 'Filler - General'
            WHEN utm_content ~* 'LHR' THEN 'LHR'
            WHEN utm_content ~* 'Microneedling' THEN 'Microneedling'
            WHEN utm_content ~* 'Traptox' THEN 'Traptox'
            WHEN utm_content ~* 'BBL' THEN 'BBL'
            WHEN utm_content ~* 'Lip Flip' THEN 'Lip Flip'
            WHEN utm_content ~* 'PRP Undereye' THEN 'PRP Undereye'
            WHEN utm_content ~* 'Clear and Brilliant' THEN 'Clear + Brilliant'
            WHEN utm_content ~* 'Wedding' THEN 'Wedding Prep'
            WHEN utm_content ~* 'General evergreen' THEN 'General evergreen'
            ELSE 'Others'
        END AS service,
        COUNT(opportunity_name) as leads,
        SUM(CASE WHEN stage = 'Replied' THEN 1 ELSE 0 END) as replied,
        SUM(CASE WHEN stage = 'Booked' OR stage = 'Patient' THEN 1 ELSE 0 END) as appointments
    FROM {{ source('gsheet_raw','crm_leads') }}
    GROUP BY 1,2,3,4,5,6,7,8
    UNION ALL
    SELECT 'year' as date_granularity, DATE_TRUNC('year',created_on::date) as date,
        CASE WHEN medium ~* 'facebook' THEN 'Facebook' ELSE medium END as channel, 
        assigned_user as location,
        utm_campaign as campaign_name, 
        utm_medium as adset_name,
        utm_content as ad_name,
        CASE WHEN utm_content ~* 'DiamondGlow' THEN 'DiamondGlow'
            WHEN utm_content ~* 'Hydrafacial' THEN 'Hydrafacial'
            WHEN utm_content ~* 'Botox' THEN 'Botox'
            WHEN utm_content ~* 'Xeomin' THEN 'Xeomin'
            WHEN utm_content ~* 'Facials' THEN 'Facials - General'
            WHEN utm_content ~* 'Lip Filler' THEN 'Lip Filler'
            WHEN utm_content ~* 'Filler' THEN 'Filler - General'
            WHEN utm_content ~* 'LHR' THEN 'LHR'
            WHEN utm_content ~* 'Microneedling' THEN 'Microneedling'
            WHEN utm_content ~* 'Traptox' THEN 'Traptox'
            WHEN utm_content ~* 'BBL' THEN 'BBL'
            WHEN utm_content ~* 'Lip Flip' THEN 'Lip Flip'
            WHEN utm_content ~* 'PRP Undereye' THEN 'PRP Undereye'
            WHEN utm_content ~* 'Clear and Brilliant' THEN 'Clear + Brilliant'
            WHEN utm_content ~* 'Wedding' THEN 'Wedding Prep'
            WHEN utm_content ~* 'General evergreen' THEN 'General evergreen'
            ELSE 'Others'
        END AS service,
        COUNT(opportunity_name) as leads,
        SUM(CASE WHEN stage = 'Replied' THEN 1 ELSE 0 END) as replied,
        SUM(CASE WHEN stage = 'Booked' OR stage = 'Patient' THEN 1 ELSE 0 END) as appointments
    FROM {{ source('gsheet_raw','crm_leads') }}
    GROUP BY 1,2,3,4,5,6,7,8),
    
fb_data as 
    (SELECT date, date_granularity, 'Facebook' as channel, location, campaign_type, campaign_name, adset_name, ad_name, service, 
        COALESCE(SUM(spend),0) as spend, COALESCE(SUM(impressions),0) as impressions, COALESCE(SUM(clicks),0) as clicks, 
        COALESCE(SUM(crm_leads),0) as crm_leads, COALESCE(SUM(crm_replied),0) as crm_replied, COALESCE(SUM(crm_appointments),0) as crm_appointments, 
        COALESCE(SUM(platform_appointments),0) as platform_appointments, COALESCE(SUM(platform_leads),0) as platform_leads,
        COALESCE(SUM(booked_appointment),0) as booked_appointment, COALESCE(SUM(completed_appointment),0) as completed_appointment,
        COALESCE(SUM(booking_complete),0) as booking_complete
    FROM
        (SELECT date, date_granularity, location, 
            CASE WHEN campaign_name in (
                '[SB] - Prospecting - DTB',
                '[SB] - Prospecting - A+SC - Catch All (NY, NJ, MD) - DTB - BookingComplete Campaign',
                '[SB] - NEW Prospecting - A+SC - Catch All (NY, NJ, MD) - DTB - BookingComplete Campaign',
                '[SB] - Prospecting - Manual A/B Test - All Clinics - BookingComplete Campaign'
                ) THEN 'Prospecting DTB' 
                WHEN campaign_name = '[SB] - Prospecting - Leads' THEN 'Prospecting Leads' 
                WHEN campaign_name = '[SB] - Retargeting - DTB' THEN 'Retargeting DTB'
                WHEN campaign_name = '[SB] - DTB - Catch All - Black Friday 2024' THEN 'Black Friday DTB'
                ELSE 'Other'
            END as campaign_type,
            campaign_name, adset_name, ad_name, service, 
            spend, impressions, link_clicks as clicks, 0 as crm_leads, 0 as crm_replied, 0 as crm_appointments, 
            appointments_scheduled as platform_appointments, leads as platform_leads, booked_appointment, completed_appointment, booking_complete
        FROM {{ source('reporting','facebook_ad_performance') }}
        UNION ALL
        SELECT date, date_granularity, location, 
            CASE WHEN campaign_name in (
                '[SB] - Prospecting - DTB',
                '[SB] - Prospecting - A+SC - Catch All (NY, NJ, MD) - DTB - BookingComplete Campaign',
                '[SB] - NEW Prospecting - A+SC - Catch All (NY, NJ, MD) - DTB - BookingComplete Campaign',
                '[SB] - Prospecting - Manual A/B Test - All Clinics - BookingComplete Campaign'
                ) THEN 'Prospecting DTB' 
                WHEN campaign_name = '[SB] - Prospecting - Leads' THEN 'Prospecting Leads' 
                WHEN campaign_name = '[SB] - Retargeting - DTB' THEN 'Retargeting DTB'
                WHEN campaign_name = '[SB] - DTB - Catch All - Black Friday 2024' THEN 'Black Friday DTB'
                ELSE 'Other'
            END as campaign_type,
            campaign_name, adset_name, ad_name, service, 
            0 as spend, 0 as impressions, 0 as clicks, 
            COALESCE(SUM(leads),0) as crm_leads, COALESCE(SUM(replied),0) as crm_replied, COALESCE(SUM(appointments),0) as crm_appointments, 
            0 as platform_appointments, 0 as platform_leads, 0 as booked_appointment, 0 as completed_appointment, 0 as booking_complete
        FROM leads_data
        WHERE channel = 'Facebook'
        GROUP BY 1,2,3,4,5,6,7,8          
        )
    GROUP BY 1,2,3,4,5,6,7,8,9),

adw_data as 
    (SELECT date, date_granularity, 'Google' as channel, location, campaign_type, campaign_name, adset_name, ad_name, service, 
        COALESCE(SUM(spend),0) as spend, COALESCE(SUM(impressions),0) as impressions, COALESCE(SUM(clicks),0) as clicks, 
        COALESCE(SUM(crm_leads),0) as crm_leads, COALESCE(SUM(crm_replied),0) as crm_replied, COALESCE(SUM(crm_appointments),0) as crm_appointments, 
        COALESCE(SUM(platform_appointments),0) as platform_appointments, COALESCE(SUM(platform_leads),0) as platform_leads, 
        COALESCE(SUM(booked_appointment),0) as booked_appointment, COALESCE(SUM(completed_appointment),0) as completed_appointment,
        COALESCE(SUM(booking_complete),0) as booking_complete
    FROM
        (SELECT date, date_granularity, null as location,
            CASE WHEN campaign_name ~* 'Search - Branded' THEN 'Search - Branded' 
                WHEN campaign_name ~* 'Search - Non Brand' THEN 'Search - Non Brand' 
                WHEN campaign_name ~* 'PMax' THEN 'PMax' 
            END as campaign_type,
            campaign_name, null as adset_name, null as ad_name, null as service, 
            spend, impressions, clicks, 0 as crm_leads, 0 as crm_replied, 0 as crm_appointments, 
            appointments_scheduled as platform_appointments, 0 as platform_leads, 0 as booked_appointment, 0 as completed_appointment, 0 as booking_complete
        FROM {{ source('reporting','googleads_campaign_performance') }}
        UNION ALL
        SELECT date, date_granularity, null as location,
            CASE WHEN campaign_name ~* 'Search - Branded' THEN 'Search - Branded' 
                WHEN campaign_name ~* 'Search - Non Brand' THEN 'Search - Non Brand' 
                WHEN campaign_name ~* 'PMax' THEN 'PMax' 
            END as campaign_type,
            campaign_name, null as adset_name, null as ad_name, null as service, 
            0 as spend, 0 as impressions, 0 as clicks, 
            COALESCE(SUM(leads),0) as crm_leads, COALESCE(SUM(replied),0) as crm_replied, COALESCE(SUM(appointments),0) as crm_appointments, 
            0 as platform_appointments, 0 as platform_leads, 0 as booked_appointment, 0 as completed_appointment, 0 as booking_complete
        FROM leads_data
        WHERE channel = 'Google'
        GROUP BY 1,2,3,4,5,6,7
        )
    GROUP BY 1,2,3,4,5,6,7,8,9)

SELECT date, date_granularity, channel, location, campaign_type, campaign_name, adset_name, ad_name, service, 
    spend, impressions, clicks, crm_leads, crm_replied, crm_appointments, platform_appointments, platform_leads,
    booked_appointment, completed_appointment, booking_complete
FROM 
    (SELECT * FROM fb_data
    UNION 
    SELECT * FROM adw_data)
