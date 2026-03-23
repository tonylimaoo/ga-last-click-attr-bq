-- CREATE OR REPLACE TABLE `rocky-rawdata.anacapri_ga_attr.events_treated`
-- PARTITION BY event_date
-- AS

WITH base_events AS (
  SELECT 
    CAST(event_date AS DATE FORMAT 'YYYYMMDD') AS event_date,
    user_pseudo_id,
    user_id,
    event_name,
    event_timestamp, -- novo
    UNIX_SECONDS(TIMESTAMP_MICROS(event_timestamp)) AS event_timestamp_seconds, -- novo
    is_active_user,
    privacy_info.analytics_storage AS privacy_info_analytics_storage,
    privacy_info.ads_storage AS privacy_info_ads_storage,
    privacy_info.uses_transient_token AS privacy_info_uses_transient_token,
    geo.region AS state,
    geo.city AS city,
    platform,
    IF(LOWER(platform) = 'web','web','app') AS origem_dados,
    device.category AS device_category,
    device.operating_system AS operating_system,
    device.browser AS browser,
    stream_id,
    traffic_source.name AS first_user_campaign_name,
    traffic_source.medium AS first_user_medium,
    traffic_source.source AS first_user_source,
    session_traffic_source_last_click.manual_campaign.campaign_id AS session_traffic_source_campaign_id,
    session_traffic_source_last_click.manual_campaign.campaign_name AS session_traffic_source_campaign_name,
    session_traffic_source_last_click.manual_campaign.source AS session_traffic_source_source,
    session_traffic_source_last_click.manual_campaign.medium AS session_traffic_source_medium,
    session_traffic_source_last_click.manual_campaign.term AS session_traffic_source_term,
    session_traffic_source_last_click.manual_campaign.content AS session_traffic_source_content,
    collected_traffic_source.manual_source AS collected_traffic_source_manual_source,
    collected_traffic_source.manual_medium AS collected_traffic_source_manual_medium,
    collected_traffic_source.manual_campaign_name AS collected_traffic_source_manual_campaign_name,
    collected_traffic_source.manual_content AS CONTEUDO_ANUNCIO,
    collected_traffic_source.manual_term AS KEYWORD,
    collected_traffic_source.gclid AS gclid,
    CASE
      WHEN event_name IN ('first_visit', 'first_open') AND collected_traffic_source.manual_source IS NULL THEN traffic_source.source
      WHEN collected_traffic_source.manual_source IN ('google') AND collected_traffic_source.manual_medium IN ('organic','youtube') AND collected_traffic_source.gclid IS NOT NULL THEN 'google'
      WHEN collected_traffic_source.gclid IS NOT NULL OR (SELECT value.string_value FROM UNNEST(event_params) WHERE KEY = 'page_location') LIKE '%gbraid%' OR (SELECT value.string_value FROM UNNEST(event_params) WHERE KEY = 'page_location') LIKE '%wbraid%' THEN 'google'
      WHEN collected_traffic_source.dclid IS NOT NULL AND collected_traffic_source.gclid IS NULL THEN 'dbm'
      ELSE collected_traffic_source.manual_source
    END AS treated_manual_source,
    CASE
      WHEN event_name IN ('first_visit', 'first_open') AND collected_traffic_source.manual_medium IS NULL THEN traffic_source.medium
      WHEN collected_traffic_source.manual_source IN ('google') AND collected_traffic_source.manual_medium IN ('organic','youtube') AND collected_traffic_source.gclid IS NOT NULL THEN 'cpc'
      WHEN collected_traffic_source.gclid IS NOT NULL OR (SELECT value.string_value FROM UNNEST(event_params) WHERE KEY = 'page_location') LIKE '%gbraid%' OR (SELECT value.string_value FROM UNNEST(event_params) WHERE KEY = 'page_location') LIKE '%wbraid%' THEN 'cpc'
      WHEN collected_traffic_source.dclid IS NOT NULL AND collected_traffic_source.gclid IS NULL THEN 'cpm'
      ELSE collected_traffic_source.manual_medium
    END AS treated_manual_medium,
    CASE
      WHEN event_name IN ('first_visit', 'first_open') AND collected_traffic_source.manual_campaign_name IS NULL THEN traffic_source.name
      WHEN collected_traffic_source.manual_source IN ('google') AND collected_traffic_source.manual_medium IN ('organic','youtube') AND collected_traffic_source.gclid IS NOT NULL THEN (
        CASE
          WHEN collected_traffic_source.gclid IS NOT NULL OR (SELECT value.string_value FROM UNNEST(event_params) WHERE KEY = 'page_location') LIKE '%gbraid%' OR (SELECT value.string_value FROM UNNEST(event_params) WHERE KEY = 'page_location') LIKE '%wbraid%' THEN '(cpc)'
          END
        )
        ELSE
          CASE
          WHEN collected_traffic_source.dclid IS NOT NULL AND collected_traffic_source.gclid IS NULL THEN '(cpm)'
          ELSE collected_traffic_source.manual_campaign_name
        END
    END AS treated_manual_campaign_name,
    CASE
      WHEN LOWER(platform) IN ('ios','android')  THEN 'App'
      WHEN LOWER(platform) = 'web' AND LOWER(device.category) = 'desktop' THEN 'Site Desktop'
      WHEN LOWER(platform) = 'web' AND LOWER(device.category) IN ( 'mobile', 'tablet' ) THEN 'Site Mobile'
    END AS canal_dispositivo,
    -- Event Params
    (SELECT value.string_value FROM UNNEST(event_params) WHERE KEY = 'page_location') AS ep_page_location,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE KEY = 'page_referrer') AS ep_page_referrer,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE KEY = 'page_title') AS ep_page_title,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE KEY = 'application') AS ep_application,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE KEY = 'engaged_session_event') AS ep_engaged_session_event,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS ep_ga_session_id,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_number') AS ep_ga_session_number,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'content_type') AS ep_content_type,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE KEY = 'search_term') as ep_search_term,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE KEY = 'payment_type') AS ep_payment_type,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE KEY = 'shipping_tier') AS ep_shipping_tier,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE KEY = 'session_engaged') AS ep_int_session_engaged,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE KEY = 'session_engaged') AS ep_string_session_engaged,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE KEY = 'firebase_screen') AS ep_firebase_screen,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE KEY = 'firebase_screen_class') AS ep_firebase_screen_class,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'method') AS ep_method,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE KEY = 'coupon') AS ep_coupon,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE KEY = 'engagement_time_msec') AS ep_engagement_time_msec,
    (SELECT value.double_value FROM UNNEST(event_params) WHERE KEY = 'shipping') AS ep_shipping,
    (SELECT value.double_value FROM UNNEST(event_params) WHERE KEY = 'tax') AS ep_tax,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE KEY = 'item_list') AS ep_item_list,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE KEY = 'item_list_id') AS ep_item_list_id,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE KEY = 'item_list_name') AS ep_item_list_name,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE KEY = 'item_id') AS ep_item_id,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE KEY = 'content_group') AS ep_content_group,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE KEY = 'creative_name') AS ep_creative_name,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE KEY = 'creative_slot') AS ep_creative_slot,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE KEY = 'promotion_id') AS ep_promotion_id,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE KEY = 'promotion_name') AS ep_promotion_name,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE KEY = 'currency') AS ep_currency,
    (SELECT value.double_value FROM UNNEST(event_params) WHERE KEY = 'value') AS ep_value,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE KEY = 'transaction_id') AS ep_transaction_id,
    ecommerce.total_item_quantity AS ec_total_item_quantity,
    ecommerce.tax_value AS ec_tax_value, 
    ecommerce.unique_items AS ec_unique_items,
    ecommerce.transaction_id AS ec_transaction_id,
    ecommerce.purchase_revenue AS ec_purchase_revenue
  FROM `projeto-gcp.analytics_123456789.events_*` 
  WHERE _TABLE_SUFFIX = REGEXP_REPLACE(CAST(CURRENT_DATE() - 1 AS STRING), '-', '')
  -- WHERE _TABLE_SUFFIX BETWEEN '20250901' AND '20260112'
)

SELECT * FROM base_events