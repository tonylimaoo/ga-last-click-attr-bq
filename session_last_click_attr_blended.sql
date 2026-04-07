WITH base_events AS (
  SELECT 
    event_date,
    user_id,
    user_pseudo_id,
    platform,
    ep_ga_session_id AS ga_session_id,
    ep_ga_session_number,
    event_name,
    event_timestamp, -- novo
    UNIX_SECONDS(TIMESTAMP_MICROS(event_timestamp)) AS event_timestamp_seconds, -- novo
    -- treated_manual_source AS latest_manual_source,
    -- treated_manual_medium AS latest_manual_medium,
    IF(treated_manual_source = 'Data Not Available', NULL, treated_manual_source) AS manual_source,
    IF(treated_manual_medium = 'Data Not Available', NULL, treated_manual_medium) AS manual_medium,
    session_traffic_source_source,
    session_traffic_source_medium,
    SPLIT(SPLIT(anacapri_ga_attr.url_decode(ep_page_location), 'utm_source=')[SAFE_OFFSET(1)], '&')[SAFE_OFFSET(0)] AS utm_source,
    SPLIT(SPLIT(anacapri_ga_attr.url_decode(ep_page_location), 'utm_medium=')[SAFE_OFFSET(1)], '&')[SAFE_OFFSET(0)] AS utm_medium,
    ep_page_location,
    -- SPLIT(SPLIT(ep_page_location, "utm_source=")[SAFE_OFFSET(1)], '&')[SAFE_OFFSET(0)] AS manual_source,
    -- SPLIT(SPLIT(ep_page_location, "utm_medium=")[SAFE_OFFSET(1)], '&')[SAFE_OFFSET(0)] AS manual_medium,
    GCLID AS gclid,
    treated_manual_campaign_name AS manual_camapaign_name,
    CONTEUDO_ANUNCIO AS conteudo_anuncio,
    KEYWORD AS keyword,
  FROM `projeto-gcp.dataset_bq.events_treated`
  -- WHERE event_date BETWEEN '2025-12-28' - 30 AND '2025-12-28' 
  WHERE event_date BETWEEN '2025-12-01' - 30 AND '2025-12-31' 
  -- WHERE event_date BETWEEN '2025-06-01' - 90 AND '2025-12-28' 
  -- AND platform = 'WEB'
),

pre_atrib AS (
  SELECT 
    event_date,
    FIRST_VALUE(user_id IGNORE NULLS) OVER(PARTITION BY session_id ORDER BY event_timestamp ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS user_id,
    user_pseudo_id,
    session_id,
    platform,
    event_name,
    event_timestamp,
    event_timestamp_seconds,
    IFNULL(manual_traffic_source, IFNULL(utm_traffic_source, NULL)) AS manual_traffic_source,
    ga_session_attr,
    utm_traffic_source
  FROM(
    SELECT
      event_date,
      user_id,
      user_pseudo_id,
      platform,
      CONCAT(user_pseudo_id, IF(ga_session_id - ga_session_id_by_timestamp <= 1800, ga_session_id, ga_session_id_by_timestamp)) AS session_id,
      event_name,
      event_timestamp, 
      event_timestamp_seconds,
      CONCAT(manual_source, ' / ', manual_medium) AS manual_traffic_source,
      CONCAT(session_traffic_source_source, ' / ', session_traffic_source_medium) AS ga_session_attr,
      CONCAT(utm_source, ' / ', utm_medium) AS utm_traffic_source,
    FROM
    (
      SELECT 
        event_date,
        user_id,
        user_pseudo_id,
        platform,
        IFNULL(
          ga_session_id,
          UNIX_SECONDS(TIMESTAMP_MICROS(event_timestamp))
        ) AS ga_session_id_by_timestamp,
        IFNULL( 
          ga_session_id,
          FIRST_VALUE(ga_session_id IGNORE NULLS) OVER(PARTITION BY user_pseudo_id, event_date ORDER BY event_timestamp ASC ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
        ) AS ga_session_id,
        event_name,
        event_timestamp,
        event_timestamp_seconds,
        CASE 
          WHEN manual_medium IS NOT NULL AND manual_source IS NULL THEN '(not set)'
          ELSE manual_source
        END AS manual_source,
        CASE 
          WHEN manual_medium IS NULL AND manual_source IS NOT NULL THEN '(not set)'
          ELSE manual_medium
        END AS manual_medium,
        session_traffic_source_source,
        session_traffic_source_medium,
        utm_source,
        utm_medium
      FROM base_events
    )
  )
),

first_attr AS (
  SELECT
    event_date,
    user_id,
    user_pseudo_id,
    session_id,
    platform,
    first_event_timestamp_seconds,
    first_value_traffic_source,
    last_attribution_event_traffic_source,
    IF(first_value_traffic_source IS NOT NULL, CONCAT(first_event_timestamp_seconds, '-', first_value_traffic_source), NULL) AS first_and_date,
    IF(last_attribution_event_traffic_source IS NOT NULL, CONCAT(first_event_timestamp_seconds, '-', last_attribution_event_traffic_source), NULL) AS last_and_date,
    ga_session_attr
  FROM (
    SELECT 
      FIRST_VALUE(event_date IGNORE NULLS) OVER(PARTITION BY session_id ORDER BY event_timestamp ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS event_date,
      user_id,
      user_pseudo_id,
      platform,
      session_id,
      FIRST_VALUE(event_timestamp_seconds) OVER (PARTITION BY session_id ORDER BY event_timestamp_seconds ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_timestamp_seconds,
      FIRST_VALUE(manual_traffic_source) OVER (PARTITION BY session_id ORDER BY event_timestamp_seconds ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_value_traffic_source,
      LAST_VALUE(manual_traffic_source IGNORE NULLS) OVER (PARTITION BY session_id ORDER BY event_timestamp_seconds ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_attribution_event_traffic_source,
      ga_session_attr
    FROM pre_atrib
  )
  GROUP BY ALL
),

second_attr AS (
  SELECT 
    event_date,
    user_pseudo_id,
    user_id,
    session_id,
    platform,
    first_event_timestamp_seconds,
    first_value_traffic_source,
    last_attribution_event_traffic_source,
    SPLIT(session_attribution_and_date, '-')[SAFE_OFFSET(1)] AS session_attribution,
    SPLIT(session_attribution_user_id_and_date, '-')[SAFE_OFFSET(1)] AS session_attribution_user_id,
    session_attribution_and_date,
    session_attribution_user_id_and_date,
    ga_session_attr
  FROM
  (
    SELECT 
      event_date,
      user_pseudo_id,
      user_id,
      session_id,
      platform,
      first_event_timestamp_seconds,
      first_value_traffic_source,
      last_attribution_event_traffic_source,
      -- IF(first_value_traffic_source IS NOT NULL,
      --   first_value_traffic_source,
      --   LAST_VALUE(last_attribution_event_traffic_source IGNORE NULLS) OVER (PARTITION BY user_pseudo_id ORDER BY first_event_timestamp_seconds ASC RANGE BETWEEN 2592000 PRECEDING AND 1 PRECEDING)
      -- ) AS session_attribution,
      -- IF(first_value_traffic_source IS NOT NULL,
      --   first_value_traffic_source,
      --   IF(user_id IS NULL, NULL, LAST_VALUE(last_attribution_event_traffic_source IGNORE NULLS) OVER (PARTITION BY user_id ORDER BY first_event_timestamp_seconds ASC RANGE BETWEEN 2592000 PRECEDING AND 1 PRECEDING))
      -- ) AS session_attribution_user_id,
      IF(first_and_date IS NOT NULL,
        first_and_date,
        LAST_VALUE(last_and_date IGNORE NULLS) OVER (PARTITION BY user_pseudo_id ORDER BY first_event_timestamp_seconds ASC RANGE BETWEEN 2592000 PRECEDING AND 1 PRECEDING)
      ) AS session_attribution_and_date,
      IF(first_and_date IS NOT NULL,
        first_and_date,
        IF(user_id IS NULL, NULL, LAST_VALUE(last_and_date IGNORE NULLS) OVER (PARTITION BY user_id ORDER BY first_event_timestamp_seconds ASC RANGE BETWEEN 2592000 PRECEDING AND 1 PRECEDING))
      ) AS session_attribution_user_id_and_date,
      ga_session_attr
    FROM first_attr
  )
),

join_purchase_data AS (
    SELECT 
      event_date,
      user_pseudo_id,
      user_id,
      session_id,
      platform,
      IFNULL(session_attribution, '(direct) / (none)') AS session_attribution,
      IFNULL(session_attribution_user_id, '(direct) / (none)') AS session_attribution_user_id,
      session_attribution_and_date,
      session_attribution_user_id_and_date,
      ga_session_attr
  FROM second_attr
  -- WHERE event_date = '2025-11-01'
  -- WHERE event_date = '2025-12-28' 
  WHERE event_date BETWEEN '2025-12-01' AND '2025-12-31' 
  -- WHERE event_date BETWEEN '2025-06-01' AND '2025-12-28'
  -- WHERE event_date = '2025-12-17'
),

general_info AS (
  SELECT 
    CONCAT(user_pseudo_id, ep_ga_session_id) AS session_id,
    user_pseudo_id,
    COUNTIF(event_name = 'purchase') AS purchases,
    SUM(IF(event_name = 'purchase', ep_shipping, 0)) AS shipping,
    SUM(ec_purchase_revenue) AS total_revenue,
  FROM `projeto-gcp.dataset_bq.events_treated`
  WHERE event_date BETWEEN '2025-12-01' AND '2025-12-31' 
  -- WHERE event_date = '2025-12-28' 
  -- AND platform = 'WEB'
  GROUP BY ALL
),

attr_revenue AS (
  SELECT
    event_date,
    session_id,
    user_pseudo_id,
    platform,
    session_attribution,
    session_attribution_user_id,
    session_attribution_date,
    session_attribution_date_user_id,
    CASE 
      WHEN ga_session_attr = '(not set) / (not set)' THEN attr_w_user_id
      WHEN ga_session_attr = 'google / organic' THEN attr_w_user_id
      WHEN ga_session_attr = '(direct) / (none)' THEN attr_w_user_id
      WHEN ga_session_attr LIKE '%safeframe.googlesyndication%' THEN attr_w_user_id
      WHEN attr_w_user_id LIKE '%safeframe.googlesyndication%' THEN 'google / cpc'
      WHEN ga_session_attr IS NULL THEN attr_w_user_id
      ELSE ga_session_attr
    END AS final_session_attribution,
    attr_w_user_id,
    purchases,
    total_revenue,
    shipping
  FROM
  (
    SELECT 
      event_date,
      session_id,
      user_pseudo_id,
      platform,
      ga_session_attr,
      session_attribution,
      session_attribution_user_id,
      session_attribution_date,
      session_attribution_date_user_id,
      IF(session_attribution_date > IFNULL(session_attribution_date_user_id, 0), IF(session_attribution = '(direct) / (none)', session_attribution_user_id, session_attribution), session_attribution_user_id) AS attr_w_user_id,
      purchases,
      total_revenue,
      shipping
    FROM(
      SELECT 
        event_date,
        gn.session_id,
        platform,
        user_id,
        session_attribution,
        session_attribution_user_id,
        CAST(SPLIT(session_attribution_and_date, '-')[SAFE_OFFSET(0)] AS INT64) AS session_attribution_date,
        CAST(SPLIT(session_attribution_user_id_and_date, '-')[SAFE_OFFSET(0)] AS INT64) AS session_attribution_date_user_id,
        ga_session_attr,
        gn.user_pseudo_id,
        -- gn.user_id,
        purchases,
        total_revenue,
        shipping
      FROM general_info gn
      LEFT JOIN join_purchase_data att
      USING (session_id)
      -- WHERE event_date = '2025-11-01'
      -- WHERE event_date BETWEEN '2025-06-01' AND '2025-12-28'
      WHERE event_date BETWEEN '2025-12-01' AND '2025-12-31' 
      -- WHERE event_date = '2025-12-28' 
    )
  )
)

SELECT 
  event_date,
  final_session_attribution,
  SUM(total_revenue) AS total_revenue,
  SUM(shipping) AS shipping,
  COUNT(DISTINCT session_id) AS sessions,
  SUM(purchases) AS purchases,
  COUNT(DISTINCT user_pseudo_id) AS users,
  session_attribution
FROM attr_revenue
WHERE event_date BETWEEN '2025-12-01' AND '2025-12-31' AND platform = 'WEB'
-- WHERE event_date = '2025-12-28' AND platform = 'WEB'
GROUP BY ALL

-- SELECT * FROM attr_revenue WHERE session_attribution != session_attribution_user_id AND session_attribution_user_id != '(direct) / (none)'

-- SELECT * FROM first_attr 
