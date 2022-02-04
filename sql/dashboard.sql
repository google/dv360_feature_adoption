/**
 * Copyright 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

SELECT
  r.reported_at AS reported_at,
  r.advertiser_id AS advertiser_id,
  r.line_item_id AS line_item_id,
  r.impressions AS impressions,
  r.billable_impressions AS billable_impressions,
  r.clicks AS clicks,
  r.click_rate AS click_rate,
  r.total_conversions AS total_conversions,
  r.last_clicks AS last_clicks,
  r.last_impressions AS last_impressions,
  r.revenue_usd AS revenue_usd,
  r.media_cost_usd AS media_cost_usd,
  s.line_item_type AS line_item_type,
  s.pacing_type AS pacing_type,
  s.bid_strategy_type AS bid_strategy_type,
  s.budget_type AS budget_type,
  s.is_audience_targeting AS is_audience_targeting,
  s.is_similar_audiences AS is_similar_audiences,
  s.is_affinity_inmarket AS is_affinity_inmarket,
  s.is_frequency_enabled AS is_frequency_enabled,
  s.active_view AS active_view,
  s.is_geography_targeting AS is_geography_targeting,
  s.is_language_targeting AS is_language_targeting,
  s.digital_content_labels AS digital_content_labels,
  s.brand_safety_sensitivity_setting AS brand_safety_sensitivity_setting,
  s.is_site_targeting AS is_site_targeting
FROM (
  SELECT
    MAX(imported_at) AS last_imported_at,
    reported_at,
    advertiser_id,
    line_item_id,
    impressions,
    billable_impressions,
    clicks,
    click_rate,
    total_conversions,
    last_clicks,
    last_impressions,
    revenue_usd,
    media_cost_usd
  FROM
    `<PROJECT_ID>.dv360_feature_adoption.reports`
  GROUP BY
    reported_at,
    advertiser_id,
    line_item_id,
    impressions,
    billable_impressions,
    clicks,
    click_rate,
    total_conversions,
    last_clicks,
    last_impressions,
    revenue_usd,
    media_cost_usd ) AS r
LEFT JOIN (
  SELECT
    MAX(imported_at) AS last_imported_at,
    advertiser_id,
    line_item_id,
    line_item_type,
    pacing_type,
    bid_strategy_type,
    budget_type,
    is_audience_targeting,
    is_similar_audiences,
    is_affinity_inmarket,
    is_frequency_enabled,
    active_view,
    is_geography_targeting,
    is_language_targeting,
    digital_content_labels,
    brand_safety_sensitivity_setting,
    is_site_targeting
  FROM
    `<PROJECT_ID>.dv360_feature_adoption.sdfs`
  GROUP BY
    advertiser_id,
    line_item_id,
    line_item_type,
    pacing_type,
    bid_strategy_type,
    budget_type,
    is_audience_targeting,
    is_similar_audiences,
    is_affinity_inmarket,
    is_frequency_enabled,
    active_view,
    is_geography_targeting,
    is_language_targeting,
    digital_content_labels,
    brand_safety_sensitivity_setting,
    is_site_targeting ) AS s
ON
  r.advertiser_id = s.advertiser_id
  AND r.line_item_id = s.line_item_id
