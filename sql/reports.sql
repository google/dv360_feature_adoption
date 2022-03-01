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
  MAX(imported_at) AS last_imported_at,
  reported_at,
  advertiser_id,
  insertion_order_id,
  line_item_id,
  line_item_status,
  device_type,
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
  insertion_order_id,
  line_item_id,
  line_item_status,
  device_type,
  impressions,
  billable_impressions,
  clicks,
  click_rate,
  total_conversions,
  last_clicks,
  last_impressions,
  revenue_usd,
  media_cost_usd
