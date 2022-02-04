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
  is_site_targeting
