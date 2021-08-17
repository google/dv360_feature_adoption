/**
 * Copyright 2021 Google LLC
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

'use strict';

const unzipper = require('unzipper');
const csv = require('fast-csv');
const {GoogleAuth} = require('google-auth-library');
const {BigQuery} = require('@google-cloud/bigquery');
const {google} = require('googleapis');

const wait = (ms) => new Promise((res) => setTimeout(res, ms));

const DATASET_NAME = 'dv360_feature_adoption';
const DOUBLECLICKBIDMANAGER_API_VERSION = 'v1.1';
const DISPLAYVIDEO_API_VERSION = 'v1';
const SDF_VERSION = 'SDF_VERSION_5_3';

const REQUIRED_API_SCOPES = [
  'https://www.googleapis.com/auth/display-video',
  'https://www.googleapis.com/auth/doubleclickbidmanager',
];

exports.DisplayVideo360FeatureAdoption = async (req, res) => {
  const advertiserId = req.query.advertiserId || req.body.advertiserId;

  if (!advertiserId) {
    throw Error('Advertiser ID is required.');
  }

  const auth = new GoogleAuth({scopes: REQUIRED_API_SCOPES});
  const client = await auth.getClient();
  google.options({auth: client});

  const createReportResponse = await google
    .doubleclickbidmanager(DOUBLECLICKBIDMANAGER_API_VERSION)
    .queries.createquery({
      asynchronous: true,
      requestBody: {
        kind: 'doubleclickbidmanager#query',
        metadata: {
          dataRange: 'LAST_7_DAYS',
          title: 'DV360 Feature Adoption Report',
          sendNotification: false,
          format: 'CSV',
        },
        params: {
          type: 'TYPE_GENERAL',
          filters: [
            {
              type: 'FILTER_ADVERTISER',
              value: advertiserId,
            },
          ],
          metrics: [
            'METRIC_IMPRESSIONS',
            'METRIC_BILLABLE_IMPRESSIONS',
            'METRIC_CLICKS',
            'METRIC_CTR',
            'METRIC_TOTAL_CONVERSIONS',
            'METRIC_REVENUE_USD',
            'METRIC_MEDIA_COST_USD',
          ],
        },
        schedule: {
          frequency: 'ONE_TIME',
        },
      },
    });

  let isReportDone = false;
  let googleCloudStoragePathForLatestReport;

  while (!isReportDone) {
    const getReportResponse = await google.doubleclickbidmanager(DOUBLECLICKBIDMANAGER_API_VERSION).queries.getquery({
      queryId: createReportResponse.data.queryId,
    });

    googleCloudStoragePathForLatestReport = getReportResponse.data.metadata.googleCloudStoragePathForLatestReport;
    isReportDone = (googleCloudStoragePathForLatestReport || '').length > 0;

    if (!isReportDone) {
      await wait(30);
    }
  }

  console.log(googleCloudStoragePathForLatestReport);

  const createSdfResponse = await google.displayvideo(DISPLAYVIDEO_API_VERSION).sdfdownloadtasks.create({
    requestBody: {
      version: SDF_VERSION,
      advertiserId,
      parentEntityFilter: {fileType: ['FILE_TYPE_LINE_ITEM'], filterType: 'FILTER_TYPE_NONE'},
    },
  });

  let isSdfDone = false;
  let getSdfResponse;

  while (!isSdfDone) {
    getSdfResponse = await google.displayvideo(DISPLAYVIDEO_API_VERSION).sdfdownloadtasks.operations.get({
      name: createSdfResponse.data.name,
    });

    isSdfDone = getSdfResponse.data.done;

    if (!isSdfDone) {
      await wait(30);
    }
  }

  const mediaResponse = await google.displayvideo(DISPLAYVIDEO_API_VERSION).media.download(
    {
      resourceName: getSdfResponse.data.response.resourceName,
      alt: 'media',
    },
    {
      responseType: 'stream',
    },
  );

  const bq = new BigQuery();
  const [dataset] = await bq.dataset(DATASET_NAME).get({autoCreate: true});
  const [table] = await dataset.table('sdfs').get({
    autoCreate: true,
    schema:
      'line_item_id: integer, line_item_type: string, pacing_type: string, bid_strategy_type: string, budget_type: string, is_audience_targeting: boolean, is_similar_audiences: boolean, is_affinity_inmarket: boolean, is_frequency_enabled: boolean, active_view: string, is_geography_targeting: boolean, is_language_targeting: boolean, digital_content_labels: string, brand_safety_sensitivity_setting: string, is_site_targeting: boolean, imported_at: date, advertiser_id: integer',
    location: 'US',
    timePartitioning: {
      type: 'DAY',
      field: 'imported_at',
    },
  });

  const mediaResponseStream = mediaResponse.data;
  const rows = [];
  mediaResponseStream
    .pipe(unzipper.ParseOne())
    .pipe(csv.parse({headers: true}))
    .transform((data) => ({
      line_item_id: data['Line Item Id'],
      line_item_type: data['Type'],
      pacing_type: data['Pacing'],
      bid_strategy_type: data['Bid Strategy Type'],
      budget_type: data['Budget Type'],
      is_audience_targeting: (data['Audience Targeting - Include'] || '').length > 0,
      is_similar_audiences: (data['Audience Targeting - Similar Audiences'] || '').toLowerCase().trim() === 'true',
      is_affinity_inmarket: (data['Affinity & In Market Targeting - Include'] || '').toLowerCase().trim() === 'true',
      is_frequency_enabled: (data['Frequency Enabled'] || '').toLowerCase().trim() === 'true',
      active_view: data['Viewability Targeting Active View'],
      is_geography_targeting: (data['Geography Targeting - Include'] || '').length > 0,
      is_language_targeting: (data['Language Targeting - Include'] || '').length > 0,
      digital_content_labels: data['Digital Content Labels - Exclude'],
      brand_safety_sensitivity_setting: data['Brand Safety Sensitivity Setting'],
      is_site_targeting: (data['Site Targeting - Include'] || '').length > 0,
      imported_at: new Date().toJSON().slice(0, 10),
      advertiser_id: advertiserId,
    }))
    .on('data', (row) => rows.push(row))
    .on('end', async () => {
      table.insert(rows);
    });

  return res.status(200).json({success: true, message: 'Done.'});
};
