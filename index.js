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

'use strict';

import {ParseOne as unzip} from 'unzipper';
import {parse} from 'fast-csv';
import {GoogleAuth} from 'google-auth-library';
import {BigQuery} from '@google-cloud/bigquery';
import {google} from 'googleapis';
import got from 'got';

const wait = (ms) => new Promise((res) => setTimeout(res, ms));

const DATASET_NAME = 'dv360_feature_adoption';
const DOUBLECLICKBIDMANAGER_API_VERSION = 'v1.1';
const DISPLAYVIDEO_API_VERSION = 'v1';
const SDF_VERSION = 'SDF_VERSION_5_4';

const REQUIRED_API_SCOPES = [
  'https://www.googleapis.com/auth/display-video',
  'https://www.googleapis.com/auth/doubleclickbidmanager',
];

/**
 * A Cloud Function for Display & Video 360 Feature Adoption report.
 * @param {Request} req The request.
 * @param {Response} res The response.
 * @return {Response} The response.
 */
export async function DisplayVideo360FeatureAdoptionReport(req, res) {
  try {
    const advertiserId = req.query.advertiserId || req.body.advertiserId;
    const dataRange = req.query.dataRange || req.body.dataRange || 'LAST_7_DAYS';

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
              dataRange,
              title: `DV360 Feature Adoption Report for Advertiser: ${advertiserId}`,
              sendNotification: false,
              format: 'CSV',
            },
            params: {
              type: 'TYPE_GENERAL',
              groupBys: [
                'FILTER_DATE',
                'FILTER_INSERTION_ORDER',
                'FILTER_LINE_ITEM',
                'FILTER_LINE_ITEM_STATUS',
                'FILTER_DEVICE_TYPE',
              ],
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
                'METRIC_LAST_CLICKS',
                'METRIC_LAST_IMPRESSIONS',
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
      const getReportResponse = await google
          .doubleclickbidmanager(DOUBLECLICKBIDMANAGER_API_VERSION)
          .queries.getquery({
            queryId: createReportResponse.data.queryId,
          });

      googleCloudStoragePathForLatestReport =
      getReportResponse.data.metadata.googleCloudStoragePathForLatestReport;
      isReportDone = (googleCloudStoragePathForLatestReport || '').length > 0;

      if (!isReportDone) {
        await wait(30);
      }
    }

    console.log('REPORT URL:', googleCloudStoragePathForLatestReport);

    const bq = new BigQuery();
    const [dataset] = await bq.dataset(DATASET_NAME).get({autoCreate: true});
    const [table] = await dataset.table('reports').get({
      autoCreate: true,
      schema: `imported_at: date, reported_at: date, advertiser_id: integer, insertion_order_id: integer, line_item_id: integer, line_item_status: string, device_type: string, impressions: integer, billable_impressions: string, clicks: integer, click_rate: string, total_conversions: string, last_clicks: string, last_impressions: string, revenue_usd: string, media_cost_usd: string`,
      location: 'US',
      timePartitioning: {
        type: 'DAY',
        field: 'imported_at',
      },
    });

    const rows = [];

    got.stream(googleCloudStoragePathForLatestReport)
        .pipe(parse({headers: true}))
        .transform((data) => ({
          imported_at: new Date().toJSON().slice(0, 10),
          reported_at: data['Date'].replace(/\//g, '-'),
          advertiser_id: advertiserId,
          insertion_order_id: data['Insertion Order ID'],
          line_item_id: data['Line Item ID'],
          line_item_status: data['Line Item Status'],
          device_type: data['Device Type'],
          impressions: data['Impressions'],
          billable_impressions: data['Billable Impressions'],
          clicks: data['Clicks'],
          click_rate: data['Click Rate (CTR)'],
          total_conversions: data['Total Conversions'],
          last_clicks: data['Post-Click Conversions'],
          last_impressions: data['Post-View Conversions'],
          revenue_usd: data['Revenue (USD)'],
          media_cost_usd: data['Media Cost (USD)'],
        }))
        .on('error', (err) => reject(err))
        .on('data', (row) => {
          if (!/\d\d\d\d-\d\d-\d\d/.test(row['reported_at'])) {
            return;
          }

          rows.push(row);
        })
        .on('end', async () => {
          try {
            await table.insert(rows);

            return res.status(200).json({
              success: true,
              message: `${rows.length} rows created in the reports table for advertiser ID: ${advertiserId}.`,
            });
          } catch (err) {
            console.error(err);
            res.status(500).send(err);
          }
        });
  } catch (err) {
    console.error(err);
    res.status(500).send(err);
  }
}

/**
 * A Cloud Function for Display & Video 360 Feature Adoption SDF.
 * @param {Request} req The request.
 * @param {Response} res The response.
 * @return {Response} The response.
 */
export async function DisplayVideo360FeatureAdoptionSdf(req, res) {
  try {
    const advertiserId = req.query.advertiserId || req.body.advertiserId;

    if (!advertiserId) {
      throw Error('Advertiser ID is required.');
    }

    const auth = new GoogleAuth({scopes: REQUIRED_API_SCOPES});
    const client = await auth.getClient();
    google.options({auth: client});

    const createSdfResponse = await google
        .displayvideo(DISPLAYVIDEO_API_VERSION)
        .sdfdownloadtasks.create({
          requestBody: {
            version: SDF_VERSION,
            advertiserId,
            parentEntityFilter: {
              fileType: ['FILE_TYPE_LINE_ITEM'],
              filterType: 'FILTER_TYPE_NONE',
            },
          },
        });

    let isSdfDone = false;
    let getSdfResponse;

    while (!isSdfDone) {
      getSdfResponse = await google
          .displayvideo(DISPLAYVIDEO_API_VERSION)
          .sdfdownloadtasks.operations.get({
            name: createSdfResponse.data.name,
          });

      isSdfDone = getSdfResponse.data.done;

      if (!isSdfDone) {
        await wait(30);
      }
    }

    const mediaResponse = await google
        .displayvideo(DISPLAYVIDEO_API_VERSION)
        .media.download(
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
      schema: `line_item_id: integer, insertion_order_id: integer, line_item_type: string, pacing_type: string, bid_strategy_type: string, budget_type: string, is_audience_targeting: boolean, is_similar_audiences: boolean, is_affinity_inmarket: boolean, is_frequency_enabled: boolean, active_view: string, is_geography_targeting: boolean, is_language_targeting: boolean, digital_content_labels: string, brand_safety_sensitivity_setting: string, is_site_targeting: boolean, imported_at: date, advertiser_id: integer`,
      location: 'US',
      timePartitioning: {
        type: 'DAY',
        field: 'imported_at',
      },
    });

    const mediaResponseStream = mediaResponse.data;

    const rows = [];

    mediaResponseStream
        .pipe(unzip())
        .pipe(parse({headers: true}))
        .transform((data) => ({
          line_item_id: data['Line Item Id'],
          insertion_order_id: data['Io Id'],
          line_item_type: data['Type'],
          pacing_type: data['Pacing'],
          bid_strategy_type: data['Bid Strategy Type'],
          budget_type: data['Budget Type'],
          is_audience_targeting:
                (data['Audience Targeting - Include'] || '').length > 0,
          is_similar_audiences:
                (data['Audience Targeting - Similar Audiences'] || '')
                    .toLowerCase()
                    .trim() === 'true',
          is_affinity_inmarket:
                (data['Affinity & In Market Targeting - Include'] || '')
                    .toLowerCase()
                    .trim() === 'true',
          is_frequency_enabled:
                (data['Frequency Enabled'] || '').toLowerCase().trim() === 'true',
          active_view: data['Viewability Targeting Active View'],
          is_geography_targeting:
                (data['Geography Targeting - Include'] || '').length > 0,
          is_language_targeting:
                (data['Language Targeting - Include'] || '').length > 0,
          digital_content_labels: data['Digital Content Labels - Exclude'],
          brand_safety_sensitivity_setting:
                data['Brand Safety Sensitivity Setting'],
          is_site_targeting: (data['Site Targeting - Include'] || '').length > 0,
          imported_at: new Date().toJSON().slice(0, 10),
          advertiser_id: advertiserId,
        }))
        .on('error', (err) => reject(err))
        .on('data', (row) => rows.push(row))
        .on('end', async () => {
          try {
            await table.insert(rows);

            return res.status(200).json({
              success: true,
              message: `${rows.length} rows created in the SDFs table for advertiser ID: ${advertiserId}.`,
            });
          } catch (err) {
            console.error(err);
            res.status(500).send(err);
          }
        });
  } catch (err) {
    console.error(err);
    res.status(500).send(err);
  }
}
