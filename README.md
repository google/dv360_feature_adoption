# Display & Video 360 Feature Adoption

**This is not an officially supported Google product. It is a reference implementation.**

## Overview

This is a reference implementation for a solution that will pull the necessary SDF and reports needed from Display & Video 360 into BigQuery to create a feature adoption dataset.

The idea is to deploy a single Cloud Function and schedule jobs for individual Display & Video 360 advertisers to generate a feature adoption dataset every 24 hours for analysis.

## Prerequisites

- [Git](https://git-scm.com)
- [Node.js](https://nodejs.org)
- [Cloud SDK](https://cloud.google.com/sdk/docs/install)

## Deployment

1.  Clone this repository:

        git clone https://github.com/google/dv360_feature_adoption.git

2.  Change into the repository's directory:

        cd dv360_feature_adoption

3.  Install the dependencies:

        npm install

4.  Create a Google Cloud project (skip if using an existing project):

        gcloud projects create dv360-feature-adoption

5.  Enable the Cloud Build, Cloud Functions, Display & Video 360, BigQuery, and Cloud Storage APIs:

        gcloud services enable cloudbuild.googleapis.com cloudfunctions.googleapis.com doubleclickbidmanager.googleapis.com displayvideo.googleapis.com storage.googleapis.com bigquery.googleapis.com

6.  Create a Service Account for authenticating:

        gcloud iam service-accounts create dv360-feature-adoption \
            --display-name="Display & Video 360 Feature Adoption"

    _You may need to grant this new service account the BigQuery Admin role in the Cloud console._

7.  Add the roles needed to run the functions:

        gcloud functions add-iam-policy-binding DisplayVideo360FeatureAdoption --region=us-central1 --member=allUsers --role=roles/cloudscheduler.jobRunner
        gcloud functions add-iam-policy-binding DisplayVideo360FeatureAdoption --region=us-central1 --member=allUsers --role=roles/cloudfunctions.invoker
        gcloud functions add-iam-policy-binding DisplayVideo360FeatureAdoption --region=us-central1 --member=allUsers --role=roles/bigquery.admin

8.  Create a Display & Video 360 User Profile using the service account's email address in the [UI](https://displayvideo.google.com) and give the user access to the Advertiser IDs you plan on scheduling jobs for.

9.  Deploy the provided Cloud Function to your Google Cloud project:

        gcloud functions deploy DisplayVideo360FeatureAdoption \
            --runtime nodejs14 \
            --memory 1GB \
            --timeout 540s \
            --trigger-http \
            --service-account "[SERVICE_ACCOUNT_EMAIL]"

    _Replace `[SERVICE_ACCOUNT_EMAIL]` with the email generated while creating the Service Account._

## Scheduling

1.  Create a **Scheduler Job** with:

        gcloud scheduler jobs create http dv360-feature-adoption-job-[ADVERTISER_ID] \
            --schedule "0 0 * * *" \
            --uri "[CLOUD_FUNCTION_URI]" \
            --http-method POST \
            --message-body='{ "advertiserId": [ADVERTISER_ID] }'

    _Replace `[CLOUD_FUNCTION_URI]` with the deployed URI for the Cloud Function and replace all instances of `[ADVERTISER_ID]` with the Display & Video 360 Advertiser ID._

## Testing

1.  Run the function locally:

        npm start

2.  Call the function:

        curl -X "POST" "http://localhost:8080" \
            -H 'Content-Type: application/json; charset=utf-8' \
            -d $'{ "advertiserId": [ADVERTISER_ID] }'

## Authors

- Tony Coconate (coconate@google.com) – Google
