
# Medallion Architecture Development in GCP

This project implements a comprehensive data processing pipeline using Apache Spark on Google Cloud Dataproc to transform and aggregate sales data across three layers in Google Cloud Storage (GCS): Raw, Bronze, Silver, and Gold. The pipeline processes raw transactional and customer data, cleans and structures it into Bronze, refines it into Silver for analytical readiness, and finally aggregates it into a Gold layer for business intelligence purposes.


## Installation

To set up and run this project locally or on Dataproc, follow these steps:

Clone the Repository:

```bash
bash

git clone <repository-url>
cd <repository-folder>
```

Install Python Dependencies:
 Ensure Python 3.7+ is installed.

Install required Python packages:
```bash
bash

pip install pyspark google-cloud-storage
```
Set Up Google Cloud SDK:
 Install the Google Cloud SDK if not already installed:

```bash
bash

curl -O https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-cli-407.0.0-linux-x86_64.tar.gz
tar -xzf google-cloud-cli-407.0.0-linux-x86_64.tar.gz
./google-cloud-sdk/install.sh
```

Authenticate with Google Cloud:

```bash
bash

gcloud auth login
gcloud config set project project1-gcp-451521
```
Ensure GCS Access:
 Ensure the Dataproc service account or your user account has read/write permissions on gs://project1-gcp1/Raw/, Bronze/, Silver/, and Gold/.

## Configuration

The project uses a `config.json` file to specify GCS paths and optional parameters. Create or update config.json with the following structure:

```bash
{
  "raw": {
    "raw_path": "gs://project1-gcp1/Raw"
  },
  "bronze": {
    "bronze_path": "gs://project1-gcp1/Bronze"
  },
  "silver": {
    "silver_path": "gs://project1-gcp1/Silver"
  },
  "gold": {
    "gold_path": "gs://project1-gcp1/Gold"
  },
  "expected_states": ["CA", "FL", "GA", "IL", "MA", "NC", "NY", "OH", "TX", "WA"],
  "default_state": "Unknown"
}
```
  raw_path: GCS path to the Raw layer (e.g., gs://project1-gcp1/Raw).
  bronze_path: GCS path to the Bronze layer (e.g., gs://project1-gcp1/Bronze).
  silver_path: GCS path to the Silver layer (e.g., gs://project1-gcp1/Silver).
  gold_path: GCS path to the Gold layer (e.g., gs://project1-gcp1/Gold).
  expected_states: List of expected state abbreviations for validation (optional, defaults to ["CA", "FL", "GA"]).
  default_state: Default state for unmatched or null customer_state values (optional, defaults to "Unknown").

Save config.json in the same directory as your scripts or upload it to GCS for Dataproc jobs.

## Spinning Up a Dataproc Cluster
To run the scripts on Google Cloud Dataproc, you need to create and manage a Dataproc cluster. 
Here’s an example:

### Create a Dataproc Cluster:

```bash
bash

gcloud dataproc clusters create my-cluster \
  --region=us-west2 \
  --zone=us-west2-a \
  --master-machine-type=e2-medium \
  --worker-machine-type=e2-medium \
  --num-workers=2 \
  --project=project1-gcp-451521

```
Adjust machine types, number of workers, and region/zone based on your needs and budget.

### Verify the Cluster:

```bash
bash

gcloud dataproc clusters list --region=us-west2
```


## Executing the Scripts on Dataproc

The pipeline consists of three scripts (or one script with stages) to process data from Raw to Bronze, Bronze to Silver, and Silver to Gold. Below are examples for each stage:

### Raw to Bronze

Purpose: Read raw CSV files, clean, deduplicate, and store in Bronze.

```bash
bash

gcloud dataproc jobs submit pyspark gs://project1-gcp1/scripts/raw_to_bronze.py \
  --cluster=my-cluster \
  --id=RawToBronze-job \
  --region=us-west2 \
  --files=gs://project1-gcp1/scripts/config.json
```

### Bronze to Silver

Purpose: Read cleaned Bronze data, apply transformations (e.g., standardization, enrichment), and store in Silver.

```bash
bash

gcloud dataproc jobs submit pyspark gs://project1-gcp1/scripts/bronze_to_silver.py \
  --cluster=my-cluster \
  --id=BronzeToSilver-job \
  --region=us-west2 \
  --files=gs://project1-gcp1/scripts/config.json
```

### Silver to Gold

Purpose: Read Silver data, filter out cancelled orders, aggregate sales data, and write to Gold partitioned by state and date.

```bash
bash

gcloud dataproc jobs submit pyspark gs://project1-gcp1/scripts/loadtogold.py \
  --cluster=my-cluster \
  --id=Gold-job \
  --region=us-west2 \
  --files=gs://project1-gcp1/scripts/config.json
```

## Monitoring and varification

### Monitor the Job:

```bash
bash

gcloud dataproc jobs wait Gold-job --region us-west2 --project project1-gcp-451521
```
### View Logs:

Check logs in the Google Cloud Console or GCS

```bash
bash

https://console.cloud.google.com/dataproc/jobs/Gold-job?project=project1-gcp-451521®ion=us-west2
gs://project1-gcp1/google-cloud-dataproc-metainfo/93e672f1-ecfb-4058-8226-8dda5c4865e6/jobs/Gold-job/driveroutput.*
```
### Verify Output:

Check each layer in GCS for the output:

```bash
bash

gsutil ls gs://project1-gcp1/Bronze/
gsutil ls gs://project1-gcp1/Silver/
gsutil ls gs://project1-gcp1/Gold/fact_sales/
```
