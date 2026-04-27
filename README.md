# GCP Patient Vitals Streaming Medallion Pipeline

Event-driven healthcare streaming pipeline built on Google Cloud using Pub/Sub, Apache Beam/Dataflow, Medallion Architecture (Bronze/Silver/Gold), Cloud Storage, and BigQuery for patient risk monitoring.

## Architecture

```text
Patient Vital Simulator (Publisher)
        ↓
Google Pub/Sub Topic
        ↓
Pub/Sub Subscription
        ↓
Apache Beam + Dataflow Streaming Pipeline
        ↓
Bronze Layer (Raw JSON in GCS)
        ↓
Silver Layer (Validated + Risk Enriched JSON in GCS)
        ↓
Gold Layer (Patient Aggregates in BigQuery)
```

## Features

- Dynamic Pub/Sub topic and subscription creation
- Real-time patient vitals event simulation
- Streaming ingestion with Apache Beam on Dataflow
- Medallion architecture:
  - Bronze → raw events
  - Silver → validated and enriched records
  - Gold → aggregated patient risk analytics
- Rule-based patient risk scoring (Low / Moderate / High)
- BigQuery analytics layer
- Error injection for malformed records
- IAM setup using service accounts and least-privilege principles

## Tech Stack

- Python
- Google Cloud Pub/Sub
- Apache Beam
- Google Cloud Dataflow
- Google Cloud Storage
- BigQuery
- Cloud Shell

## Example Event

```json
{
  "patient_id": "P004",
  "heart_rate": 105,
  "spo2": 92,
  "temperature": 38.6,
  "bp_systolic": 132,
  "bp_diastolic": 84
}
```

## Risk Scoring Logic

Patient risk is classified using rule-based thresholds:

- Low → normal vitals
- Moderate → one abnormal vital
- High → multiple abnormal vitals

## Project Structure

```text
patient-vital-monitoring/
├── simulator/
│   ├── patient_vital_simulator.py
│   └── .env.example
│
├── dataflow/
│   ├── streaming_medallion_pipeline.py
│   └── .env.example
│
├── requirements.txt
├── README.md
└── .gitignore
```

## Setup

Install dependencies:

```bash
pip install -r requirements.txt
```

Load environment variables:

```bash
export $(grep -v '^#' .env | xargs)
```

## Run Publisher

```bash
python simulator/patient_vital_simulator.py
```

## Start Dataflow Streaming Job

```bash
python -m streaming_medallion_pipeline \
  --runner DataflowRunner \
  --project "$GCP_PROJECT" \
  --region "$REGION" \
  --staging_location "$STAGING_PATH" \
  --temp_location "$TEMP_PATH" \
  --streaming
```

## Sample BigQuery Query

```sql
SELECT
  patient_id,
  risk_level,
  avg_heart_rate,
  avg_spo2
FROM healthcare.patient_risk_analytics_v2
LIMIT 20;
```

## Future Improvements

- Dead-letter topic for failed records
- Schema registry / validation
- Streaming alerts for high-risk patients
- FHIR-style healthcare event modeling
- CI/CD deployment with Terraform
- Model-based risk scoring

## Why This Project

This project simulates a claims-style event-driven healthcare data pipeline similar to architectures used in payer and pharmacy benefit environments for streaming ingestion, data quality, patient monitoring, and downstream analytics.

## License

MIT
