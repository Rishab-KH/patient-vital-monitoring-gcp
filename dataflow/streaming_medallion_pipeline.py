import os
import json
from dotenv import load_dotenv

load_dotenv()

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.transforms.window import FixedWindows

PROJECT_ID = os.getenv("GCP_PROJECT")
PUBSUB_SUBSCRIPTION = os.getenv("PUBSUB_SUBSCRIPTION")
BRONZE_PATH = os.getenv("BRONZE_PATH")
SILVER_PATH = os.getenv("SILVER_PATH")
STAGING_PATH = os.getenv("STAGING_PATH")
TEMP_PATH = os.getenv("TEMP_PATH")
BQ_TABLE = os.getenv("BIGQUERY_TABLE")
REGION = os.getenv("REGION")

pipeline_options = PipelineOptions(
    project=PROJECT_ID,
    streaming=True,
    runner="DataflowRunner",
    temp_location=TEMP_PATH,
    staging_location=STAGING_PATH,
    region=REGION
)

pipeline_options.view_as(StandardOptions).streaming = True

def parse_json(element):
    try:
        return json.loads(element)
    except json.JSONDecodeError:
        print(f"Invalid JSON: {element}")
        return None

def data_quality(record):
    try:
        if record is None:
            return False
        p_id = record.get("patient_id")
        hr = record.get("heart_rate")
        spo2 = record.get("spo2")
        temp = record.get("temperature")
        bp_sys = record.get("bp_systolic")
        bp_dia = record.get("bp_diastolic")

        if p_id is None or hr is None or spo2 is None or temp is None or bp_sys is None or bp_dia is None:
            return False
        
        if not (0 < spo2 <= 100):
            return False
        if not (0 < hr < 200):
            return False
        if not (30 <= temp <= 45):
            return False

        return True

    except Exception as ex:
        print(f"Invalid record: {record} with error: {ex}")
        return False


def calculate_risk(record):
    
    hr = record["heart_rate"]
    spo2 = record["spo2"]
    temp = record["temperature"]

    risk_score = 0

    if hr < 60 or hr > 100:
        risk_score += 0.35

    if spo2 < 94:
        risk_score += 0.35

    if temp < 36.0 or temp > 38.0:
        risk_score += 0.30

    record["risk_score"] = risk_score
    record["risk_model_version"] = "v2_rule_based"

    if risk_score < 0.3:
        record["risk_level"] = "Low"
    elif risk_score < 0.6:
        record["risk_level"] = "Moderate"
    else:
        record["risk_level"] = "High"

    return record


# ---pipeline---

with beam.Pipeline(options=pipeline_options) as p:

    # Raw data to Bronze Layer
    bronze_data = (
        p
        | "Read from PubSub" >> beam.io.ReadFromPubSub(subscription=PUBSUB_SUBSCRIPTION)
        | "Parse JSON Decode string" >> beam.Map(lambda x: x.decode('utf-8'))
        | "Batch into Bronze Layer" >> beam.WindowInto(FixedWindows(60)) # Batch into windows of 1 min
    )
    
    # Write Raw Message To GCS Bronze Bucket

    bronze_data | "Write to Bronze Bucket" >> beam.io.WriteToText(
        BRONZE_PATH + "raw_data",
        file_name_suffix = ".json",
        shard_name_template="-SSSSS-of-NNNNN"
    )

    # Enriched data to silver layer

    silver_data = (
        bronze_data
        | "Parse JSON" >> beam.Map(parse_json)
        | "Data Quality" >> beam.Filter(data_quality)
        | "Calculate Risk" >> beam.Map(calculate_risk)
    )

    # Write enriched data to GCS Silver Bucket

    silver_data |  "Silver to JSON String" >> beam.Map(json.dumps) \
        | "Write to Silver Bucket" >> beam.io.WriteToText(
        SILVER_PATH + "cleaned_data",
        file_name_suffix = ".json",
        shard_name_template="-SSSSS-of-NNNNN"
    )

    # Gold Layer

    def extract_for_aggregation(record):
        return (record["patient_id"], record)

    def aggregate_records(key_values):
        patient_id, records = key_values
        count = len(records)
        avg_heart_rate = sum(r["heart_rate"] for r in records) / count
        avg_spo2 = sum(r["spo2"] for r in records) / count
        avg_temp = sum(r["temperature"] for r in records) / count
        risk_levels = [r['risk_level'] for r in records]

        if "High" in risk_levels:
            risk_level = "High"
        elif "Moderate" in risk_levels:
            risk_level = "Moderate"
        else:
            risk_level = "Low"
        
        return {
            "patient_id": patient_id,
            "count": count,
            "avg_heart_rate": avg_heart_rate,
            "avg_spo2": avg_spo2,
            "avg_temp": avg_temp,
            "risk_level": risk_level
        }

    # Creating Curated Dataset

    gold_data = (
        silver_data 
        | "Key by patient_id" >> beam.Map(extract_for_aggregation)
        | "Group by patient_id" >> beam.GroupByKey()
        | "Aggregate per patient" >> beam.Map(aggregate_records)
    )

    # Write Gold to BigQuery

    gold_data | "Write to BigQuery" >> beam.io.WriteToBigQuery(
    table=BQ_TABLE,
    schema={
        "fields": [
            {"name": "patient_id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "count", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "avg_heart_rate", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "avg_spo2", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "avg_temp", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "risk_level", "type": "STRING", "mode": "NULLABLE"},
        ]
    },
    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
)