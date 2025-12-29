#!/usr/bin/env python3
"""
xtract_e2e_full.py

End-to-end: trigger XTRACT -> wait run -> wait stable parquet -> upload to GCS -> create BQ external table -> optional materialize -> delete local files.

Default XTRACT server base is set to:
  http://acew1pxtraxus01:8085
"""

import os
import sys
import json
import time
import argparse
import logging
from pathlib import Path
from datetime import datetime
import requests

import pyarrow.parquet as pq
import pyarrow as pa
from google.cloud import storage, bigquery
from google.oauth2 import service_account

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

DEFAULT_XTRACT_SERVER = "http://acew1pxtraxus01:8085"


# ---------------------------
# Helper: build URLs
# ---------------------------

def normalize_server(server: str) -> str:
    return server.rstrip("/")


def build_start_url_from_server(server: str, job_name: str) -> str:
    server = normalize_server(server)
    return f"{server}/start/{job_name}/"


def build_logs_url(server: str, job_name: str) -> str:
    server = normalize_server(server)
    return f"{server}/logs/extractions/{job_name}"


# ---------------------------
# Trigger XTRACT and extract webServerLog
# ---------------------------

def trigger_xtract_job(job_url: str):
    """
    Trigger XTRACT using start URL. Expect JSON with fields including:
      - extractionName
      - webLogTimestamp or webServerLog
    Returns: (extractionName, webServerLogTimestamp)
    """
    logging.info("Triggering XTRACT start URL: %s", job_url)
    r = requests.get(job_url)
    try:
        r.raise_for_status()
    except Exception:
        logging.error("Start request failed: status=%s body=%s", r.status_code, r.text)
        raise

    try:
        data = r.json()
    except ValueError:
        raise RuntimeError(f"XTRACT start returned non-JSON: {r.text}")

    # XTRACT instances vary: check common keys
    job_name = data.get("extractionName") or data.get("name") or data.get("jobName")
    web_ts = data.get("webServerLog") or data.get("webLogTimestamp") or data.get("webServerTimestamp")

    if not job_name or not web_ts:
        raise RuntimeError(f"XTRACT start response missing expected fields: {data}")

    logging.info("XTRACT start response: extractionName=%s webServerLog=%s", job_name, web_ts)
    return job_name, web_ts


# ---------------------------
# Find real timestamp (startedAt) by matching webServerLog in logs/extractions
# ---------------------------

def find_real_timestamp_from_logs(server: str, job_name: str, web_ts: str, timeout: int = 60, poll_interval: int = 2):
    """
    Poll /logs/extractions/{job_name} until a run whose webServerLog equals web_ts appears.
    Return that run's 'startedAt' timestamp (string).
    """
    logs_url = build_logs_url(server, job_name)
    logging.info("Polling extraction logs: %s to find run with webServerLog=%s", logs_url, web_ts)
    start = time.time()

    while True:
        resp = requests.get(logs_url)
        try:
            resp.raise_for_status()
        except Exception:
            logging.warning("Logs endpoint returned %s: %s", resp.status_code, resp.text)
            # keep retrying until timeout
            if time.time() - start > timeout:
                raise RuntimeError("Failed to query logs endpoint within timeout.")
            time.sleep(poll_interval)
            continue

        try:
            payload = resp.json()
        except ValueError:
            logging.warning("Non-JSON response from logs endpoint: %s", resp.text)
            time.sleep(poll_interval)
            continue

        runs = payload.get("runs") or payload.get("data") or []
        # look for matching webServerLog (exact match)
        for run in runs:
            if run.get("webServerLog") == web_ts or run.get("webLogTimestamp") == web_ts:
                started_at = run.get("startedAt")
                logging.info("Matched run: startedAt=%s state=%s", started_at, run.get("state"))
                return started_at

        # not found yet
        if time.time() - start > timeout:
            raise TimeoutError(f"No run matching webServerLog={web_ts} found in logs after {timeout}s")
        time.sleep(poll_interval)


# ---------------------------
# Wait for the specific run to finish by polling logs/extractions
# ---------------------------

def wait_for_run_completion(server: str, job_name: str, started_at: str, poll_interval: int = 5, timeout: int = 1800):
    """
    Poll logs/extractions/{job_name} until the run with startedAt == started_at has a finished state.
    Accept common finished states containing 'Finished' or 'NoErrors'.
    """
    logs_url = build_logs_url(server, job_name)
    logging.info("Polling run status for startedAt=%s at %s", started_at, logs_url)
    start_time = time.time()

    while True:
        resp = requests.get(logs_url)
        resp.raise_for_status()
        payload = resp.json()
        runs = payload.get("runs") or []
        for run in runs:
            if run.get("startedAt") == started_at:
                state = run.get("state") or ""
                logging.info("Run state=%s", state)
                # treat states that contain 'Finished' as success (FinishedNoErrors etc.)
                if "Finished" in state or "finished" in state or "Completed" in state or "completed" in state:
                    logging.info("Run finished successfully (state=%s).", state)
                    return True
                if "Error" in state or "Failed" in state or "failed" in state:
                    raise RuntimeError(f"Run ended with failure state: {state}")
        if time.time() - start_time > timeout:
            raise TimeoutError("Timeout waiting for run to finish.")
        time.sleep(poll_interval)


# ---------------------------
# Parse XTRACT timestamp formats into datetime
# ---------------------------

def parse_xtract_timestamp(ts_str: str) -> datetime:
    """
    Parse timestamps like '2025-11-14_16:43:17.820' (ms precision)
    or '2025-11-14_16:37:37.264'. Returns naive local datetime.
    """
    fmt_base = "%Y-%m-%d_%H:%M:%S"
    if "." in ts_str:
        base, frac = ts_str.split(".", 1)
        dt = datetime.strptime(base, fmt_base)
        # fractional to microseconds
        micros = int((frac + "000000")[:6])
        dt = dt.replace(microsecond=micros)
    else:
        dt = datetime.strptime(ts_str, fmt_base)
    return dt


# ---------------------------
# Wait for Parquet files created/modified at/after job start and stable
# ---------------------------

def wait_for_output_files_by_start_time(folder: str, job_start_dt: datetime, expected_pattern: str = "*.parquet",
                                        timeout: int = 1800, poll_interval: int = 5, stable_interval: int = 3):
    """
    Wait for files in folder matching expected_pattern whose mtime >= job_start_dt (with 1s tolerance)
    and whose sizes remain stable for stable_interval seconds.
    Returns list[Path]
    """
    folder_p = Path(folder)
    if not folder_p.exists():
        raise RuntimeError(f"Output folder does not exist: {folder}")

    job_ts_epoch = job_start_dt.timestamp()
    logging.info("Waiting for parquet files in %s with mtime >= %s", folder, job_start_dt.isoformat())
    start = time.time()

    while True:
        all_candidates = list(folder_p.glob(expected_pattern))
        # filter by mtime >= job start (allow small tolerance)
        candidates = [p for p in all_candidates if p.exists() and p.stat().st_mtime >= (job_ts_epoch - 1)]

        if candidates:
            # ensure stable sizes
            stable = True
            for p in candidates:
                try:
                    size1 = p.stat().st_size
                except FileNotFoundError:
                    stable = False
                    break
                time.sleep(stable_interval)
                try:
                    size2 = p.stat().st_size
                except FileNotFoundError:
                    stable = False
                    break
                if size1 != size2:
                    logging.info("File %s changing size (%d -> %d). Waiting...", p.name, size1, size2)
                    stable = False
                    break
            if stable:
                logging.info("Found stable parquet files: %s", [p.name for p in candidates])
                return candidates

        if time.time() - start > timeout:
            raise TimeoutError("Timeout waiting for output parquet files.")
        time.sleep(poll_interval)


# ---------------------------
# Extra guard: wait until file isn't locked (Windows)
# ---------------------------

def wait_for_file_unlocked(path: Path, timeout: int = 120, poll_interval: int = 1):
    """
    Attempt to open path for reading. If PermissionError on Windows (file locked), retry until unlocked.
    """
    logging.info("Waiting for file to be unlocked: %s", path)
    start = time.time()
    while True:
        try:
            with open(path, "rb"):
                logging.info("File unlocked: %s", path)
                return
        except PermissionError:
            # locked by writer
            pass
        except Exception as e:
            # other issues (transient) - keep retrying until timeout
            logging.debug("Error opening file %s: %s", path, e)

        if time.time() - start > timeout:
            raise TimeoutError(f"File {path} remained locked/unreadable after {timeout}s")
        time.sleep(poll_interval)


# ---------------------------
# BigQuery schema sanitization (recursive + per-struct dedupe)
# ---------------------------

def sanitize_column_name(name: str) -> str:
    new = name.replace(".", "__").replace("[", "").replace("]", "").replace(" ", "_")
    if new and new[0].isdigit():
        new = "_" + new
    new = "".join(ch if ch.isalnum() or ch == "_" else "_" for ch in new)
    return new


def convert_arrow_type_to_bq(field_type):
    if pa.types.is_integer(field_type):
        return "INT64"
    if pa.types.is_floating(field_type):
        return "FLOAT64"
    if pa.types.is_boolean(field_type):
        return "BOOL"
    if pa.types.is_string(field_type) or pa.types.is_large_string(field_type):
        return "STRING"
    if pa.types.is_timestamp(field_type):
        return "TIMESTAMP"
    if pa.types.is_date(field_type):
        return "DATE"
    return "STRING"


def get_sanitized_bq_schema(parquet_file: Path):
    pf = pq.ParquetFile(str(parquet_file))
    arrow_schema = pf.schema_arrow

    def sanitize_fields(fields):
        used = {}
        bq_fields = []
        for field in fields:
            base = sanitize_column_name(field.name)
            new_name = base
            counter = 1
            while new_name in used:
                new_name = f"{base}__{counter}"
                counter += 1
            used[new_name] = True

            if pa.types.is_struct(field.type):
                sub = sanitize_fields(field.type)
                bq_field = bigquery.SchemaField(new_name, "RECORD", mode="NULLABLE", fields=sub)
            elif pa.types.is_list(field.type):
                inner = field.type.value_type
                if pa.types.is_struct(inner):
                    sub = sanitize_fields(inner)
                    bq_field = bigquery.SchemaField(new_name, "RECORD", mode="REPEATED", fields=sub)
                else:
                    bq_field = bigquery.SchemaField(new_name, convert_arrow_type_to_bq(inner), mode="REPEATED")
            else:
                bq_field = bigquery.SchemaField(new_name, convert_arrow_type_to_bq(field.type), mode="NULLABLE")

            bq_fields.append(bq_field)
        return bq_fields

    schema = sanitize_fields(arrow_schema)
    # log top-level mapping
    try:
        orig_names = pf.schema_arrow.names
        logging.info("Top-level mapping original->sanitized:")
        for orig, san in zip(orig_names, [f.name for f in schema]):
            if orig != san:
                logging.info("  %s -> %s", orig, san)
    except Exception:
        pass
    return schema


# ---------------------------
# Upload to GCS
# ---------------------------

def upload_to_gcs(bucket_name: str, folder_name: str, files, credentials_path: str, clean_partition: bool = False):
    creds = service_account.Credentials.from_service_account_file(credentials_path)
    client = storage.Client(credentials=creds, project=creds.project_id)
    bucket = client.bucket(bucket_name)

    if clean_partition:
        logging.info("Cleaning GCS prefix gs://%s/%s/", bucket_name, folder_name)
        blobs = client.list_blobs(bucket_name, prefix=f"{folder_name}/")
        deleted = 0
        for blob in blobs:
            blob.delete()
            deleted += 1
        logging.info("Deleted %d objects", deleted)

    gcs_paths = []
    for p in files:
        blob_name = f"{folder_name}/{os.path.basename(str(p))}"
        blob = bucket.blob(blob_name)
        logging.info("Uploading %s -> gs://%s/%s", p, bucket_name, blob_name)
        blob.upload_from_filename(str(p))
        gcs_paths.append(f"gs://{bucket_name}/{blob_name}")
    logging.info("Uploaded %d file(s) to GCS", len(gcs_paths))
    return gcs_paths, creds.project_id


# ---------------------------
# Create BigQuery external table (override schema)
# ---------------------------

def create_external_table(project_id: str, dataset: str, table: str, gcs_uris, credentials_path: str, schema, partitioned: bool = False):
    creds = service_account.Credentials.from_service_account_file(credentials_path)
    client = bigquery.Client(credentials=creds, project=project_id)
    table_id = f"{project_id}.{dataset}.{table}"

    try:
        client.get_table(table_id)
        logging.info("BigQuery table %s exists - skipping create", table_id)
        return
    except Exception:
        logging.info("Creating BigQuery external table %s", table_id)

    external_config = bigquery.ExternalConfig("PARQUET")
    external_config.source_uris = gcs_uris
    external_config.schema = schema

    if partitioned:
        from google.cloud.bigquery.external_config import HivePartitioningOptions
        hive = HivePartitioningOptions()
        hive.mode = "AUTO"
        hive.require_partition_filter = False
        uri_prefix = gcs_uris[0]
        if "dt=" in uri_prefix:
            uri_prefix = uri_prefix.split("dt=")[0]
            if not uri_prefix.endswith("/"):
                uri_prefix += "/"
        hive.source_uri_prefix = uri_prefix
        external_config.hive_partitioning = hive
        logging.info("Enabled hive partitioning (prefix=%s)", uri_prefix)

    table_ref = bigquery.Table(table_id)
    table_ref.external_data_configuration = external_config
    table_obj = client.create_table(table_ref)
    logging.info("Created external table %s", table_obj.full_table_id)


# ---------------------------
# Materialize table
# ---------------------------

def create_materialized_table_from_latest_partition(project_id, src_dataset, src_table, dest_dataset, dest_table, service_account_path, meta_source_system="BWH"):
    creds = service_account.Credentials.from_service_account_file(service_account_path)
    client = bigquery.Client(credentials=creds, project=project_id)
    src_full = f"{project_id}.{src_dataset}.{src_table}"
    dest_full = f"{project_id}.{dest_dataset}.{dest_table}"

    query = f"""
    CREATE OR REPLACE TABLE `{dest_full}` AS
    SELECT
      *,
      '{src_full}' AS meta_table_name,
      CURRENT_DATETIME() AS meta_ods_insert_date,
      '{meta_source_system}' AS meta_source_system
    FROM `{src_full}`
    WHERE dt = (SELECT MAX(dt) FROM `{src_full}`);
    """
    logging.info("Running materialization query for %s", dest_full)
    job = client.query(query)
    job.result()
    logging.info("Materialized latest partition to %s", dest_full)


# ---------------------------
# Delete local files
# ---------------------------

def delete_local_files(files):
    for p in files:
        try:
            os.remove(p)
            logging.info("Deleted local file %s", p)
        except Exception as e:
            logging.warning("Unable to delete %s: %s", p, e)


# ---------------------------
# Main flow
# ---------------------------

def main():
    parser = argparse.ArgumentParser(description="XTRACT -> GCS -> BigQuery E2E (uses logs/extractions endpoint)")
    parser.add_argument("--config", required=True, help="Path to JSON config")
    parser.add_argument("--job_name", required=True, help="XTRACT job name (extraction name)")
    parser.add_argument("--xtract_server", required=False, default=DEFAULT_XTRACT_SERVER,
                        help=f"XTRACT server base (default {DEFAULT_XTRACT_SERVER})")
    parser.add_argument("--job_url", required=False, help="Full start URL (optional, overrides server + job_name)")
    parser.add_argument("--timeout", type=int, default=1800, help="Timeout seconds for job run and file readiness")
    args = parser.parse_args()

    # load config
    with open(args.config, "r") as fh:
        config = json.load(fh)

    # decide start URL
    if args.job_url:
        start_url = args.job_url
    else:
        start_url = build_start_url_from_server(args.xtract_server, args.job_name)

    # trigger job
    job_name_returned, web_ts = trigger_xtract_job(start_url)

    # find the real startedAt timestamp via logs endpoint matching webServerLog
    real_started_at = find_real_timestamp_from_logs(args.xtract_server, job_name_returned, web_ts, timeout=60, poll_interval=2)

    # wait until that run finishes
    wait_for_run_completion(args.xtract_server, job_name_returned, real_started_at, poll_interval=5, timeout=args.timeout)

    # parse startedAt into datetime and find files created/modified after it
    job_start_dt = parse_xtract_timestamp(real_started_at)

    # wait for parquet files mtime >= startedAt and stable
    downloaded_files = wait_for_output_files_by_start_time(config["input_folder"], job_start_dt,
                                                          expected_pattern="*.parquet",
                                                          timeout=args.timeout, poll_interval=5, stable_interval=3)

    # ensure files unlocked
    for p in downloaded_files:
        wait_for_file_unlocked(p, timeout=120, poll_interval=1)

    # create sanitized schema from first parquet file
    schema = get_sanitized_bq_schema(downloaded_files[0])

    # table name from job_name_returned
    safe_table_name = (job_name_returned or args.job_name).upper().replace(" ", "_")

    # upload to GCS
    load_date = datetime.now().strftime("%Y-%m-%d")
    folder_with_date = (f"{config['folder']}/{safe_table_name}/dt={load_date}"
                        if config.get("partitioned", False)
                        else f"{config['folder']}/{safe_table_name}")

    gcs_paths, project_id = upload_to_gcs(config["bucket"], folder_with_date, downloaded_files,
                                         config["service_account"], clean_partition=config.get("clean_partition", True))

    # create external table in BigQuery with overridden schema
    create_external_table(project_id, config["bq_dataset"], safe_table_name, gcs_paths, config["service_account"], schema,
                          partitioned=config.get("partitioned", False))

    # optional materialized table
    if "bq_target_dataset" in config:
        target_table = config.get("bq_target_table", safe_table_name)
        create_materialized_table_from_latest_partition(project_id, config["bq_dataset"], safe_table_name,
                                                        config["bq_target_dataset"], target_table,
                                                        config["service_account"])

    # delete local files
    delete_local_files(downloaded_files)

    logging.info("End-to-end processing completed for job %s", safe_table_name)


if __name__ == "__main__":
    main()
