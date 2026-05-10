import boto3
import pandas as pd
import time
import streamlit as st

ATHENA_DATABASE    = "gp_gold"
ATHENA_RESULTS_S3  = "s3://gp-data-lake-dev/athena-results/"
AWS_REGION         = "us-east-1"

@st.cache_data(ttl=3600)  # Cache results for 1 hour
def run_query(sql: str) -> pd.DataFrame:
    """
    Execute a query against Athena and return results as a DataFrame.
    Results are cached for 1 hour to avoid repeated S3 scans.
    """
    client = boto3.client("athena", region_name=AWS_REGION)

    response = client.start_query_execution(
        QueryString             = sql,
        QueryExecutionContext   = {"Database": ATHENA_DATABASE},
        ResultConfiguration     = {"OutputLocation": ATHENA_RESULTS_S3},
    )

    execution_id = response["QueryExecutionId"]

    # Poll until complete
    while True:
        status = client.get_query_execution(
            QueryExecutionId=execution_id
        )["QueryExecution"]["Status"]["State"]

        if status == "SUCCEEDED":
            break
        elif status in ("FAILED", "CANCELLED"):
            error = client.get_query_execution(
                QueryExecutionId=execution_id
            )["QueryExecution"]["Status"].get("StateChangeReason", "Unknown error")
            raise Exception(f"Athena query failed: {error}")

        time.sleep(1)

    # Fetch results
    paginator = client.get_paginator("get_query_results")
    pages     = paginator.paginate(QueryExecutionId=execution_id)

    rows    = []
    headers = None

    for page in pages:
        result_rows = page["ResultSet"]["Rows"]
        if headers is None:
            headers = [col["VarCharValue"] for col in result_rows[0]["Data"]]
            result_rows = result_rows[1:]
        for row in result_rows:
            rows.append([col.get("VarCharValue", "") for col in row["Data"]])

    return pd.DataFrame(rows, columns=headers)


def run_query_bronze_silver(sql: str, database: str) -> pd.DataFrame:
    """Query non-Gold databases when needed."""
    client = boto3.client("athena", region_name=AWS_REGION)

    response = client.start_query_execution(
        QueryString           = sql,
        QueryExecutionContext = {"Database": database},
        ResultConfiguration   = {"OutputLocation": ATHENA_RESULTS_S3},
    )

    execution_id = response["QueryExecutionId"]

    while True:
        status = client.get_query_execution(
            QueryExecutionId=execution_id
        )["QueryExecution"]["Status"]["State"]

        if status == "SUCCEEDED":
            break
        elif status in ("FAILED", "CANCELLED"):
            raise Exception(f"Athena query failed")
        time.sleep(1)

    paginator = client.get_paginator("get_query_results")
    pages     = paginator.paginate(QueryExecutionId=execution_id)

    rows    = []
    headers = None

    for page in pages:
        result_rows = page["ResultSet"]["Rows"]
        if headers is None:
            headers = [col["VarCharValue"] for col in result_rows[0]["Data"]]
            result_rows = result_rows[1:]
        for row in result_rows:
            rows.append([col.get("VarCharValue", "") for col in row["Data"]])

    return pd.DataFrame(rows, columns=headers)