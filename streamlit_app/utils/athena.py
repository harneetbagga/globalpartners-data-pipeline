import boto3
import pandas as pd
import time
import streamlit as st

ATHENA_DATABASE   = "gp_gold"
ATHENA_RESULTS_S3 = "s3://gp-data-lake-dev/athena-results/"
AWS_REGION        = "us-east-1"


def get_boto3_client(service: str):
    """
    Get boto3 client using Streamlit secrets when deployed,
    or default credentials when running locally.
    """
    try:
        # Streamlit Cloud — credentials from secrets.toml
        return boto3.client(
            service,
            region_name          = AWS_REGION,
            aws_access_key_id    = st.secrets["aws"]["access_key_id"],
            aws_secret_access_key = st.secrets["aws"]["secret_access_key"],
        )
    except (KeyError, FileNotFoundError):
        # Local — use default ~/.aws/credentials
        return boto3.client(service, region_name=AWS_REGION)


@st.cache_data(ttl=3600)
def run_query(sql: str) -> pd.DataFrame:
    """Execute a query against Athena and return results as a DataFrame."""
    client = get_boto3_client("athena")

    response = client.start_query_execution(
        QueryString           = sql,
        QueryExecutionContext = {"Database": ATHENA_DATABASE},
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
            error = client.get_query_execution(
                QueryExecutionId=execution_id
            )["QueryExecution"]["Status"].get("StateChangeReason", "Unknown error")
            raise Exception(f"Athena query failed: {error}")

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