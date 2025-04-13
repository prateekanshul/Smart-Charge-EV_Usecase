import json
import boto3
from io import BytesIO
from urllib.parse import urlparse
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from metadata_extraction.support_utils import *


def run_data_quality_checks(df: DataFrame, output_uri: str = None, output_filename: str = "dq_issues.json", profile_name="root"):
    total_rows = df.count()
    issues_summary = []

    # 1. Empty Column Check
    for column in df.columns:
        null_count = df.filter(col(column).isNull() | (col(column) == "")).count()
        if null_count == total_rows:
            issues_summary.append({
                "check_type": "Empty Column",
                "column": column,
                "issue_count": null_count
            })

    # 2. Duplicate Row Check
    duplicate_count = total_rows - df.dropDuplicates().count()
    if duplicate_count > 0:
        issues_summary.append({
            "check_type": "Duplicate Rows",
            "column": "Entire Row",
            "issue_count": duplicate_count
        })

    # 3. Alphanumeric Violations
    for column in df.columns:
        if dict(df.dtypes)[column] == 'string':
            count_violation = df.filter(~col(column).rlike(r'^[a-zA-Z0-9 ]*$')).count()
            if count_violation > 0:
                issues_summary.append({
                    "check_type": "Non-Alphanumeric",
                    "column": column,
                    "issue_count": count_violation
                })

    # 4. Special Character Check
    special_pattern = r'[!@#$%^&*(),.?":{}|<>]'
    for column in df.columns:
        if dict(df.dtypes)[column] == 'string':
            count_violation = df.filter(col(column).rlike(special_pattern)).count()
            if count_violation > 0:
                issues_summary.append({
                    "check_type": "Special Characters",
                    "column": column,
                    "issue_count": count_violation
                })

    # Write to S3 if path provided
    if issues_summary and output_uri:
        session = get_boto3_session(profile_name)
        full_s3_path = f"{output_uri.rstrip('/')}/{output_filename}"
        write_json_to_s3(issues_summary, full_s3_path, session)
    elif not issues_summary:
        print("No data quality issues found.")

    return issues_summary
