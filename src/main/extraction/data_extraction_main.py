import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from constants.extraction_constants import *
from data_quality_checks import run_data_quality_checks
from data_insights import run_data_insights
import boto3
from urllib.parse import urlparse


# --------------------- S3 Reader ---------------------
def read_json_file_from_s3(s3_path, profile_name="root"):
    session = boto3.Session(profile_name=profile_name)
    s3 = session.client("s3")

    parsed = urlparse(s3_path)
    bucket = parsed.netloc
    key = parsed.path.lstrip("/")

    response = s3.get_object(Bucket=bucket, Key=key)
    return json.load(response["Body"])


# --------------------- Schema Utils ---------------------
def build_spark_schema(columns_metadata):
    type_mapping = {
        "text": StringType(),
        "number": DoubleType(),
        "meta_data": StringType(),
        "point": StringType(),
    }
    fields = [
        StructField(col["fieldName"], type_mapping.get(col["dataType"], StringType()), True)
        for col in columns_metadata
    ]
    return StructType(fields)


def cast_value(value, spark_type):
    if value in [None, ""]:
        return None
    try:
        if isinstance(spark_type, DoubleType):
            return float(value)
        elif isinstance(spark_type, IntegerType):
            return int(value)
        elif isinstance(spark_type, StringType):
            return str(value)
        else:
            return value
    except:
        return None


def create_dataframe_from_data(spark, data, schema, column_names):
    structured_rows = []
    for row in data:
        casted_row = []
        for i, field in enumerate(schema.fields):
            value = row[i] if i < len(row) else None
            casted_row.append(cast_value(value, field.dataType))
        structured_rows.append(tuple(casted_row))
    return spark.createDataFrame(structured_rows, schema=schema)


# --------------------- Writer ---------------------
def write_data(df, output_path, in_type='csv'):
    df = df.repartition(1)

    if in_type == 'delta':
        df.write.mode("overwrite") \
            .option("header", "true") \
            .format("delta") \
            .save(f"{output_path}/delta")
    elif in_type == 'iceberg':
        df.write.format("iceberg") \
            .mode("overwrite") \
            .saveAsTable(f"{ICEBERG_CATALOG}.{ICEBERG_TABLE_NAME}")
    else:
        df.write.mode("overwrite") \
            .option("header", "true") \
            .csv(f"{output_path}/csv")


# --------------------- Main Pipeline ---------------------
def main():
    spark = SparkSession.builder \
        .appName("EV Dataset Extractor") \
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.local.type", "hadoop") \
        .config("spark.sql.catalog.local.warehouse", ICEBERG_OUTPUT_DIR) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.access.key", "") \
        .config("spark.hadoop.fs.s3a.secret.key", "") \
        .config("spark.hadoop.fs.s3a.endpoint", "s3.us-east-1.amazonaws.com") \
        .getOrCreate()


    # Load metadata and raw data from S3
    raw_json = read_json_file_from_s3(JSON_INPUT_PATH)
    columns_metadata = read_json_file_from_s3(METADATA_OUTPUT_PATH)

    column_names = [col["fieldName"] for col in columns_metadata]
    spark_schema = build_spark_schema(columns_metadata)
    raw_data = raw_json["data"]
    data_df = create_dataframe_from_data(spark, raw_data, spark_schema, column_names)

    write_data(data_df, ICEBERG_OUTPUT_DIR, 'iceberg')
    print(f"Data written to Iceberg at: {ICEBERG_OUTPUT_DIR}")

    # Reading the data written in ICEBERG
    iceberg_table = f"{ICEBERG_CATALOG}.{ICEBERG_TABLE_NAME}"
    read_df = spark.read.table(iceberg_table)

    # Run Data Quality Checks
    dq_issues = run_data_quality_checks(read_df, DATA_QUALITY_OUTPUT_DIR)
    print(f"dq_issues: {dq_issues}")

    # Run Data Insights Checks
    data_insights = run_data_insights(read_df, DATA_INSIGHTS_OUTPUT_DIR)
    print(f"data_insights: {data_insights}")

    spark.stop()


if __name__ == "__main__":
    main()
