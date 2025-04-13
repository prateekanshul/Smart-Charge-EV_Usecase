import json
from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.functions import col, count, avg, desc, row_number
from metadata_extraction.support_utils import get_boto3_session, write_json_to_s3


def get_data_insights(df: DataFrame):
    insights = {}

    if "make" in df.columns:
        popular_makes = df.groupBy("make").count().orderBy(desc("count")).limit(5)
        insights["Most Selling Electric Vehicle Makes"] = [{"Make": row["make"], "Count": row["count"]} for row in popular_makes.collect()]

    if "model_year" in df.columns:
        model_years = df.groupBy("model_year").count().orderBy(desc("count")).limit(5)
        insights["YoY Trend of Electric Vehicles"] = [{"Model Year": row["model_year"], "Count": row["count"]} for row in model_years.collect()]

    if "electric_range" in df.columns:
        avg_range = df.select(avg(col("electric_range").cast("double"))).first()[0]
        insights["Average Electric Range"] = round(avg_range, 2) if avg_range else None

    if "county" in df.columns:
        top_counties = df.groupBy("county").count().orderBy(desc("count")).limit(5)
        insights["Trend of Electric Vehicles across Counties"] = [{"County": row["county"], "Count": row["count"]} for row in top_counties.collect()]

    if "model_year" in df.columns and "model" in df.columns:
        model_sales = df.groupBy("model_year", "model").agg(count("*").alias("count"))
        window_spec = Window.partitionBy("model_year").orderBy(col("count").desc())
        top_per_year = model_sales.withColumn("rank", row_number().over(window_spec)) \
            .filter(col("rank") == 1) \
            .select("model_year", "model", "count")

        # insights["Top Model Per Year"] = [
        #     {"Model Year": row["model_year"], "Model": row["model"], "Count": row["count"]}
        #     for row in top_per_year.collect()
        # ]

    return insights


def run_data_insights(
        df: DataFrame,
        output_s3_uri: str,
        filename: str = "data_insights.json",
        profile_name: str = "root"
):
    print("Running Data Insights on passed-in DataFrame...")

    insights = get_data_insights(df)

    if output_s3_uri:
        full_s3_uri = f"{output_s3_uri.rstrip('/')}/{filename}"
        session = get_boto3_session(profile_name)
        write_json_to_s3(insights, full_s3_uri, session)
        print(f"Insights written to {full_s3_uri}")

    return insights
