import json
import boto3
from io import BytesIO
from urllib.parse import urlparse
from support_utils import *


# ---------- Metadata Extraction ----------
def extract_columns_metadata(json_obj):
    columns = json_obj.get("meta", {}).get("view", {}).get("columns", [])
    return [
        {
            "name": col.get("name"),
            "fieldName": col.get("fieldName"),
            "dataType": col.get("dataTypeName")
        }
        for col in columns
    ]


# ---------- Main ----------
def main():
    session = get_boto3_session(profile_name="root")

    input_s3_path = "s3://pas-sf-demo-data/input-data/ElectricVehiclePopulationData.json"
    output_s3_path = "s3://pas-sf-demo-data/output-data/metadata/columns_metadata.json"

    json_obj = read_json_from_s3(input_s3_path, session)
    metadata = extract_columns_metadata(json_obj)
    write_json_to_s3(metadata, output_s3_path, session)


if __name__ == "__main__":
    main()
