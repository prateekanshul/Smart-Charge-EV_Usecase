import json
import boto3
from io import BytesIO
from urllib.parse import urlparse
from support_utils import *


# Extract approvals
def extract_approvals_info(json_obj):
    approvals = json_obj.get("meta", {}).get("view", {}).get("approvals", [])
    extracted = []
    for approval in approvals:
        submitter = approval.get("submitter", {})
        extracted.append({
            "submissionId": approval.get("submissionId"),
            "reviewedAt": approval.get("reviewedAt"),
            "reviewedAutomatically": approval.get("reviewedAutomatically"),
            "state": approval.get("state"),
            "submitterId": submitter.get("id"),
            "submitterName": submitter.get("displayName")
        })
    return extracted


# Main function
def main():
    session = get_boto3_session("root")

    input_s3_path = "s3://pas-sf-demo-data/input-data/ElectricVehiclePopulationData.json"
    output_s3_path = "s3://pas-sf-demo-data/output-data/metadata/approval_metadata.json"

    json_obj = read_json_from_s3(input_s3_path, session)
    approvals_info = extract_approvals_info(json_obj)
    write_json_to_s3(approvals_info, output_s3_path, session)


if __name__ == "__main__":
    main()
