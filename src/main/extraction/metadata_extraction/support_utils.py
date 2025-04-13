import json
import boto3
from io import BytesIO
from urllib.parse import urlparse


# ---------- AWS Session ----------
def get_boto3_session(profile_name="root"):
    return boto3.Session(profile_name=profile_name)


# ---------- S3 Read/Write ----------
def read_json_from_s3(s3_uri, session):
    s3 = session.client("s3")
    parsed = urlparse(s3_uri)
    bucket = parsed.netloc
    key = parsed.path.lstrip("/")

    response = s3.get_object(Bucket=bucket, Key=key)
    return json.load(response["Body"])


def write_json_to_s3(data, s3_uri, session):
    s3 = session.client("s3")
    parsed = urlparse(s3_uri)
    bucket = parsed.netloc
    key = parsed.path.lstrip("/")

    json_bytes = json.dumps(data, indent=2).encode("utf-8")
    s3.upload_fileobj(BytesIO(json_bytes), Bucket=bucket, Key=key)
    print(f"Column metadata saved to: s3://{bucket}/{key}")