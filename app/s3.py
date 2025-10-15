import os, boto3

S3_BUCKET = os.getenv("S3_BUCKET")
S3_PREFIX = os.getenv("S3_PREFIX", "raw/")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

s3 = boto3.client("s3", region_name=AWS_REGION)

def ping_s3(max_keys=5):
    """Lista algunos objetos de raw/ y backup/ para verificar permisos."""
    result = {}
    for prefix in [os.getenv("S3_PREFIX", "raw/"), "backup/"]:
        try:
            resp = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix, MaxKeys=max_keys)
            items = [obj["Key"] for obj in resp.get("Contents", [])]
            result[prefix] = items
        except Exception as e:
            result[prefix] = f"ERROR: {e.__class__.__name__}: {e}"
    return result
