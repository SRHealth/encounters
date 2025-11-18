#!/usr/bin/env python3
import os
import sys
import json
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError, BotoCoreError
from datetime import datetime
from urllib.parse import unquote_plus

# ---------- Borrowed-style helpers ----------

def load_credentials(credentials_path="/Users/stephanielaface/Documents/GitHub/encounters/credentials.json"):
    """
    Load credentials from JSON file
    """
    try:
        with open(credentials_path, 'r') as f:
            credentials = json.load(f)
        return credentials
    except FileNotFoundError:
        print(f"Error: credentials.json not found at {credentials_path}")
        print("Please ensure the file exists in the current directory or provide the correct path")
        return None
    except json.JSONDecodeError:
        print("Error: Invalid JSON format in credentials.json")
        return None


# ---------- S3 download logic (FIXED) ----------

def init_s3_client(aws_credentials: dict | None):
    """
    Initialize and return a boto3 S3 client, honoring optional explicit credentials.
    """
    if aws_credentials:
        return boto3.client(
            's3',
            aws_access_key_id=aws_credentials.get('access_key_id'),
            aws_secret_access_key=aws_credentials.get('secret_access_key'),
            aws_session_token=aws_credentials.get('session_token'),
            region_name=aws_credentials.get('region', 'us-west-2'),
            config=Config(signature_version='s3v4', retries={'max_attempts': 10, 'mode': 'standard'})
        )
    return boto3.client('s3', config=Config(signature_version='s3v4', retries={'max_attempts': 10, 'mode': 'standard'}))


def list_all_objects(s3_client, bucket: str, prefix: str | None):
    """
    Generator yielding object dicts for all objects under bucket/prefix (handles pagination).
    """
    kwargs = {'Bucket': bucket}
    if prefix:
        kwargs['Prefix'] = prefix.lstrip('/')

    while True:
        resp = s3_client.list_objects_v2(**kwargs)
        for obj in resp.get('Contents', []):
            yield obj
        if resp.get('IsTruncated'):
            kwargs['ContinuationToken'] = resp.get('NextContinuationToken')
        else:
            break


def relativize_key_to_prefix(key: str, prefix_norm: str) -> str:
    """
    Return the portion of `key` that comes *after* `prefix_norm`.
    If key doesn't start with prefix_norm, return key unchanged (fallback).
    """
    key = unquote_plus(key)
    prefix_norm = prefix_norm.strip('/')

    if not prefix_norm:
        return key  # no prefix: entire key is relative

    if key == prefix_norm:
        return ""  # object named exactly the prefix (rare, but be safe)

    prefix_with_slash = prefix_norm + '/'
    if key.startswith(prefix_with_slash):
        return key[len(prefix_with_slash):]
    return key  # fallback if API returned keys outside the provided prefix


def ensure_parent_dir(path: str):
    parent = os.path.dirname(path)
    if parent and not os.path.exists(parent):
        os.makedirs(parent, exist_ok=True)


def download_s3_prefix(
    bucket: str,
    prefix: str,
    dest_root: str,
    aws_credentials: dict | None = None,
    skip_existing: bool = True,
) -> int:
    """
    Download all S3 objects from bucket/prefix into a local directory under dest_root.

    *** FIX: local path = dest_root / RELATIVE(key, prefix) ***
    This prevents duplicating the prefix in the local path.
    """
    s3 = init_s3_client(aws_credentials)

    prefix_norm = prefix.strip().lstrip('/')
    dest_root = os.path.abspath(os.path.expanduser(dest_root))

    print(f"Listing s3://{bucket}/{prefix_norm or ''} ...")

    count = 0
    found_any = False
    for obj in list_all_objects(s3, bucket, prefix_norm):
        found_any = True
        key = obj['Key']

        # Skip "folder placeholder" keys
        if key.endswith('/'):
            continue

        rel_key = relativize_key_to_prefix(key, prefix_norm)  # <<< THE IMPORTANT FIX
        local_path = os.path.join(dest_root, rel_key)

        ensure_parent_dir(local_path)

        if skip_existing and os.path.exists(local_path):
            print(f"• Skipping existing: {local_path}")
            count += 1
            continue

        try:
            s3.download_file(bucket, key, local_path)
            size_mb = obj.get('Size', 0) / (1024 * 1024)
            print(f"✓ Downloaded: {key}  ->  {local_path}  ({size_mb:.2f} MB)")
            count += 1
        except (ClientError, BotoCoreError) as e:
            print(f"✗ Failed: s3://{bucket}/{key}  ->  {local_path}\n  Reason: {e}")

    if not found_any:
        print("No objects found under the specified bucket/prefix.")
    return count


# ---------- CLI / script entry ----------

if __name__ == "__main__":
    # Load credentials
    credentials_path = os.environ.get("SRH_CREDENTIALS_PATH", "credentials.json")
    credentials = load_credentials(credentials_path)

    if credentials is None:
        print("Failed to load credentials. Exiting.")
        sys.exit(1)

    # S3 config (adjust as needed)
    S3_BUCKET = "aws-b2b-edi"
    S3_PREFIX = "production/priority-output-edi/ca-bb4007205c4d48cb8/tp-f1eb2352582e481b8"

    # Destination under ~/Downloads — point it at the *prefix root* you want on disk.
    # With the fix, files go under this folder using paths *relative* to S3 prefix.
    # Example result:
    #   ~/Downloads/aws-b2b-edi/production/priority-output-edi/ca-.../tp-.../<files...>
    downloads_root = os.path.expanduser("~/Downloads")
    dest_dir = os.path.join(downloads_root, S3_BUCKET, S3_PREFIX)

    # Optional override
    dest_dir = os.environ.get("SRH_S3_DOWNLOAD_DIR", dest_dir)

    aws_creds = credentials.get("aws", None)

    print("S3 Bulk Downloader (fixed nesting)")
    print("=" * 40)
    print(f"Source:      s3://{S3_BUCKET}/{S3_PREFIX}")
    print(f"Destination: {dest_dir}")
    print(f"Started:     {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    try:
        os.makedirs(dest_dir, exist_ok=True)
        total = download_s3_prefix(
            bucket=S3_BUCKET,
            prefix=S3_PREFIX,
            dest_root=dest_dir,
            aws_credentials=aws_creds,
            skip_existing=True,  # change to False to force re-downloads
        )
        print("\nDone.")
        print(f"Files processed (downloaded or skipped if present): {total}")
        print(f"Local folder: {dest_dir}")
    except Exception as e:
        print(f"Unhandled error: {e}")
        sys.exit(1)