"""
GDPR Obfuscator Module

This module provides functionality to obfuscate GDPR-sensitive fields in various file formats (CSV, JSON, Parquet) stored in AWS S3.
It reads a file from S3, replaces specified PII fields with obfuscated values, and returns
a byte-stream of the modified file. The primary function expects a JSON string input detailing
the S3 file location and the fields to obfuscate.
"""

import boto3
import csv
import io
import json
import pandas as pd
from io import BytesIO


def obfuscate_csv_from_json(json_input_str):
    """
    Obfuscates specified fields in a file stored on S3, supporting CSV, JSON, and Parquet.

    Args:
        json_input_str (str): A JSON string containing:
            - file_to_obfuscate: S3 URL of the file (CSV, JSON, or Parquet)
            - pii_fields: A list of field names to obfuscate

    Returns:
        bytes: A bytes stream of the obfuscated file, suitable for boto3 S3 PutObject.

    Raises:
        ValueError: If the S3 URL is invalid or if the file type is unsupported.
    """
    input_data = json.loads(json_input_str)
    s3_url = input_data["file_to_obfuscate"]
    pii_fields = input_data["pii_fields"]

    # Validate and parse S3 URL
    if not s3_url.startswith("s3://"):
        raise ValueError("Invalid S3 URL: Must start with 's3://'")
    s3_parts = s3_url[5:].split("/", 1)
    if len(s3_parts) != 2:
        raise ValueError("Invalid S3 URL: Must be in the form s3://bucket/key")
    bucket, key = s3_parts

    # Initialize S3 client and get object
    s3 = boto3.client("s3")
    response = s3.get_object(Bucket=bucket, Key=key)
    file_bytes = response["Body"].read()

    # Determine file type based on extension
    if key.endswith(".csv"):
        body = file_bytes.decode("utf-8")
        input_stream = io.StringIO(body)
        output_stream = io.StringIO()

        reader = csv.DictReader(input_stream)
        fieldnames = reader.fieldnames
        if fieldnames is None:
            raise ValueError("CSV file has no header row.")
        writer = csv.DictWriter(
            output_stream, fieldnames=fieldnames, quoting=csv.QUOTE_MINIMAL
        )
        writer.writeheader()

        for row in reader:
            for field in pii_fields:
                if field in row:
                    row[field] = "***"
            writer.writerow(row)

        return output_stream.getvalue().encode("utf-8")

    elif key.endswith(".json"):
        text = file_bytes.decode("utf-8")
        data_list = json.loads(text)
        # Assume data_list is a list of dictionaries
        for row in data_list:
            for field in pii_fields:
                if field in row:
                    row[field] = "***"
        new_text = json.dumps(data_list)
        return new_text.encode("utf-8")

    elif key.endswith(".parquet"):
        df = pd.read_parquet(BytesIO(file_bytes))
        for field in pii_fields:
            if field in df.columns:
                df[field] = "***"
        out_buffer = BytesIO()
        df.to_parquet(out_buffer, index=False)
        return out_buffer.getvalue()

    else:
        raise ValueError(
            "Unsupported file type. Only CSV, JSON, and Parquet are supported."
        )
