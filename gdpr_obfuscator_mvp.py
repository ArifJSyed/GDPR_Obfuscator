"""
GDPR Obfuscator Module

This module provides functionality to obfuscate GDPR-sensitive fields in a CSV file stored in AWS S3.
It reads a CSV file from S3, replaces specified PII fields with obfuscated values, and returns
a byte-stream of the modified CSV. The primary function expects a JSON string input detailing
the S3 file location and the fields to obfuscate.
"""

import boto3
import csv
import io
import json


def obfuscate_csv_from_json(json_input_str):
    """
    Obfuscates specified fields in a CSV file stored on S3.

    Args:
        json_input_str (str): A JSON string containing:
            - file_to_obfuscate: S3 URL of the CSV file (e.g., "s3://bucket/key")
            - pii_fields: A list of field names to obfuscate in the CSV

    Returns:
        bytes: A bytes stream of the obfuscated CSV file, suitable for boto3 S3 PutObject.

    Raises:
        ValueError: If the S3 URL is invalid or improperly formatted.
        Exception: Propagates exceptions from S3 operations or CSV processing.
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

    # Initialize S3 client
    s3 = boto3.client("s3")

    # Retrieve object from S3
    response = s3.get_object(Bucket=bucket, Key=key)
    body = response["Body"].read().decode("utf-8")

    input_stream = io.StringIO(body)
    output_stream = io.StringIO()

    # Setup CSV reader and writer
    reader = csv.DictReader(input_stream)
    fieldnames = reader.fieldnames
    if fieldnames is None:
        raise ValueError("CSV file has no header row.")
    writer = csv.DictWriter(
        output_stream, fieldnames=fieldnames, quoting=csv.QUOTE_MINIMAL
    )
    writer.writeheader()

    # Process each row, obfuscating specified fields
    for row in reader:
        for field in pii_fields:
            if field in row:
                row[field] = "***"
        writer.writerow(row)

    # Return the resulting CSV as bytes
    result = output_stream.getvalue().encode("utf-8")
    return result
