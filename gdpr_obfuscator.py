"""
GDPR Obfuscator Module

This module provides functionality to obfuscate GDPR-sensitive fields in various file formats
(CSV, JSON, Parquet) stored in AWS S3.
It reads a file from S3, replaces specified PII fields with obfuscated values, and returns
a byte-stream of the modified file. The primary function expects a JSON string input detailing
the S3 file location and the fields to obfuscate.
"""

import csv
import io
import json
from io import BytesIO
import pandas as pd
import boto3


def obfuscate_csv(data: str, pii_fields: list) -> bytes:
    """
    Obfuscates PII fields in CSV data.

    Args:
        data (str): The CSV file content as a string.
        pii_fields (list): List of field names to obfuscate.

    Returns:
        bytes: The obfuscated CSV content as bytes.

    Raises:
        ValueError: If the CSV file has no header row.
    """
    # Create input and output streams for processing CSV data.
    input_stream = io.StringIO(data)
    output_stream = io.StringIO()

    # Use DictReader to parse CSV rows into dictionaries.
    reader = csv.DictReader(input_stream)
    fieldnames = reader.fieldnames
    if fieldnames is None:
        raise ValueError("CSV file has no header row.")

    # Initialize a CSV writer with the same fieldnames.
    writer = csv.DictWriter(
        output_stream, fieldnames=fieldnames, quoting=csv.QUOTE_MINIMAL
    )
    writer.writeheader()

    # Iterate over each row and obfuscate PII fields.
    for row in reader:
        for field in pii_fields:
            if field in row:
                row[field] = "***"
        writer.writerow(row)

    # Return the obfuscated CSV as bytes.
    return output_stream.getvalue().encode("utf-8")


def obfuscate_json(data: str, pii_fields: list) -> bytes:
    """
    Obfuscates PII fields in JSON data.

    Args:
        data (str): The JSON file content as a string (assumed to be a list of dictionaries).
        pii_fields (list): List of field names to obfuscate.

    Returns:
        bytes: The obfuscated JSON content as bytes.
    """
    # Parse the JSON string into a Python object.
    data_list = json.loads(data)

    # Iterate through each dictionary in the list and obfuscate PII fields.
    for row in data_list:
        for field in pii_fields:
            if field in row:
                row[field] = "***"

    # Dump the modified data back to a JSON string and encode as bytes.
    return json.dumps(data_list).encode("utf-8")


def obfuscate_parquet(file_bytes: bytes, pii_fields: list) -> bytes:
    """
    Obfuscates PII fields in a Parquet file.

    Args:
        file_bytes (bytes): The Parquet file content as bytes.
        pii_fields (list): List of field names to obfuscate.

    Returns:
        bytes: The obfuscated Parquet file as bytes.
    """
    # Read the Parquet file into a pandas DataFrame.
    df = pd.read_parquet(BytesIO(file_bytes))

    # Replace the specified PII fields with an obfuscated value.
    for field in pii_fields:
        if field in df.columns:
            df[field] = "***"

    # Write the modified DataFrame back to a Parquet file in a BytesIO buffer.
    out_buffer = BytesIO()
    df.to_parquet(out_buffer, index=False)
    return out_buffer.getvalue()


def obfuscate_file(json_input_str: str) -> bytes:
    """
    Obfuscates specified fields in a file stored on S3, supporting CSV, JSON, and Parquet formats.

    This function expects a JSON string with the following keys:
      - file_to_obfuscate: S3 URL of the file (e.g., s3://bucket/key)
      - pii_fields: A list of field names containing PII to be obfuscated.

    The function downloads the file from S3, determines the file type based on its extension,
    obfuscates the PII fields using the appropriate helper function, and returns the modified
    file as bytes.

    Args:
        json_input_str (str): A JSON string containing:
            - file_to_obfuscate: S3 URL of the file.
            - pii_fields: List of PII fields to obfuscate.

    Returns:
        bytes: A byte stream of the obfuscated file, ready for use with boto3 S3 PutObject.

    Raises:
        ValueError: If the S3 URL is invalid or if the file type is unsupported.
    """
    # Parse the input JSON to extract parameters.
    input_data = json.loads(json_input_str)
    s3_url = input_data["file_to_obfuscate"]
    pii_fields = input_data["pii_fields"]

    # Validate the S3 URL format.
    if not s3_url.startswith("s3://"):
        raise ValueError("Invalid S3 URL: Must start with 's3://'")

    # Extract the bucket and key from the S3 URL.
    s3_parts = s3_url[5:].split("/", 1)
    if len(s3_parts) != 2:
        raise ValueError("Invalid S3 URL: Must be in the form s3://bucket/key")
    bucket, key = s3_parts

    # Initialize an S3 client and retrieve the object.
    s3 = boto3.client("s3")
    response = s3.get_object(Bucket=bucket, Key=key)
    file_bytes = response["Body"].read()

    # Determine the file type based on its extension and call the corresponding function.
    if key.endswith(".csv"):
        # Process CSV: decode bytes to string.
        return obfuscate_csv(file_bytes.decode("utf-8"), pii_fields)
    if key.endswith(".json"):
        # Process JSON: decode bytes to string.
        return obfuscate_json(file_bytes.decode("utf-8"), pii_fields)
    if key.endswith(".parquet"):
        # Process Parquet: work directly with bytes.
        return obfuscate_parquet(file_bytes, pii_fields)
    raise ValueError(
        "Unsupported file type. Only CSV, JSON, and Parquet are supported."
    )
