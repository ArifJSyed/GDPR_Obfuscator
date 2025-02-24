"""
Test module for the gdpr_obfuscator.
This module contains tests for CSV, JSON, and Parquet obfuscation functionality,
using fake S3 client classes to simulate AWS S3 behavior.
"""

import json
import csv
import io
from io import BytesIO
import pandas as pd
import pytest
from gdpr_obfuscator import obfuscate_file


# pylint: disable=too-few-public-methods
class FakeS3Body:
    """
    Fake S3 body to simulate the 'Body' attribute of a boto3 S3 object.

    Attributes:
        data (bytes): The content of the S3 object.
    """

    def __init__(self, data):
        # Data should be bytes.
        self.data = data

    def read(self):
        """Return the stored bytes."""
        return self.data


# pylint: disable=too-few-public-methods
class FakeS3Client:
    """
    Fake S3 client to simulate the boto3 S3 client's get_object method.

    Attributes:
        bucket (str): The S3 bucket name.
        key (str): The S3 object key.
        data (bytes): The content of the S3 object.
    """

    def __init__(self, bucket, key, data):
        self.bucket = bucket
        self.key = key
        self.data = data  # bytes

    def get_object(self, Bucket, Key):  # pylint: disable=invalid-name
        """
        Simulate boto3 S3 get_object method.

        Args:
            Bucket (str): The S3 bucket name (intentionally capitalized to mimic boto3).
            Key (str): The S3 key (intentionally capitalized to mimic boto3).

        Returns:
            dict: A dictionary with a 'Body' key containing a FakeS3Body.

        Raises:
            ValueError: If the bucket/key do not match the expected values.
        """
        if Bucket == self.bucket and Key == self.key:
            return {"Body": FakeS3Body(self.data)}
        raise ValueError("Object not found")


def test_obfuscate_csv(monkeypatch):
    """
    Test that obfuscate_file correctly obfuscates specified fields in a CSV file.
    """
    # Define CSV content with header and one data row.
    csv_content = (
        "student_id,name,course,cohort,graduation_date,email_address\n"
        "1234,John Smith,Software,2024-03-31,2023-06-30,j.smith@email.com\n"
    )
    bucket = "my_ingestion_bucket"
    key = "new_data/file1.csv"
    s3_url = f"s3://{bucket}/{key}"
    input_json = json.dumps(
        {"file_to_obfuscate": s3_url, "pii_fields": ["name", "email_address"]}
    )

    # Create a fake S3 client with the CSV content.
    fake_s3_client = FakeS3Client(bucket, key, csv_content.encode("utf-8"))
    monkeypatch.setattr("gdpr_obfuscator.boto3.client", lambda service: fake_s3_client)

    output_bytes = obfuscate_file(input_json)
    output_str = output_bytes.decode("utf-8")

    # Verify that PII fields have been obfuscated.
    assert "***" in output_str
    assert "John Smith" not in output_str
    assert "j.smith@email.com" not in output_str

    # Verify that non-PII fields remain unchanged.
    assert "1234" in output_str
    assert "Software" in output_str

    reader = csv.DictReader(io.StringIO(output_str))
    for row in reader:
        assert row["name"] == "***"
        assert row["email_address"] == "***"


def test_no_pii(monkeypatch):
    """
    Test that obfuscate_file leaves CSV data unchanged when no PII fields are provided.
    """
    csv_content = "id,info\n1,some data\n"
    bucket = "bucket"
    key = "file.csv"
    s3_url = f"s3://{bucket}/{key}"
    input_json = json.dumps({"file_to_obfuscate": s3_url, "pii_fields": []})

    fake_s3_client = FakeS3Client(bucket, key, csv_content.encode("utf-8"))
    monkeypatch.setattr("gdpr_obfuscator.boto3.client", lambda service: fake_s3_client)

    output_bytes = obfuscate_file(input_json)
    output_str = output_bytes.decode("utf-8")

    # Verify that the original data is preserved.
    assert "some data" in output_str
    reader = csv.DictReader(io.StringIO(output_str))
    for row in reader:
        assert row["info"] == "some data"


def test_no_matching_fields(monkeypatch):
    """
    Test that obfuscate_file leaves CSV data unchanged when provided PII fields do not exist.
    """
    csv_content = "id,info\n1,some data\n"
    bucket = "bucket"
    key = "file.csv"
    s3_url = f"s3://{bucket}/{key}"
    input_json = json.dumps(
        {"file_to_obfuscate": s3_url, "pii_fields": ["non_existent_field"]}
    )

    fake_s3_client = FakeS3Client(bucket, key, csv_content.encode("utf-8"))
    monkeypatch.setattr("gdpr_obfuscator.boto3.client", lambda service: fake_s3_client)

    output_bytes = obfuscate_file(input_json)
    output_str = output_bytes.decode("utf-8")

    # Verify that the data remains unchanged when no matching PII fields are found.
    assert "some data" in output_str
    reader = csv.DictReader(io.StringIO(output_str))
    for row in reader:
        assert row["info"] == "some data"


def test_invalid_s3_url():
    """
    Test that obfuscate_file raises a ValueError when an invalid S3 URL is provided.
    """
    input_json = json.dumps(
        {"file_to_obfuscate": "invalid_url", "pii_fields": ["name"]}
    )

    with pytest.raises(ValueError) as excinfo:
        obfuscate_file(input_json)
    assert "Invalid S3 URL" in str(excinfo.value)


def test_empty_csv(monkeypatch):
    """
    Test that obfuscate_file handles a CSV file with only a header row correctly.
    """
    # CSV with header only (no data rows)
    csv_content = "id,name\n"
    bucket = "bucket"
    key = "empty.csv"
    s3_url = f"s3://{bucket}/{key}"
    input_json = json.dumps({"file_to_obfuscate": s3_url, "pii_fields": ["name"]})

    fake_s3_client = FakeS3Client(bucket, key, csv_content.encode("utf-8"))
    monkeypatch.setattr("gdpr_obfuscator.boto3.client", lambda service: fake_s3_client)

    output_bytes = obfuscate_file(input_json)
    output_str = output_bytes.decode("utf-8")

    # Verify that the header remains and there are no data rows.
    assert "id,name" in output_str
    reader = csv.DictReader(io.StringIO(output_str))
    rows = list(reader)
    assert len(rows) == 0


def test_obfuscate_json(monkeypatch):
    """
    Test that obfuscate_file correctly obfuscates specified fields in a JSON file.
    """
    data = [
        {"id": 1, "name": "John Smith", "email": "john@example.com"},
        {"id": 2, "name": "Jane Doe", "email": "jane@example.com"},
    ]
    json_content = json.dumps(data)
    bucket = "bucket"
    key = "data/file.json"
    s3_url = f"s3://{bucket}/{key}"
    input_json = json.dumps(
        {"file_to_obfuscate": s3_url, "pii_fields": ["name", "email"]}
    )

    fake_s3_client = FakeS3Client(bucket, key, json_content.encode("utf-8"))
    monkeypatch.setattr("gdpr_obfuscator.boto3.client", lambda service: fake_s3_client)

    output_bytes = obfuscate_file(input_json)
    output_text = output_bytes.decode("utf-8")
    result_data = json.loads(output_text)

    for item in result_data:
        assert item["name"] == "***"
        assert item["email"] == "***"


def test_obfuscate_parquet(monkeypatch):
    """
    Test that obfuscate_file correctly obfuscates specified fields in a Parquet file.
    """
    # Create a small DataFrame to simulate a Parquet file.
    df = pd.DataFrame(
        {
            "id": [1, 2],
            "name": ["John Smith", "Jane Doe"],
            "email": ["john@example.com", "jane@example.com"],
            "other": ["data1", "data2"],
        }
    )
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    parquet_content = buffer.getvalue()

    bucket = "bucket"
    key = "data/file.parquet"
    s3_url = f"s3://{bucket}/{key}"
    input_json = json.dumps(
        {"file_to_obfuscate": s3_url, "pii_fields": ["name", "email"]}
    )

    fake_s3_client = FakeS3Client(bucket, key, parquet_content)
    monkeypatch.setattr("gdpr_obfuscator.boto3.client", lambda service: fake_s3_client)

    output_bytes = obfuscate_file(input_json)
    result_df = pd.read_parquet(BytesIO(output_bytes))

    # Verify that the PII fields have been obfuscated and other fields remain unchanged.
    assert all(result_df["name"] == "***")
    assert all(result_df["email"] == "***")
    assert all(result_df["id"] == df["id"])
    assert all(result_df["other"] == df["other"])


def test_unsupported_file_type(monkeypatch):
    """
    Test that obfuscate_file raises a ValueError for unsupported file types.
    """
    bucket = "bucket"
    key = "data/file.txt"
    s3_url = f"s3://{bucket}/{key}"
    input_json = json.dumps({"file_to_obfuscate": s3_url, "pii_fields": ["field"]})

    fake_s3_client = FakeS3Client(bucket, key, b"some content")
    monkeypatch.setattr("gdpr_obfuscator.boto3.client", lambda service: fake_s3_client)

    with pytest.raises(ValueError) as excinfo:
        obfuscate_file(input_json)
    assert "Unsupported file type" in str(excinfo.value)
