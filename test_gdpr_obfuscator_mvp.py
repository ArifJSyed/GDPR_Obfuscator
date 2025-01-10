import json
import csv
import io
import pytest
from gdpr_obfuscator import obfuscate_csv_from_json

# Helper classes to simulate S3 behavior for testing
class FakeS3Body:
    def __init__(self, data):
        self.data = data

    def read(self):
        return self.data.encode('utf-8')


class FakeS3Client:
    def __init__(self, bucket, key, data):
        self.bucket = bucket
        self.key = key
        self.data = data

    def get_object(self, Bucket, Key):
        if Bucket == self.bucket and Key == self.key:
            return {'Body': FakeS3Body(self.data)}
        raise Exception("Object not found")


# Test Cases

def test_obfuscate_csv(monkeypatch):
    # Updated CSV content with correct alignment of header and row values
    csv_content = (
        "student_id,name,course,cohort,graduation_date,email_address\n"
        "1234,John Smith,Software,2024-03-31,2023-06-30,j.smith@email.com\n"
    )
    bucket = "my_ingestion_bucket"
    key = "new_data/file1.csv"
    s3_url = f"s3://{bucket}/{key}"
    input_json = json.dumps({
        "file_to_obfuscate": s3_url,
        "pii_fields": ["name", "email_address"]
    })

    # Setup fake S3 client
    fake_s3_client = FakeS3Client(bucket, key, csv_content)
    monkeypatch.setattr("gdpr_obfuscator.boto3.client", lambda service: fake_s3_client)

    # Invoke the function
    output_bytes = obfuscate_csv_from_json(input_json)
    output_str = output_bytes.decode('utf-8')

    # Verify that PII fields have been obfuscated
    assert "***" in output_str
    assert "John Smith" not in output_str
    assert "j.smith@email.com" not in output_str

    # Verify that non-sensitive fields remain intact
    assert "1234" in output_str
    assert "Software" in output_str

    # Confirm CSV structure by reading back the CSV content
    reader = csv.DictReader(io.StringIO(output_str))
    for row in reader:
        assert row["name"] == "***"
        assert row["email_address"] == "***"


def test_no_pii(monkeypatch):
    # Test when no pii_fields provided. CSV should remain unchanged.
    csv_content = (
        "id,info\n"
        "1,some data\n"
    )
    bucket = "bucket"
    key = "file.csv"
    s3_url = f"s3://{bucket}/{key}"
    input_json = json.dumps({
        "file_to_obfuscate": s3_url,
        "pii_fields": []
    })

    fake_s3_client = FakeS3Client(bucket, key, csv_content)
    monkeypatch.setattr("gdpr_obfuscator.boto3.client", lambda service: fake_s3_client)

    output_bytes = obfuscate_csv_from_json(input_json)
    output_str = output_bytes.decode('utf-8')

    # Since no fields to obfuscate, output should match input.
    assert "some data" in output_str
    reader = csv.DictReader(io.StringIO(output_str))
    for row in reader:
        assert row["info"] == "some data"


def test_no_matching_fields(monkeypatch):
    # Test when specified pii_fields do not match any CSV headers.
    csv_content = (
        "id,info\n"
        "1,some data\n"
    )
    bucket = "bucket"
    key = "file.csv"
    s3_url = f"s3://{bucket}/{key}"
    input_json = json.dumps({
        "file_to_obfuscate": s3_url,
        "pii_fields": ["non_existent_field"]
    })

    fake_s3_client = FakeS3Client(bucket, key, csv_content)
    monkeypatch.setattr("gdpr_obfuscator.boto3.client", lambda service: fake_s3_client)

    output_bytes = obfuscate_csv_from_json(input_json)
    output_str = output_bytes.decode('utf-8')

    # Data should remain unchanged since no matching fields
    assert "some data" in output_str
    reader = csv.DictReader(io.StringIO(output_str))
    for row in reader:
        assert row["info"] == "some data"


def test_invalid_s3_url():
    # Test invalid S3 URL format
    input_json = json.dumps({
        "file_to_obfuscate": "invalid_url",
        "pii_fields": ["name"]
    })

    with pytest.raises(ValueError) as excinfo:
        obfuscate_csv_from_json(input_json)
    assert "Invalid S3 URL" in str(excinfo.value)


def test_empty_csv(monkeypatch):
    # Test with an empty CSV (only header, no data rows)
    csv_content = "id,name\n"  # header only
    bucket = "bucket"
    key = "empty.csv"
    s3_url = f"s3://{bucket}/{key}"
    input_json = json.dumps({
        "file_to_obfuscate": s3_url,
        "pii_fields": ["name"]
    })

    fake_s3_client = FakeS3Client(bucket, key, csv_content)
    monkeypatch.setattr("gdpr_obfuscator.boto3.client", lambda service: fake_s3_client)

    output_bytes = obfuscate_csv_from_json(input_json)
    output_str = output_bytes.decode('utf-8')

    # Output should at least contain the header unchanged
    assert "id,name" in output_str
    # No data rows should be present
    reader = csv.DictReader(io.StringIO(output_str))
    rows = list(reader)
    assert len(rows) == 0
