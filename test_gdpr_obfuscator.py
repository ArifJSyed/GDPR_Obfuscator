import json
import csv
import io
import pandas as pd
import pytest
from io import BytesIO
from gdpr_obfuscator import obfuscate_csv_from_json

# Helper classes to simulate S3 behavior for testing
class FakeS3Body:
    def __init__(self, data):
        # Data should be bytes
        self.data = data

    def read(self):
        return self.data

class FakeS3Client:
    def __init__(self, bucket, key, data):
        self.bucket = bucket
        self.key = key
        self.data = data  # bytes

    def get_object(self, Bucket, Key):
        if Bucket == self.bucket and Key == self.key:
            return {'Body': FakeS3Body(self.data)}
        raise Exception("Object not found")

# CSV Tests

def test_obfuscate_csv(monkeypatch):
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

    fake_s3_client = FakeS3Client(bucket, key, csv_content.encode('utf-8'))
    monkeypatch.setattr("gdpr_obfuscator.boto3.client", lambda service: fake_s3_client)

    output_bytes = obfuscate_csv_from_json(input_json)
    output_str = output_bytes.decode('utf-8')

    assert "***" in output_str
    assert "John Smith" not in output_str
    assert "j.smith@email.com" not in output_str

    assert "1234" in output_str
    assert "Software" in output_str

    reader = csv.DictReader(io.StringIO(output_str))
    for row in reader:
        assert row["name"] == "***"
        assert row["email_address"] == "***"

def test_no_pii(monkeypatch):
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

    fake_s3_client = FakeS3Client(bucket, key, csv_content.encode('utf-8'))
    monkeypatch.setattr("gdpr_obfuscator.boto3.client", lambda service: fake_s3_client)

    output_bytes = obfuscate_csv_from_json(input_json)
    output_str = output_bytes.decode('utf-8')

    assert "some data" in output_str
    reader = csv.DictReader(io.StringIO(output_str))
    for row in reader:
        assert row["info"] == "some data"

def test_no_matching_fields(monkeypatch):
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

    fake_s3_client = FakeS3Client(bucket, key, csv_content.encode('utf-8'))
    monkeypatch.setattr("gdpr_obfuscator.boto3.client", lambda service: fake_s3_client)

    output_bytes = obfuscate_csv_from_json(input_json)
    output_str = output_bytes.decode('utf-8')

    assert "some data" in output_str
    reader = csv.DictReader(io.StringIO(output_str))
    for row in reader:
        assert row["info"] == "some data"

def test_invalid_s3_url():
    input_json = json.dumps({
        "file_to_obfuscate": "invalid_url",
        "pii_fields": ["name"]
    })

    with pytest.raises(ValueError) as excinfo:
        obfuscate_csv_from_json(input_json)
    assert "Invalid S3 URL" in str(excinfo.value)

def test_empty_csv(monkeypatch):
    csv_content = "id,name\n"  # header only
    bucket = "bucket"
    key = "empty.csv"
    s3_url = f"s3://{bucket}/{key}"
    input_json = json.dumps({
        "file_to_obfuscate": s3_url,
        "pii_fields": ["name"]
    })

    fake_s3_client = FakeS3Client(bucket, key, csv_content.encode('utf-8'))
    monkeypatch.setattr("gdpr_obfuscator.boto3.client", lambda service: fake_s3_client)

    output_bytes = obfuscate_csv_from_json(input_json)
    output_str = output_bytes.decode('utf-8')

    assert "id,name" in output_str
    reader = csv.DictReader(io.StringIO(output_str))
    rows = list(reader)
    assert len(rows) == 0

# JSON Tests

def test_obfuscate_json(monkeypatch):
    data = [
        {"id": 1, "name": "John Smith", "email": "john@example.com"},
        {"id": 2, "name": "Jane Doe", "email": "jane@example.com"}
    ]
    json_content = json.dumps(data)
    bucket = "bucket"
    key = "data/file.json"
    s3_url = f"s3://{bucket}/{key}"
    input_json = json.dumps({
        "file_to_obfuscate": s3_url,
        "pii_fields": ["name", "email"]
    })

    fake_s3_client = FakeS3Client(bucket, key, json_content.encode('utf-8'))
    monkeypatch.setattr("gdpr_obfuscator.boto3.client", lambda service: fake_s3_client)

    output_bytes = obfuscate_csv_from_json(input_json)
    output_text = output_bytes.decode('utf-8')
    result_data = json.loads(output_text)

    for item in result_data:
        assert item["name"] == "***"
        assert item["email"] == "***"

# Parquet Tests

def test_obfuscate_parquet(monkeypatch):
    # Create a small DataFrame
    df = pd.DataFrame({
        "id": [1, 2],
        "name": ["John Smith", "Jane Doe"],
        "email": ["john@example.com", "jane@example.com"],
        "other": ["data1", "data2"]
    })
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    parquet_content = buffer.getvalue()

    bucket = "bucket"
    key = "data/file.parquet"
    s3_url = f"s3://{bucket}/{key}"
    input_json = json.dumps({
        "file_to_obfuscate": s3_url,
        "pii_fields": ["name", "email"]
    })

    fake_s3_client = FakeS3Client(bucket, key, parquet_content)
    monkeypatch.setattr("gdpr_obfuscator.boto3.client", lambda service: fake_s3_client)

    output_bytes = obfuscate_csv_from_json(input_json)
    result_df = pd.read_parquet(BytesIO(output_bytes))

    assert all(result_df["name"] == "***")
    assert all(result_df["email"] == "***")
    assert all(result_df["id"] == df["id"])
    assert all(result_df["other"] == df["other"])

def test_unsupported_file_type(monkeypatch):
    bucket = "bucket"
    key = "data/file.txt"
    s3_url = f"s3://{bucket}/{key}"
    input_json = json.dumps({
        "file_to_obfuscate": s3_url,
        "pii_fields": ["field"]
    })

    fake_s3_client = FakeS3Client(bucket, key, b"some content")
    monkeypatch.setattr("gdpr_obfuscator.boto3.client", lambda service: fake_s3_client)

    with pytest.raises(ValueError) as excinfo:
        obfuscate_csv_from_json(input_json)
    assert "Unsupported file type" in str(excinfo.value)
