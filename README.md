# GDPR Obfuscator

The **GDPR Obfuscator** is a Python library designed to intercept and obfuscate personally identifiable information (PII) from files stored in AWS S3. It supports CSV, JSON, and Parquet file formats, replacing sensitive fields with obfuscated strings (e.g., `"***"`). This tool helps ensure compliance with GDPR by anonymizing sensitive data before it is stored or processed for analysis.

## Features
- **Multi-format Support:** Handles CSV, JSON, and Parquet files.
- **Flexible Field Obfuscation:** Specify which fields contain PII to be obfuscated.
- **AWS S3 Integration:** Reads files from S3 and outputs a byte-stream suitable for uploading back to S3 via boto3.
- **Extensible:** Easily integrates into existing Python codebases and workflows such as AWS Lambda, Step Functions, or Airflow.

## Installation

1. **Clone the Repository:**
   ```bash
   git clone https://github.com/ArifJSyed/gdpr-obfuscator.git
   cd gdpr-obfuscator

2. **Use Makefile to create the environment:**
   All requirements will be installed into a Python virtual environment (venv)
   ```bash
   make create-environment

3. **Use Makefile to build the environment requirements:**
   ```bash
   make requirements

4. **Use Makefile to set up dev requirements:**
   ```bash
   make dev-setup

5. **Run checks all checks for code compliance, security and unit testing (can be run individually - see Makefile for breakdown):**
   ```bash
   make run-checks

## Usage

The code can be run from within the venv. To activate the venv, run:
   ```bash
   source venv/bin/activate
   ```
**gdpr_obfuscator.py** contains the main code and
**test_gdpr_obfuscator.py** contains the tests for use with pytest which can be run from the Makefile eg. **make unit-test**

## Main function

**def obfuscate_file(json_input_str: str) -> bytes:**
Obfuscates specified fields in a file stored on S3, supporting CSV, JSON, and Parquet formats.

This function expects a JSON string with the following keys:

- **file_to_obfuscate:** S3 URL of the file (e.g., s3://bucket/key)
- **pii_fields:** A list of field names containing PII to be obfuscated.
  
The function downloads the file from S3, determines the file type based on its extension, obfuscates the PII fields using the appropriate helper function, and returns the modified file as bytes.

- **Args:** json_input_str (str): A JSON string containing: - file_to_obfuscate: S3 URL of the file. - pii_fields: List of PII fields to obfuscate.

- **Returns:** bytes: A byte stream of the obfuscated file, ready for use with boto3 S3 PutObject.

- **Raises:** ValueError: If the S3 URL is invalid or if the file type is unsupported.

## Example usage

The input might be:
```json
{
    "file_to_obfuscate": "s3://my_ingestion_bucket/new_data/file1.csv",
    "pii_fields": ["name", "email_address"]
}
```

The target CSV file would look like this:
```csv
student_id,name,course,cohort,graduation_date,email_address
...
1234,'John Smith','Software','2024-03-31','j.smith@email.com'
...
```

The output will be a byte-stream representation of a file like this:
```csv
student_id,name,course,cohort,graduation_date,email_address
...
1234,'***','Software','2024-03-31','***'
...
```

The output format provides content compatible with the `boto3` [S3 Put Object](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/put_object.html) function.

Further documentation can be found here: https://arifjsyed.github.io/GDPR_Obfuscator/

[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
