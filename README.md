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

## Usage

**def obfuscate_file(json_input_str: str) -> bytes:**
Obfuscates specified fields in a file stored on S3, supporting CSV, JSON, and Parquet formats.

This function expects a JSON string with the following keys:

- file_to_obfuscate: S3 URL of the file (e.g., s3://bucket/key)
- pii_fields: A list of field names containing PII to be obfuscated.
  
The function downloads the file from S3, determines the file type based on its extension, obfuscates the PII fields using the appropriate helper function, and returns the modified file as bytes.

- Args: json_input_str (str): A JSON string containing: - file_to_obfuscate: S3 URL of the file. - pii_fields: List of PII fields to obfuscate.

- Returns: bytes: A byte stream of the obfuscated file, ready for use with boto3 S3 PutObject.

- Raises: ValueError: If the S3 URL is invalid or if the file type is unsupported.

  Further documentation can be found here: https://arifjsyed.github.io/GDPR_Obfuscator/
