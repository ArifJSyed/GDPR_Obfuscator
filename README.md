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

