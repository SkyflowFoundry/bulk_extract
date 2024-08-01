# Skyflow Data and Token Dump Script

This Python script allows you to extract data from a Skyflow vault and save it to a CSV file. Additionally, it can tokenize the data and save the tokens to a separate CSV file.

## Features

- Extract data from Skyflow vault with specified redaction level.
- Supports parallel API calls for faster data retrieval.
- Option to tokenize data and save tokens to a separate CSV file.
- Logs errors to a specified file with retry logic for failed API calls.
- Option to specify a unique ID column to be the first column in the output CSV.

## Requirements

- Python 3.7+
- Skyflow Python SDK
- `requests` library
- `tqdm` library
- `concurrent.futures` library (included in the Python standard library)
- `tempfile` library (included in the Python standard library)

## Usage

### Command Line Arguments

```sh
--vaultid:              Required. Vault ID.
--vurl:                 Required. Vault URL (e.g., ebfc9bee4242.vault.skyflowapis.com).
--redaction             Required: Redaction level (`DEFAULT`, `REDACTED`, `MASKED`, `PLAIN_TEXT`).
--pc:                   Required. Path to the credentials JSON file (either --pc or --bt must be specified).
--bt:                   Required. Bearer token for API calls (either --pc or --bt must be specified).
--table:                Required. Table name in the vault.
--output:               Required. Path to the output CSV file.
--output_token_data:    Optional: Path to the output tokens CSV file. Should only be used with --dump_tokens parameter.
--dump_tokens:          Optional. Dump tokens in a separate CSV file.
--rows_per_call:        Optional. Number of rows to be extracted per API call (default: 25).
--unique_id_column:     Optional. Specify a unique ID column to be the first column in the output CSV
--mt:                   Optional. Maximum number of parallel API calls (default: 5, max: 7).
--log_error:            Optional. Path to the log file (default: error.log).
```


### Example Usage

```bash
python3 data_dump.py \
  --vaultid YOUR_VAULT_ID \
  --vurl YOUR_VAULT_URL \
  --redaction PLAIN_TEXT \
  --pc path/to/credentials.json \
  --table table_name \
  --output data_dump_output.csv \
  --output_token_data data_dump_token_output.csv \
  --mt 7 \
  --dump_tokens \
  --unique_id_column account_id \
  --rows_per_call 100
```

## Logging

Errors and failed records are logged in the specified log and failed records files.

## Important Notes:

1. Tokens can be dumped as standalone task. It can only be used with data dump process. The columns which does not have tokenization enabled will be left blank in the tokens output file.
2. Please this script in the directory with read write permissions
2. This is tested on Python 3.10.12. it may or may not work on lower versions
3. Do not increase the max number of rows to anything more than 25 as this is limit enforced on Skyflow Vault. The script will fail of this number is more than 25.
4. Skyflow Python SDK Should be installed to use this script. Refer to https://github.com/skyflowapi/skyflow-python?tab=readme-ov-file#installation for installing the skyflow SDK.
5. If you are going to use credentials.json file then make sure the file is in same directory or full path of the file is specified

## License

This is an open-source community script, and Skyflow bears no responsibility for its usage.


