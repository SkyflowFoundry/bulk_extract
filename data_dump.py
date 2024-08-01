import argparse
import requests
import csv
import os
import time
from tqdm import tqdm
import concurrent.futures
import tempfile
from skyflow.service_account import generate_bearer_token, is_expired

# Set up argument parser
parser = argparse.ArgumentParser(description="Dump data from Skyflow vault and write to output CSV.")
parser.add_argument('--vaultid', required=True, help='Vault ID (Required)')
parser.add_argument('--vurl', required=True, help='Vault URL (e.g., identifier.vault.skyflowapis.com) (Required)')
parser.add_argument('--redaction', required=True, choices=['DEFAULT', 'REDACTED', 'MASKED', 'PLAIN_TEXT'], help='Redaction level (Required)')
parser.add_argument('--pc', help='Path to the credentials JSON file (Required if --bt is not provided)')
parser.add_argument('--bt', help='Bearer token to call the API (Required if --pc is not provided)')
parser.add_argument('--table', required=True, help='Table name in the vault (Required)')
parser.add_argument('--output', required=True, help='Path to the output CSV file (Required)')
parser.add_argument('--output_token_data', help='Path to the output tokens CSV file (Optional)')
parser.add_argument('--mt', type=int, choices=range(1, 8), default=5, help='Maximum number of parallel API calls (default: 5, max: 7) (Optional)')
parser.add_argument('--dump_tokens', action='store_true', help='Dump tokens in a separate CSV file (Optional)')
parser.add_argument('--unique_id_column', help='Specify a unique ID column to be the first column in the output CSV (Optional)')
parser.add_argument('--rows_per_call', type=int, default=25, help='Number of rows to retrieve per API call (default: 25) (Optional)')
parser.add_argument('--log_error', default='error_log.txt', help='File path for error log (default: error_log.txt) (Optional)')
args = parser.parse_args()

# Variables from command line parameters
VAULT_ID = args.vaultid
VAULT_URL = args.vurl
REDACTION_LEVEL = args.redaction
PATH_TO_CREDENTIALS_JSON = args.pc
BEARER_TOKEN = args.bt
TABLE_NAME = args.table
OUTPUT_CSV = args.output
OUTPUT_TOKEN_CSV = args.output_token_data
MAX_PARALLEL_TASKS = args.mt
ROWS_PER_CHUNK = args.rows_per_call
DUMP_TOKENS_ENABLED = args.dump_tokens
UNIQUE_ID_COLUMN = args.unique_id_column
ERROR_LOG_FILE = args.log_error

# Ensure that either PATH_TO_CREDENTIALS_JSON or BEARER_TOKEN is provided, but not both
if (PATH_TO_CREDENTIALS_JSON and BEARER_TOKEN) or (not PATH_TO_CREDENTIALS_JSON and not BEARER_TOKEN):
    parser.error('You must provide either --pc or --bt, but not both.')

# Function to generate bearer token
bearerToken = BEARER_TOKEN
tokenType = 'Bearer'

def token_provider():
    global bearerToken
    global tokenType
    if not bearerToken:
        if bearerToken is None or is_expired(bearerToken):
            bearerToken, tokenType = generate_bearer_token(PATH_TO_CREDENTIALS_JSON)
    return bearerToken, tokenType

# Function to log errors
def log_error(message):
    with open(ERROR_LOG_FILE, 'a') as error_log:
        error_log.write(f"{message}\n")

# Function to make API call with retry logic
def make_api_call(url, headers, method='GET', data=None):
    retries = 3
    for attempt in range(retries):
        try:
            if method == 'GET':
                response = requests.get(url, headers=headers)
            elif method == 'POST':
                response = requests.post(url, headers=headers, json=data)
            response.raise_for_status()  # Raise an exception for HTTP errors
            return response
        except requests.RequestException as e:
            if attempt < retries - 1:
                time.sleep(2 ** attempt)  # Exponential backoff
                continue
            else:
                log_error(f"API call failed after {retries} attempts: {e}")
                return None

# Function to process a chunk of rows
def process_chunk(offset, headers, fieldnames, temp_output_path):
    api_url = f"https://{VAULT_URL}/v1/vaults/{VAULT_ID}/{TABLE_NAME}?redaction={REDACTION_LEVEL}&offset={offset}&order_by=ASCENDING"
    response = make_api_call(api_url, headers)
    if response and response.status_code == 200:
        response_data = response.json()
        records = response_data.get('records', [])

        with open(temp_output_path, mode='a', newline='') as tempfile_output:
            writer = csv.DictWriter(tempfile_output, fieldnames=fieldnames)
            for record in records:
                row = {key: value for key, value in record['fields'].items()}
                writer.writerow(row)
        return len(records)
    else:
        error_message = f"Error occurred at offset {offset}: {response.text if response else 'No response'}"
        log_error(error_message)
        print(error_message)
        return 0

# Function to process tokens for a chunk of skyflow_ids
def process_token_chunk(skyflow_ids, headers, temp_output_token_path, fieldnames):
    api_url = f"https://{VAULT_URL}/v1/vaults/{VAULT_ID}/{TABLE_NAME}?tokenization=true"
    for skyflow_id in skyflow_ids:
        api_url += f"&skyflow_ids={skyflow_id}"
    response = make_api_call(api_url, headers)
    
    if response and response.status_code == 200:
        response_data = response.json()
        records = response_data.get('records', [])

        with open(temp_output_token_path, mode='a', newline='') as tempfile_output:
            writer = csv.DictWriter(tempfile_output, fieldnames=fieldnames)
            for record in records:
                fields = record.get('fields', {})
                row = {field: fields.get(field, '') for field in fieldnames}
                writer.writerow(row)
        return len(records)
    else:
        error_message = f"Error occurred for skyflow_ids {skyflow_ids}: {response.text if response else 'No response'}"
        log_error(error_message)
        print(error_message)
        return 0

# Generate bearer token if not provided
if not BEARER_TOKEN:
    accessToken, tokenType = token_provider()
else:
    accessToken = BEARER_TOKEN
headers = {
    "Authorization": f"Bearer {accessToken}",
    "Content-Type": "application/json"
}

# Get the total record count
query_url = f"https://{VAULT_URL}/v1/vaults/{VAULT_ID}/query"
query_payload = {"query": f"select count(*) from {TABLE_NAME}"}
query_response = make_api_call(query_url, headers, method='POST', data=query_payload)
if not query_response or query_response.status_code != 200:
    error_message = f"Failed to retrieve record count: {query_response.text if query_response else 'No response'}"
    log_error(error_message)
    print(error_message)
    exit(1)

query_data = query_response.json()

# Extract the total record count
total_records = query_data['records'][0]['fields']['count(*)']
print(f"Total records to retrieve: {total_records}")

# Create a temporary file to buffer the output
with tempfile.NamedTemporaryFile(delete=False, mode='w', newline='') as temp_output_file:
    temp_output_path = temp_output_file.name
    writer = csv.DictWriter(temp_output_file, fieldnames=[])
    writer.writeheader()

# Read the first batch to get the field names
initial_response = make_api_call(f"https://{VAULT_URL}/v1/vaults/{VAULT_ID}/{TABLE_NAME}?redaction={REDACTION_LEVEL}&offset=0&order_by=ASCENDING", headers)
if not initial_response or initial_response.status_code != 200:
    error_message = f"Failed to retrieve data: {initial_response.text if initial_response else 'No response'}"
    log_error(error_message)
    print(error_message)
    exit(1)

initial_data = initial_response.json()

if 'records' not in initial_data or not initial_data['records']:
    error_message = "No records found in initial response"
    log_error(error_message)
    print(error_message)
    exit(1)

fieldnames = initial_data['records'][0]['fields'].keys()

# Check for unique_id_column validity
if UNIQUE_ID_COLUMN:
    if UNIQUE_ID_COLUMN not in fieldnames:
        error_message = "Unique column specified is not found. Please rerun the script without the parameter or specify the correct name of unique column."
        log_error(error_message)
        print(error_message)
        exit(1)
    # Reorder fieldnames with unique_id_column first and skyflow_id second
    fieldnames = [UNIQUE_ID_COLUMN, 'skyflow_id'] + [f for f in fieldnames if f not in [UNIQUE_ID_COLUMN, 'skyflow_id']]
else:
    # Ensure skyflow_id is the first column
    fieldnames = ['skyflow_id'] + [f for f in fieldnames if f != 'skyflow_id']

print(f"Fieldnames for CSV: {fieldnames}")  # Debug statement

# Write header to temporary file
with open(temp_output_path, mode='w', newline='') as temp_output_file:
    writer = csv.DictWriter(temp_output_file, fieldnames=fieldnames)
    writer.writeheader()

# Start timing the process
start_time = time.time()

# Process records in chunks with known total record count
offset = 0
total_records_dumped = 0
with tqdm(total=total_records, desc="Dumping Data") as pbar:
    while offset < total_records:
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_PARALLEL_TASKS) as executor:
            futures = []
            for i in range(MAX_PARALLEL_TASKS):
                if offset >= total_records:
                    break
                futures.append(executor.submit(process_chunk, offset, headers, fieldnames, temp_output_path))
                offset += ROWS_PER_CHUNK

            for future in concurrent.futures.as_completed(futures):
                retrieved_records = future.result()
                total_records_dumped += retrieved_records
                pbar.update(retrieved_records)

# Move data from temporary file to final output file
with open(temp_output_path, mode='r') as temp_output_file, open(OUTPUT_CSV, mode='w', newline='') as outfile:
    writer = csv.writer(outfile)
    for row in csv.reader(temp_output_file):
        writer.writerow(row)

# Clean up temporary file
os.remove(temp_output_path)

# Calculate and print total time taken
end_time = time.time()
total_time_taken = end_time - start_time
print(f"Data dump completed. Total records dumped: {total_records_dumped}")
print(f"Total time taken: {total_time_taken:.2f} seconds")

# Tokenization process
if DUMP_TOKENS_ENABLED:
    start_time_tokenization = time.time()
    skyflow_ids = []
    additional_fields = {}
    with open(OUTPUT_CSV, mode='r') as infile:
        reader = csv.DictReader(infile)
        for row in reader:
            skyflow_ids.append(row['skyflow_id'])
            if UNIQUE_ID_COLUMN:
                additional_fields[row['skyflow_id']] = row[UNIQUE_ID_COLUMN]

    total_ids = len(skyflow_ids)
    print(f"Total skyflow_ids to tokenize: {total_ids}")

    # Create a temporary file to buffer the tokenized output
    with tempfile.NamedTemporaryFile(delete=False, mode='w', newline='') as temp_output_token_file:
        temp_output_token_path = temp_output_token_file.name
        writer = csv.DictWriter(temp_output_token_file, fieldnames=fieldnames)
        writer.writeheader()

    # Process skyflow_ids in chunks for tokenization
    offset = 0
    total_tokens_dumped = 0
    with tqdm(total=total_ids, desc="Dumping Tokens") as pbar:
        while offset < total_ids:
            with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_PARALLEL_TASKS) as executor:
                futures = []
                for i in range(MAX_PARALLEL_TASKS):
                    if offset >= total_ids:
                        break
                    end_index = min(offset + ROWS_PER_CHUNK, total_ids)
                    futures.append(executor.submit(process_token_chunk, skyflow_ids[offset:end_index], headers, temp_output_token_path, fieldnames))
                    offset += ROWS_PER_CHUNK

                for future in concurrent.futures.as_completed(futures):
                    retrieved_tokens = future.result()
                    total_tokens_dumped += retrieved_tokens
                    pbar.update(retrieved_tokens)

    # Read tokenized data from temporary file and merge with additional_fields from data dump
    merged_data = []
    with open(temp_output_token_path, mode='r') as temp_output_token_file:
        reader = csv.DictReader(temp_output_token_file)
        for row in reader:
            if UNIQUE_ID_COLUMN and row['skyflow_id'] in additional_fields:
                row[UNIQUE_ID_COLUMN] = additional_fields[row['skyflow_id']]
            merged_data.append(row)

    # Write merged data to final tokenized output file
    with open(OUTPUT_TOKEN_CSV, mode='w', newline='') as outfile:
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()
        for row in merged_data:
            writer.writerow(row)

    # Clean up temporary file
    os.remove(temp_output_token_path)

    # Calculate and print total time taken for tokenization
    end_time_tokenization = time.time()
    total_time_tokenization = end_time_tokenization - start_time_tokenization
    print(f"Tokenization completed. Total tokens dumped: {total_tokens_dumped}")
    print(f"Total time taken for tokenization: {total_time_tokenization:.2f} seconds")
