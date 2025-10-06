# DBT Cloud Log Retriever

A Python program to retrieve dbt Cloud logs from staging and production environments.

## Features

- Fetches all environments from your dbt Cloud account
- Filters for staging and production deployment types
- Retrieves runs from the last 5 days (configurable)
- Downloads run logs and debug logs for each run
- Organizes logs by environment in a structured directory

## Prerequisites

- Python 3.7 or higher
- dbt Cloud account with API access
- dbt Cloud API token
- dbt Cloud account ID

## Installation

1. Clone or download this repository

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Set up your credentials:
   - Copy `.env.example` to `.env`
   - Fill in your dbt Cloud API token and account ID

```bash
cp .env.example .env
# Edit .env with your credentials
```

## Configuration

### Environment Variables

- `DBT_CLOUD_API_TOKEN`: Your dbt Cloud API token
  - Generate from: https://cloud.getdbt.com/#/profile/api/
  
- `DBT_CLOUD_ACCOUNT_ID`: Your dbt Cloud account ID
  - Find in the URL when logged into dbt Cloud

- `DBT_CLOUD_BASE_URL` (optional): Full base URL to the dbt Cloud API v2
  - Example: `https://cloud.getdbt.com/api/v2`
  - Use this to fully override the API endpoint

- `DBT_CLOUD_HOST` (optional): Domain for regional dbt Cloud hosts
  - Examples: `cloud.getdbt.com`, `emea.dbt.com`, `au.dbt.com`
  - The tool will construct `https://<host>/api/v2` automatically

### Optional: Load .env file

If you want to automatically load the `.env` file, you can modify the script or use:

```python
from dotenv import load_dotenv
load_dotenv()
```

## Usage

### Basic Usage

Export your environment variables and run the script:

```bash
export DBT_CLOUD_API_TOKEN="your_token_here"
export DBT_CLOUD_ACCOUNT_ID="your_account_id_here"
# Default US host (implicit): https://cloud.getdbt.com
# Or specify a regional host via DBT_CLOUD_HOST
export DBT_CLOUD_HOST="emea.dbt.com"   # or au.dbt.com

# Alternatively, provide the full base URL explicitly
# export DBT_CLOUD_BASE_URL="https://emea.dbt.com/api/v2"

python dbt_log_retriever.py
```

### CLI Options

You can override environment variables and control behavior with flags:

```bash
python dbt_log_retriever.py \
  --base-url https://emea.dbt.com/api/v2 \   # or use --host emea.dbt.com
  --host emea.dbt.com \                      # optional; constructs https://<host>/api/v2
  --days-back 5 \                            # default: 5
  --deployment-types staging,production \    # default: staging,production
  --output-dir dbt_logs \                    # default: dbt_logs
  --save-details \                           # also save run_<id>_details.json
  --use-debug-logs \                         # use step debug_logs instead of logs
  --concurrency 8                            # concurrent runs per environment (default: 4)
```

- **--base-url vs --host**: If both provided, `--base-url` takes precedence. If neither provided, falls back to env vars, then defaults to `https://cloud.getdbt.com/api/v2`.
- **--deployment-types**: comma-separated list, e.g., `production` or `staging,production`.
- **--use-debug-logs**: switches the combined output to use more verbose debug logs.
- **--save-details**: writes the full run detail JSON alongside the combined logs.

### Examples

```bash
# Production only for the last day, write combined debug logs, process runs concurrently
python dbt_log_retriever.py \
  --deployment-types production \
  --days-back 1 \
  --use-debug-logs \
  --concurrency 8

# Use EMEA regional host and save full details
python dbt_log_retriever.py \
  --host emea.dbt.com \
  --save-details
```

### Using with .env file

```bash
# Install python-dotenv if not already installed
pip install python-dotenv

# Load environment variables and run
python -c "from dotenv import load_dotenv; load_dotenv(); exec(open('dbt_log_retriever.py').read())"
```

Or modify the script to load .env automatically by adding this at the top of `main()`:

```python
from dotenv import load_dotenv
load_dotenv()
```

### Programmatic Usage

You can also use the classes in your own Python code:

```python
from dbt_log_retriever import DBTCloudClient, DBTLogRetriever

# Initialize client
client = DBTCloudClient(
    api_token="your_token",
    account_id="your_account_id"
)

# Initialize retriever
retriever = DBTLogRetriever(
    client=client,
    output_dir="my_logs"
)

# Retrieve logs
retriever.retrieve_logs(
    deployment_types=["staging", "production"],
    days_back=7  # Get logs from last 7 days
)
```

## Output Structure

Logs are saved in the following structure:

```
dbt_logs/
├── environment_name_123/
│   ├── run_456_details.json
│   ├── run_456_logs.txt
│   ├── run_456_debug.txt
│   ├── run_789_details.json
│   ├── run_789_logs.txt
│   └── run_789_debug.txt
└── another_environment_124/
    └── ...
```

## Workflow

The script follows these steps:

1. **Get all environments**: Fetches all environments from your dbt Cloud account
2. **Filter environments**: Filters for `staging` and `production` deployment types
3. **List runs**: For each filtered environment, lists all runs from the last 5 days
4. **Retrieve logs**: For each run, downloads:
   - Run details (JSON)
   - Run logs (TXT)
   - Debug logs (TXT)

## Error Handling

- The script includes comprehensive error handling and logging
- Failed API requests will be logged but won't stop the entire process
- Missing logs will be noted in the output

## API Documentation

This script uses the dbt Cloud API v2:
- [List Environments](https://docs.getdbt.com/dbt-cloud/api-v2#/operations/List%20Environments)
- [List Runs](https://docs.getdbt.com/dbt-cloud/api-v2#/operations/List%20Runs)
- [Retrieve Run](https://docs.getdbt.com/dbt-cloud/api-v2#/operations/Retrieve%20Run)

## Customization

You can customize the behavior by modifying these parameters:

- `deployment_types`: Change which environment types to include
- `days_back`: Adjust the time window for run retrieval
- `output_dir`: Change where logs are saved
- `limit`: Adjust the number of runs retrieved per API call (in `list_runs` method)

## License

MIT License - Feel free to use and modify as needed.
