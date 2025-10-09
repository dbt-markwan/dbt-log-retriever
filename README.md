# dbt Cloud Log Retriever

A Python program to retrieve dbt Cloud logs from your environments with flexible filtering options.

## Features

- Fetches all environments from your dbt Cloud account
- Flexible filtering by deployment types, environment names, or IDs
- Retrieves runs from the last N days (configurable)
- Downloads run details and optionally writes combined logs from run steps
- Concurrent processing for faster retrieval
- Organizes logs by environment in a structured directory
- Supports regional dbt Cloud hosts

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

python dbt_cloud_log_retriever.py
```

### CLI Options

You can override environment variables and control behavior with flags:

```bash
python dbt_cloud_log_retriever.py \
  --base-url https://emea.dbt.com/api/v2 \   # or use --host emea.dbt.com
  --host emea.dbt.com \                      # optional; constructs https://<host>/api/v2
  --days-back 5 \                            # default: 5
  --deployment-types staging,production \    # filter by deployment type (optional)
  --env-names "Production,Staging" \         # filter by environment names (optional)
  --env-ids 123456,789012 \                  # filter by environment IDs (optional)
  --output-dir dbt_logs \                    # default: dbt_logs
  --write-logs \                             # write combined logs from step logs (default: off)
  --use-debug-logs \                         # when writing logs, use debug logs instead
  --no-run-steps \                           # exclude run_steps from API response (default: include)
  --concurrency 8                            # concurrent runs per environment (default: 4)
```

#### Filter Options

- **--deployment-types**: Comma-separated deployment types (e.g., `production` or `staging,production`). If not specified, all types are included.
- **--env-names**: Comma-separated environment names (exact match, case-sensitive).
- **--env-ids**: Comma-separated environment IDs.
- **Filter logic**: Filters work together with AND logic. For example, using both `--deployment-types production --env-names Production` will only match environments that are BOTH production type AND named "Production".
- **No filters**: If no filter arguments are provided, all environments are processed.

#### Other Options

- **--base-url vs --host**: If both provided, `--base-url` takes precedence. If neither provided, falls back to env vars, then defaults to `https://cloud.getdbt.com/api/v2`.
- **--write-logs**: By default, only run detail JSON is saved. Use this flag to write combined logs from run steps.
- **--use-debug-logs**: When `--write-logs` is provided, switches combined output to debug logs instead of regular logs.
- **--no-run-steps**: Exclude `run_steps` from API response to reduce payload size. By default, run_steps are included (needed for `--write-logs`).
- **--concurrency**: Number of runs to process concurrently per environment (default: 4).

### Examples

```bash
# Filter by deployment type: production environments only
python dbt_cloud_log_retriever.py \
  --deployment-types production \
  --days-back 7

# Filter by specific environment names
python dbt_cloud_log_retriever.py \
  --env-names "Production,Staging Analytics"

# Filter by specific environment IDs
python dbt_cloud_log_retriever.py \
  --env-ids 379972,386090

# Combine filters: production type AND specific name
python dbt_cloud_log_retriever.py \
  --deployment-types production \
  --env-names "Production"

# Get all environments (no filters)
python dbt_cloud_log_retriever.py \
  --deployment-types ""

# Production environments, last day, write combined debug logs with high concurrency
python dbt_cloud_log_retriever.py \
  --deployment-types production \
  --days-back 1 \
  --write-logs \
  --use-debug-logs \
  --concurrency 8

# Use EMEA regional host
python dbt_cloud_log_retriever.py \
  --host emea.dbt.com \
  --env-ids 123456

# Fetch only run metadata (no run_steps) for faster API responses
python dbt_cloud_log_retriever.py \
  --env-ids 379972 \
  --no-run-steps
```

### Using with .env file

```bash
# Install python-dotenv if not already installed
pip install python-dotenv

# Load environment variables and run
python -c "from dotenv import load_dotenv; load_dotenv(); exec(open('dbt_cloud_log_retriever.py').read())"
```

Or modify the script to load .env automatically by adding this at the top of `main()`:

```python
from dotenv import load_dotenv
load_dotenv()
```

### Programmatic Usage

You can also use the classes in your own Python code:

```python
from dbt_cloud_log_retriever import dbtCloudClient, dbtLogRetriever

# Initialize client
client = dbtCloudClient(
    api_token="your_token",
    account_id="your_account_id",
    base_url="https://cloud.getdbt.com/api/v2"  # optional
)

# Initialize retriever
retriever = dbtLogRetriever(
    client=client,
    output_dir="my_logs"
)

# Retrieve logs with various filtering options
retriever.retrieve_logs(
    deployment_types=["staging", "production"],  # optional
    env_names=["Production", "Staging"],         # optional
    env_ids=[379972, 386090],                    # optional
    days_back=7,                                 # last 7 days
    write_logs=True,                             # write combined logs
    use_debug_logs=True,                         # use debug logs
    concurrency=8,                               # concurrent processing
    include_run_steps=True                       # include run steps (default: True)
)

# Get only run metadata (no run_steps) for faster API responses
retriever.retrieve_logs(
    env_ids=[379972],
    days_back=3,
    save_details=True,
    include_run_steps=False  # exclude run_steps from API response
)

# Or get all environments with no filters
retriever.retrieve_logs(
    days_back=3,
    save_details=True,
    write_logs=True
)
```

## Output Structure

Logs are saved in the following structure:

```
dbt_logs/
├── Production_379972/
│   ├── run_434267619_details.json  # Always saved (default)
│   ├── run_434267619_logs.txt      # Optional (use --write-logs)
│   ├── run_434309651_details.json
│   └── run_434309651_logs.txt
└── Staging_386090/
    ├── run_434641833_details.json
    └── run_434641833_logs.txt
```

- Directory names follow the pattern: `{environment_name}_{environment_id}`
- Run details JSON files are always saved (unless `--no-save-details` is used)
- Combined log TXT files are only created when `--write-logs` is specified
- Log files contain combined output from all run steps, sorted by step index

## Workflow

The script follows these steps:

1. **Get all environments**: Fetches all environments from your dbt Cloud account
2. **Filter environments**: Applies optional filters by deployment type, environment name, or ID
3. **List runs**: For each filtered environment, lists all runs from the last N days
4. **Retrieve logs**: For each run, concurrently downloads:
   - Run details (JSON) - always saved by default
   - Combined logs (TXT) - optional, from run step logs (use `--write-logs`)
5. **Organize output**: Saves files in environment-specific directories

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

- `deployment_types`: Filter by environment deployment types (staging, production, etc.)
- `env_names`: Filter by specific environment names (exact match)
- `env_ids`: Filter by specific environment IDs
- `days_back`: Adjust the time window for run retrieval
- `output_dir`: Change where logs are saved
- `concurrency`: Number of concurrent runs to process per environment
- `write_logs`: Whether to write combined logs from run steps
- `use_debug_logs`: Whether to use debug logs instead of regular logs
- `include_run_steps`: Whether to include run_steps in API response (default: True, needed for `write_logs`)
- `limit`: Adjust the number of runs retrieved per API call (in `list_runs` method, default: 100)

## License

MIT License - Feel free to use and modify as needed.
