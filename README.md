# dbt Cloud Log Retriever

A Python program to retrieve dbt Cloud logs from your environments with flexible filtering options.

## Features

- Fetches all environments from your dbt Cloud account
- Flexible filtering by deployment types, environment names, or IDs
- **Efficient data fetching** with configurable limit parameter
- Date range filtering by creation time or finish time (client-side precision)
- Hybrid approach: reduced API payload + precise filtering
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
  --base-url https://emea.dbt.com/api/v2 \      # or use --host emea.dbt.com
  --host emea.dbt.com \                         # optional; constructs https://<host>/api/v2
  --deployment-types staging,production \       # filter by deployment type (optional)
  --env-names "Production,Staging" \            # filter by environment names (optional)
  --env-ids 123456,789012 \                     # filter by environment IDs (optional)
  --days-back 5 \                               # convenience: last N days (optional)
  --created-after 2024-01-01T00:00:00Z \        # filter runs created after (optional)
  --created-before 2024-12-31T23:59:59Z \       # filter runs created before (optional)
  --finished-after 2024-01-01T00:00:00Z \       # filter runs finished after (optional)
  --finished-before 2024-12-31T23:59:59Z \      # filter runs finished before (optional)
  --limit 100 \                                 # max runs per environment (default: 100)
  --output-dir dbt_logs \                       # default: dbt_logs
  --write-logs \                                # write combined logs from step logs (default: off)
  --use-debug-logs \                            # when writing logs, use debug logs instead
  --concurrency 8                               # concurrent runs per environment (default: 4)
```

#### Environment Filter Options

- **--deployment-types**: Comma-separated deployment types (e.g., `production` or `staging,production`). If not specified, all types are included.
- **--env-names**: Comma-separated environment names (exact match, case-sensitive).
- **--env-ids**: Comma-separated environment IDs.
- **Filter logic**: Filters work together with AND logic. For example, using both `--deployment-types production --env-names Production` will only match environments that are BOTH production type AND named "Production".
- **No filters**: If no filter arguments are provided, all environments are processed.

#### Run Filter Options (Hybrid Filtering)

These filters use a **hybrid approach** for optimal performance: the `--limit` parameter reduces API payload size, while date filters are applied client-side for precision:

- **--days-back**: Convenience option to fetch runs from the last N days. Overrides `--created-after`.
- **--created-after**: Filter runs created after this ISO 8601 datetime (e.g., `2024-01-01T00:00:00Z`).
- **--created-before**: Filter runs created before this ISO 8601 datetime.
- **--finished-after**: Filter runs that finished after this ISO 8601 datetime.
- **--finished-before**: Filter runs that finished before this ISO 8601 datetime.
- **--limit**: Maximum number of runs to retrieve per environment (default: 100). Controls API payload size.

**Performance Note**: The `--limit` parameter reduces data transfer by limiting API response size. Date filters are then applied client-side for precise control. This hybrid approach combines efficiency with flexibility.

#### Other Options

- **--base-url vs --host**: If both provided, `--base-url` takes precedence. If neither provided, falls back to env vars, then defaults to `https://cloud.getdbt.com/api/v2`.
- **--write-logs**: By default, only run detail JSON is saved. Use this flag to write combined logs from run steps.
- **--use-debug-logs**: When `--write-logs` is provided, switches combined output to debug logs instead of regular logs.
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

# Use date range filtering (server-side): runs created in January 2024
python dbt_cloud_log_retriever.py \
  --created-after 2024-01-01T00:00:00Z \
  --created-before 2024-01-31T23:59:59Z

# Filter by finished date: runs that completed in the last week
python dbt_cloud_log_retriever.py \
  --finished-after 2024-01-15T00:00:00Z \
  --finished-before 2024-01-22T23:59:59Z

# Combine environment and date filters: Production env, runs from last 30 days, limit 200 runs
python dbt_cloud_log_retriever.py \
  --deployment-types production \
  --days-back 30 \
  --limit 200

# Get specific date range with higher limit for busy environments
python dbt_cloud_log_retriever.py \
  --env-ids 379972 \
  --created-after 2024-01-01T00:00:00Z \
  --created-before 2024-03-31T23:59:59Z \
  --limit 500

# Use EMEA regional host with date filtering
python dbt_cloud_log_retriever.py \
  --host emea.dbt.com \
  --env-ids 123456 \
  --days-back 7
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
    deployment_types=["staging", "production"],  # optional environment filters
    env_names=["Production", "Staging"],         # optional
    env_ids=[379972, 386090],                    # optional
    days_back=7,                                 # convenience: last 7 days
    write_logs=True,                             # write combined logs
    use_debug_logs=True,                         # use debug logs
    concurrency=8,                               # concurrent processing
    limit=100                                    # max runs per environment
)

# Use date range filtering (server-side for efficiency)
retriever.retrieve_logs(
    created_after="2024-01-01T00:00:00Z",       # runs created after
    created_before="2024-01-31T23:59:59Z",      # runs created before
    finished_after="2024-01-15T00:00:00Z",      # runs finished after
    finished_before="2024-01-31T23:59:59Z",     # runs finished before
    limit=200,                                   # higher limit for busy periods
    write_logs=True
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
3. **List runs (optimized fetching)**: For each filtered environment:
   - Fetches runs with configurable limit parameter (reduces payload size)
   - Applies date range filters client-side (precise control)
   - Hybrid approach: efficiency + flexibility
4. **Retrieve logs**: For each run, concurrently downloads:
   - Run details (JSON) - always saved by default
   - Combined logs (TXT) - optional, from run step logs (use `--write-logs`)
5. **Organize output**: Saves files in environment-specific directories

### Performance Benefits

**Hybrid filtering approach** provides optimal performance:

**Efficiency (limit parameter)**:
- Reduces API payload size by limiting response
- Faster API responses
- Lower bandwidth usage
- Configurable based on your needs

**Precision (client-side filtering)**:
- Accurate date range filtering
- Supports both created_at and finished_at timestamps
- Flexible filter combinations
- No API limitations

This approach works around dbt Cloud API v2 limitations while maintaining performance and providing the filtering flexibility users need.

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

### Environment Filters
- `deployment_types`: Filter by environment deployment types (staging, production, etc.)
- `env_names`: Filter by specific environment names (exact match)
- `env_ids`: Filter by specific environment IDs

### Run Filters (Hybrid Approach)
- `days_back`: Convenience parameter for last N days (overrides created_after)
- `created_after`: Filter runs created after this ISO 8601 datetime (client-side)
- `created_before`: Filter runs created before this ISO 8601 datetime (client-side)
- `finished_after`: Filter runs finished after this ISO 8601 datetime (client-side)
- `finished_before`: Filter runs finished before this ISO 8601 datetime (client-side)
- `limit`: Maximum number of runs per environment (default: 100, reduces API payload)

### Output Options
- `output_dir`: Change where logs are saved
- `write_logs`: Whether to write combined logs from run steps
- `use_debug_logs`: Whether to use debug logs instead of regular logs
- `save_details`: Whether to save run details JSON (default: True)

### Performance Options
- `concurrency`: Number of concurrent runs to process per environment (default: 4)

## License

MIT License - Feel free to use and modify as needed.
