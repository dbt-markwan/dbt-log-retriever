#!/usr/bin/env python3
"""
dbt Cloud Log Retriever

This script retrieves dbt Cloud logs by:
1. Fetching all environments
2. Filtering for staging/production deployment types
3. Listing runs from the last 5 days for each environment
4. Retrieving run and debug logs for each run
"""

import os
import sys
import requests
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional
import json
import logging
from pathlib import Path
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class dbtCloudClient:
    """Client for interacting with dbt Cloud API v2"""
    
    def __init__(self, api_token: str, account_id: str, base_url: str = "https://cloud.getdbt.com/api/v2"):
        """
        Initialize the dbt Cloud API client
        
        Args:
            api_token: dbt Cloud API token
            account_id: dbt Cloud account ID
            base_url: Base URL for dbt Cloud API (default: https://cloud.getdbt.com/api/v2)
        """
        self.api_token = api_token
        self.account_id = account_id
        self.base_url = base_url
        self.headers = {
            "Authorization": f"Token {api_token}",
            "Content-Type": "application/json"
        }
        # Reuse a session for connection pooling and lower latency
        self._session = requests.Session()
        self._session.headers.update(self.headers)
    
    def _make_request(self, method: str, endpoint: str, params: Optional[Dict] = None) -> Dict:
        """
        Make an API request to dbt Cloud
        
        Args:
            method: HTTP method (GET, POST, etc.)
            endpoint: API endpoint
            params: Query parameters
            
        Returns:
            JSON response as dictionary
        """
        url = f"{self.base_url}/{endpoint}"
        
        try:
            response = self._session.request(
                method=method,
                url=url,
                params=params,
                timeout=30,
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {e}")
            raise
    
    def get_environments(self) -> List[Dict]:
        """
        Get all environments for the account
        
        Returns:
            List of environment dictionaries
        """
        logger.info(f"Fetching environments for account {self.account_id}")
        endpoint = f"accounts/{self.account_id}/environments/"
        
        response = self._make_request("GET", endpoint)
        environments = response.get("data", [])
        logger.info(f"Found {len(environments)} environments")
        
        return environments
    
    def filter_environments(
        self, 
        environments: List[Dict], 
        deployment_types: Optional[List[str]] = None,
        env_names: Optional[List[str]] = None,
        env_ids: Optional[List[int]] = None
    ) -> List[Dict]:
        """
        Filter environments by deployment type, name, and/or ID
        
        Args:
            environments: List of environment dictionaries
            deployment_types: List of deployment types to filter by (e.g., ['staging', 'production'])
            env_names: List of environment names to filter by (exact match)
            env_ids: List of environment IDs to filter by
            
        Returns:
            Filtered list of environments
        """
        filtered = environments
        filters_applied = []
        
        # Filter by deployment types
        if deployment_types:
            filtered = [env for env in filtered if env.get("deployment_type") in deployment_types]
            filters_applied.append(f"deployment_types={deployment_types}")
        
        # Filter by environment names
        if env_names:
            filtered = [env for env in filtered if env.get("name") in env_names]
            filters_applied.append(f"env_names={env_names}")
        
        # Filter by environment IDs
        if env_ids:
            filtered = [env for env in filtered if env.get("id") in env_ids]
            filters_applied.append(f"env_ids={env_ids}")
        
        if filters_applied:
            logger.info(f"Filtered to {len(filtered)} environments with filters: {', '.join(filters_applied)}")
        else:
            logger.info(f"No filters applied, using all {len(filtered)} environments")
        
        return filtered
    
    def list_runs(
        self, 
        environment_id: int, 
        days_back: Optional[int] = None,
        created_after: Optional[str] = None,
        created_before: Optional[str] = None,
        finished_after: Optional[str] = None,
        finished_before: Optional[str] = None,
        limit: int = 100
    ) -> List[Dict]:
        """
        List runs for a specific environment with server-side filtering via API query parameters
        
        Args:
            environment_id: Environment ID
            days_back: Number of days to look back (default: None). If specified, overrides created_after.
            created_after: ISO 8601 datetime string for created_at start range (e.g., "2024-01-01T00:00:00Z")
            created_before: ISO 8601 datetime string for created_at end range
            finished_after: ISO 8601 datetime string for finished_at start range
            finished_before: ISO 8601 datetime string for finished_at end range
            limit: Maximum number of runs to retrieve (default: 100)
            
        Returns:
            List of run dictionaries (filtered server-side by the API)
        """
        endpoint = f"accounts/{self.account_id}/runs/"
        
        # Build API parameters for server-side filtering
        params = {
            "environment_id": environment_id,
            "order_by": "-created_at",  # Most recent first
            "limit": limit
        }
        
        # Store date filters for client-side filtering
        # Note: The dbt Cloud API v2 appears to have limited support for date range filtering
        # We'll fetch with limit and filter client-side if date filters are specified
        use_client_side_filtering = False
        created_after_dt = None
        created_before_dt = None
        finished_after_dt = None
        finished_before_dt = None
        
        # Handle days_back (convenience parameter)
        if days_back is not None:
            date_threshold = datetime.now(timezone.utc) - timedelta(days=days_back)
            created_after = date_threshold.isoformat().replace('+00:00', 'Z')
            created_after_dt = date_threshold.replace(tzinfo=None)
            created_before_dt = datetime.now(timezone.utc).replace(tzinfo=None)
            use_client_side_filtering = True
            logger.info(f"Fetching runs for environment {environment_id} (last {days_back} days)")
        
        # Parse date strings to datetime objects for client-side filtering
        if created_after:
            try:
                created_after_dt = datetime.fromisoformat(created_after.replace('Z', '+00:00')).replace(tzinfo=None)
                use_client_side_filtering = True
            except:
                pass
        
        if created_before:
            try:
                created_before_dt = datetime.fromisoformat(created_before.replace('Z', '+00:00')).replace(tzinfo=None)
                use_client_side_filtering = True
            except:
                pass
        
        if finished_after:
            try:
                finished_after_dt = datetime.fromisoformat(finished_after.replace('Z', '+00:00')).replace(tzinfo=None)
                use_client_side_filtering = True
            except:
                pass
        
        if finished_before:
            try:
                finished_before_dt = datetime.fromisoformat(finished_before.replace('Z', '+00:00')).replace(tzinfo=None)
                use_client_side_filtering = True
            except:
                pass
        
        # Make request (without date filtering - API v2 doesn't fully support it)
        response = self._make_request("GET", endpoint, params)
        runs = response.get("data", [])
        
        # Client-side filtering if date filters were specified
        if use_client_side_filtering:
            filtered_runs = []
            for run in runs:
                # Parse run dates
                run_created_at = None
                run_finished_at = None
                
                if run.get("created_at"):
                    try:
                        run_created_at = datetime.fromisoformat(run["created_at"].replace('Z', '+00:00')).replace(tzinfo=None)
                    except:
                        pass
                
                if run.get("finished_at"):
                    try:
                        run_finished_at = datetime.fromisoformat(run["finished_at"].replace('Z', '+00:00')).replace(tzinfo=None)
                    except:
                        pass
                
                # Apply filters
                passes_filters = True
                
                if created_after_dt and run_created_at:
                    if run_created_at < created_after_dt:
                        passes_filters = False
                
                if created_before_dt and run_created_at:
                    if run_created_at > created_before_dt:
                        passes_filters = False
                
                if finished_after_dt and run_finished_at:
                    if run_finished_at < finished_after_dt:
                        passes_filters = False
                
                if finished_before_dt and run_finished_at:
                    if run_finished_at > finished_before_dt:
                        passes_filters = False
                
                if passes_filters:
                    filtered_runs.append(run)
            
            runs = filtered_runs
            logger.info(f"Client-side filtered to {len(runs)} runs")
        else:
            logger.info(f"Found {len(runs)} runs")
        
        return runs
    
    def get_run_details(self, run_id: int, include_related: Optional[List[str]] = None) -> Dict:
        """
        Retrieve detailed information for a specific run
        
        Args:
            run_id: Run ID
            include_related: List of related resources to include (e.g., ["debug_logs"])
            
        Returns:
            Run details dictionary
        """
        logger.info(f"Fetching details for run {run_id}")
        endpoint = f"accounts/{self.account_id}/runs/{run_id}/"
        params = None
        if include_related:
            # API expects a comma-separated string
            params = {"include_related": ",".join(include_related)}
        response = self._make_request("GET", endpoint, params)
        return response.get("data", {})


class dbtLogRetriever:
    """Main class for retrieving dbt logs"""
    
    def __init__(self, client: dbtCloudClient, output_dir: str = "dbt_logs"):
        """
        Initialize the log retriever
        
        Args:
            client: dbtCloudClient instance
            output_dir: Directory to save logs
        """
        self.client = client
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
    
    def retrieve_logs(
        self, 
        deployment_types: Optional[List[str]] = None,
        env_names: Optional[List[str]] = None,
        env_ids: Optional[List[int]] = None,
        days_back: Optional[int] = None,
        created_after: Optional[str] = None,
        created_before: Optional[str] = None,
        finished_after: Optional[str] = None,
        finished_before: Optional[str] = None,
        save_details: bool = True, 
        write_logs: bool = False, 
        use_debug_logs: bool = False, 
        concurrency: int = 4,
        limit: int = 100
    ):
        """
        Main method to retrieve all logs
        
        Args:
            deployment_types: List of deployment types to filter by (default: None, uses all types)
            env_names: List of environment names to filter by (default: None)
            env_ids: List of environment IDs to filter by (default: None)
            days_back: Number of days to look back for runs (default: None)
            created_after: ISO 8601 datetime for created_at start range (e.g., "2024-01-01T00:00:00Z")
            created_before: ISO 8601 datetime for created_at end range
            finished_after: ISO 8601 datetime for finished_at start range
            finished_before: ISO 8601 datetime for finished_at end range
            save_details: Whether to save run details JSON
            write_logs: Whether to write combined logs from steps
            use_debug_logs: Whether to use debug logs instead of regular logs
            concurrency: Number of concurrent runs to process per environment
            limit: Maximum number of runs to retrieve per environment (default: 100)
        """
        logger.info("=" * 80)
        logger.info("Starting dbt log retrieval process")
        logger.info("=" * 80)
        
        # Step 1: Get all environments
        environments = self.client.get_environments()
        
        # Step 2: Filter environments
        filtered_envs = self.client.filter_environments(
            environments, 
            deployment_types=deployment_types,
            env_names=env_names,
            env_ids=env_ids
        )
        
        if not filtered_envs:
            logger.warning("No environments found matching the specified filters")
            return
        
        # Step 3 & 4: For each environment, get runs and retrieve logs
        total_runs = 0
        total_logs_retrieved = 0
        
        for env in filtered_envs:
            env_id = env.get("id")
            env_name = env.get("name", f"env_{env_id}")
            deployment_type = env.get("deployment_type")
            
            logger.info("-" * 80)
            logger.info(f"Processing environment: {env_name} (ID: {env_id}, Type: {deployment_type})")
            logger.info("-" * 80)
            
            # Get runs for this environment (using server-side filtering)
            runs = self.client.list_runs(
                env_id, 
                days_back=days_back,
                created_after=created_after,
                created_before=created_before,
                finished_after=finished_after,
                finished_before=finished_before,
                limit=limit
            )
            total_runs += len(runs)
            
            if not runs:
                logger.info(f"No runs found for environment {env_name}")
                continue
            
            # Create directory for this environment
            env_dir = self.output_dir / f"{env_name}_{env_id}"
            env_dir.mkdir(exist_ok=True)
            
            # Process runs concurrently per environment
            def process_run(run_obj: Dict) -> int:
                run_id_local = run_obj.get("id")
                run_status_local = run_obj.get("status_humanized", "unknown")
                created_at_local = run_obj.get("created_at", "unknown")
                logger.info(f"Processing run {run_id_local} (Status: {run_status_local}, Created: {created_at_local})")

                run_details_local = self.client.get_run_details(run_id_local, include_related=["run_steps"])  

                if save_details:
                    details_file_local = env_dir / f"run_{run_id_local}_details.json"
                    with open(details_file_local, 'w') as f:
                        json.dump(run_details_local, f, indent=2)
                    logger.info(f"Saved run details to {details_file_local}")

                if write_logs:
                    combined_lines_local: List[str] = []
                    run_steps_local = run_details_local.get("run_steps") or []
                    try:
                        run_steps_local = sorted(run_steps_local, key=lambda s: s.get("index", 0))
                    except Exception:
                        pass
                    for step in run_steps_local:
                        step_logs = step.get("debug_logs") if use_debug_logs else step.get("logs")
                        if not step_logs and use_debug_logs:
                            step_logs = step.get("truncated_debug_logs")
                        if not step_logs:
                            step_logs = step.get("truncated_debug_logs") or step.get("debug_logs") or ""
                        if not step_logs:
                            continue
                        if not step_logs.endswith("\n"):
                            step_logs = f"{step_logs}\n"
                        combined_lines_local.append(step_logs)
                    if combined_lines_local:
                        clean_log_path_local = env_dir / f"run_{run_id_local}_logs.txt"
                        with open(clean_log_path_local, 'w') as f:
                            f.writelines(combined_lines_local)
                        logger.info(f"Saved combined run logs to {clean_log_path_local}")
                        return 1
                    return 0
                # Not writing logs
                return 0

            if runs:
                with ThreadPoolExecutor(max_workers=max(1, concurrency)) as executor:
                    futures = [executor.submit(process_run, r) for r in runs]
                    for fut in as_completed(futures):
                        try:
                            total_logs_retrieved += int(fut.result() or 0)
                        except Exception as e:
                            logger.warning(f"Run processing failed: {e}")
        
        # Summary
        logger.info("=" * 80)
        logger.info("Log retrieval complete!")
        logger.info(f"Processed {len(filtered_envs)} environments")
        logger.info(f"Found {total_runs} total runs")
        logger.info(f"Retrieved {total_logs_retrieved} log files")
        logger.info(f"Logs saved to: {self.output_dir.absolute()}")
        logger.info("=" * 80)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="dbt Cloud log retriever with server-side filtering")
    
    # Connection options
    parser.add_argument("--base-url", dest="base_url", help="Full dbt Cloud API base URL (e.g., https://emea.dbt.com/api/v2)")
    parser.add_argument("--host", dest="host", help="dbt Cloud host domain (e.g., emea.dbt.com)")
    
    # Environment filters
    parser.add_argument("--deployment-types", dest="deployment_types", default="", help="Comma-separated deployment types (e.g., staging,production). If not specified, all types are included.")
    parser.add_argument("--env-names", dest="env_names", default="", help="Comma-separated environment names to filter by (exact match)")
    parser.add_argument("--env-ids", dest="env_ids", default="", help="Comma-separated environment IDs to filter by")
    
    # Run date filters (server-side filtering)
    parser.add_argument("--days-back", dest="days_back", type=int, default=None, help="Days back to fetch runs (convenience option, overrides created-after)")
    parser.add_argument("--created-after", dest="created_after", help="Filter runs created after this datetime (ISO 8601: 2024-01-01T00:00:00Z)")
    parser.add_argument("--created-before", dest="created_before", help="Filter runs created before this datetime (ISO 8601: 2024-12-31T23:59:59Z)")
    parser.add_argument("--finished-after", dest="finished_after", help="Filter runs finished after this datetime (ISO 8601)")
    parser.add_argument("--finished-before", dest="finished_before", help="Filter runs finished before this datetime (ISO 8601)")
    parser.add_argument("--limit", dest="limit", type=int, default=100, help="Maximum number of runs to retrieve per environment (default: 100)")
    
    # Output options
    parser.add_argument("--output-dir", dest="output_dir", default="dbt_logs", help="Directory to save logs (default: dbt_logs)")
    parser.add_argument("--no-save-details", dest="save_details", action="store_false", help="Do not save full run detail JSON (default is to save)")
    parser.set_defaults(save_details=True)
    parser.add_argument("--write-logs", dest="write_logs", action="store_true", help="Write combined run logs from step logs (default off)")
    parser.add_argument("--use-debug-logs", dest="use_debug_logs", action="store_true", help="Use debug_logs instead of logs for combined output when --write-logs is set")
    
    # Performance options
    parser.add_argument("--concurrency", dest="concurrency", type=int, default=4, help="Concurrent runs to process per environment (default: 4)")
    
    return parser.parse_args()


def main():
    """Main entry point"""
    args = parse_args()
    # Get credentials from environment variables
    api_token = os.getenv("DBT_CLOUD_API_TOKEN")
    account_id = os.getenv("DBT_CLOUD_ACCOUNT_ID")
    base_url_env = args.base_url or os.getenv("DBT_CLOUD_BASE_URL")
    host_env = args.host or os.getenv("DBT_CLOUD_HOST")
    
    if not api_token:
        logger.error("DBT_CLOUD_API_TOKEN environment variable not set")
        sys.exit(1)
    
    if not account_id:
        logger.error("DBT_CLOUD_ACCOUNT_ID environment variable not set")
        sys.exit(1)
    
    # Determine base URL (support regional hosts)
    # Priority: DBT_CLOUD_BASE_URL (full URL) > dbt_CLOUD_HOST (domain only) > default
    if base_url_env:
        base_url = base_url_env.rstrip("/")
    elif host_env:
        # Normalize host to a full URL with scheme and API path
        host = host_env.strip()
        if host.startswith("http://") or host.startswith("https://"):
            normalized = host
        else:
            normalized = f"https://{host}"
        base_url = f"{normalized.rstrip('/')}/api/v2"
    else:
        base_url = "https://cloud.getdbt.com/api/v2"
    
    logger.info(f"Using dbt Cloud API base URL: {base_url}")
    
    # Initialize client
    client = dbtCloudClient(api_token=api_token, account_id=account_id, base_url=base_url)
    
    # Initialize retriever
    retriever = dbtLogRetriever(client=client, output_dir=args.output_dir)
    
    # Parse filter arguments
    deployment_types = [t.strip() for t in args.deployment_types.split(",") if t.strip()] if args.deployment_types else None
    env_names = [n.strip() for n in args.env_names.split(",") if n.strip()] if args.env_names else None
    env_ids = [int(i.strip()) for i in args.env_ids.split(",") if i.strip()] if args.env_ids else None
    
    # Retrieve logs with server-side filtering
    retriever.retrieve_logs(
        deployment_types=deployment_types,
        env_names=env_names,
        env_ids=env_ids,
        days_back=args.days_back,
        created_after=args.created_after,
        created_before=args.created_before,
        finished_after=args.finished_after,
        finished_before=args.finished_before,
        save_details=args.save_details,
        write_logs=args.write_logs,
        use_debug_logs=args.use_debug_logs,
        concurrency=args.concurrency,
        limit=args.limit,
    )


if __name__ == "__main__":
    main()
