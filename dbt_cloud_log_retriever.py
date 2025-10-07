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
    
    def list_runs(self, environment_id: int, days_back: int = 5) -> List[Dict]:
        """
        List runs for a specific environment within the last N days
        
        Args:
            environment_id: Environment ID
            days_back: Number of days to look back (default: 5)
            
        Returns:
            List of run dictionaries
        """
        logger.info(f"Fetching runs for environment {environment_id} (last {days_back} days)")
        endpoint = f"accounts/{self.account_id}/runs/"
        
        # Calculate the date threshold (timezone-aware UTC)
        date_threshold = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(days=days_back)
        
        # API parameters
        params = {
            "environment_id": environment_id,
            "order_by": "-created_at",  # Most recent first
            "limit": 100  # Adjust as needed
        }
        
        response = self._make_request("GET", endpoint, params)
        all_runs = response.get("data", [])
        
        # Filter runs by date
        filtered_runs = []
        for run in all_runs:
            created_at_str = run.get("created_at")
            if created_at_str:
                # Parse the datetime string
                created_at = datetime.fromisoformat(created_at_str.replace('Z', '+00:00'))
                if created_at.replace(tzinfo=None) >= date_threshold:
                    filtered_runs.append(run)
        
        logger.info(f"Found {len(filtered_runs)} runs in the last {days_back} days")
        return filtered_runs
    
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
        days_back: int = 5, 
        save_details: bool = True, 
        write_logs: bool = False, 
        use_debug_logs: bool = False, 
        concurrency: int = 4
    ):
        """
        Main method to retrieve all logs
        
        Args:
            deployment_types: List of deployment types to filter by (default: None, uses all types)
            env_names: List of environment names to filter by (default: None)
            env_ids: List of environment IDs to filter by (default: None)
            days_back: Number of days to look back for runs
            save_details: Whether to save run details JSON
            write_logs: Whether to write combined logs from steps
            use_debug_logs: Whether to use debug logs instead of regular logs
            concurrency: Number of concurrent runs to process per environment
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
            
            # Get runs for this environment
            runs = self.client.list_runs(env_id, days_back)
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
    parser = argparse.ArgumentParser(description="dbt Cloud log retriever")
    parser.add_argument("--base-url", dest="base_url", help="Full dbt Cloud API base URL (e.g., https://emea.dbt.com/api/v2)")
    parser.add_argument("--host", dest="host", help="dbt Cloud host domain (e.g., emea.dbt.com)")
    parser.add_argument("--days-back", dest="days_back", type=int, default=5, help="Days back to fetch runs (default: 5)")
    parser.add_argument("--deployment-types", dest="deployment_types", default="staging,production", help="Comma-separated deployment types (e.g., staging,production). If not specified, all types are included.")
    parser.add_argument("--env-names", dest="env_names", default="", help="Comma-separated environment names to filter by (exact match)")
    parser.add_argument("--env-ids", dest="env_ids", default="", help="Comma-separated environment IDs to filter by")
    parser.add_argument("--output-dir", dest="output_dir", default="dbt_logs", help="Directory to save logs (default: dbt_logs)")
    parser.add_argument("--no-save-details", dest="save_details", action="store_false", help="Do not save full run detail JSON (default is to save)")
    parser.set_defaults(save_details=True)
    parser.add_argument("--write-logs", dest="write_logs", action="store_true", help="Write combined run logs from step logs (default off)")
    parser.add_argument("--use-debug-logs", dest="use_debug_logs", action="store_true", help="Use debug_logs instead of logs for combined output when --write-logs is set")
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
    
    # Retrieve logs
    retriever.retrieve_logs(
        deployment_types=deployment_types,
        env_names=env_names,
        env_ids=env_ids,
        days_back=args.days_back,
        save_details=args.save_details,
        write_logs=args.write_logs,
        use_debug_logs=args.use_debug_logs,
        concurrency=args.concurrency,
    )


if __name__ == "__main__":
    main()
