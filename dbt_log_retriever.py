#!/usr/bin/env python3
"""
DBT Cloud Log Retriever

This script retrieves dbt Cloud logs by:
1. Fetching all environments
2. Filtering for staging/production deployment types
3. Listing runs from the last 5 days for each environment
4. Retrieving run and debug logs for each run
"""

import os
import sys
import requests
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import json
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DBTCloudClient:
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
            response = requests.request(
                method=method,
                url=url,
                headers=self.headers,
                params=params,
                timeout=30
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
    
    def filter_environments(self, environments: List[Dict], deployment_types: List[str]) -> List[Dict]:
        """
        Filter environments by deployment type
        
        Args:
            environments: List of environment dictionaries
            deployment_types: List of deployment types to filter by (e.g., ['staging', 'production'])
            
        Returns:
            Filtered list of environments
        """
        filtered = [
            env for env in environments 
            if env.get("deployment_type") in deployment_types
        ]
        logger.info(f"Filtered to {len(filtered)} environments with deployment types: {deployment_types}")
        
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
        
        # Calculate the date threshold
        date_threshold = datetime.utcnow() - timedelta(days=days_back)
        
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
    
    def get_run_artifact(self, run_id: int, artifact_type: str) -> Optional[str]:
        """
        Retrieve run artifacts (logs)
        
        Args:
            run_id: Run ID
            artifact_type: Type of artifact (e.g., 'run_logs.txt', 'debug_logs.txt')
            
        Returns:
            Artifact content as string, or None if not available
        """
        logger.info(f"Fetching {artifact_type} for run {run_id}")
        endpoint = f"accounts/{self.account_id}/runs/{run_id}/artifacts/{artifact_type}"
        
        try:
            url = f"{self.base_url}/{endpoint}"
            response = requests.get(
                url,
                headers=self.headers,
                timeout=30
            )
            response.raise_for_status()
            return response.text
        except requests.exceptions.RequestException as e:
            logger.warning(f"Could not retrieve {artifact_type} for run {run_id}: {e}")
            return None


class DBTLogRetriever:
    """Main class for retrieving dbt logs"""
    
    def __init__(self, client: DBTCloudClient, output_dir: str = "dbt_logs"):
        """
        Initialize the log retriever
        
        Args:
            client: DBTCloudClient instance
            output_dir: Directory to save logs
        """
        self.client = client
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
    
    def retrieve_logs(self, deployment_types: List[str] = ["staging", "production"], days_back: int = 5):
        """
        Main method to retrieve all logs
        
        Args:
            deployment_types: List of deployment types to filter by
            days_back: Number of days to look back for runs
        """
        logger.info("=" * 80)
        logger.info("Starting dbt log retrieval process")
        logger.info("=" * 80)
        
        # Step 1: Get all environments
        environments = self.client.get_environments()
        
        # Step 2: Filter environments
        filtered_envs = self.client.filter_environments(environments, deployment_types)
        
        if not filtered_envs:
            logger.warning("No environments found matching the specified deployment types")
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
            
            # Process each run
            for run in runs:
                run_id = run.get("id")
                run_status = run.get("status_humanized", "unknown")
                created_at = run.get("created_at", "unknown")
                
                logger.info(f"Processing run {run_id} (Status: {run_status}, Created: {created_at})")
                
                # Get run details, including step-level logs
                run_details = self.client.get_run_details(run_id, include_related=["run_steps"])
                
                # Save run details
                details_file = env_dir / f"run_{run_id}_details.json"
                with open(details_file, 'w') as f:
                    json.dump(run_details, f, indent=2)
                logger.info(f"Saved run details to {details_file}")
                
                # Build a clean, native-like log by concatenating step logs in order
                combined_lines: List[str] = []
                run_steps = run_details.get("run_steps") or []
                # Sort by index to ensure correct order
                try:
                    run_steps = sorted(run_steps, key=lambda s: s.get("index", 0))
                except Exception:
                    # If sorting fails, keep original order
                    pass
                for step in run_steps:
                    step_logs = step.get("logs") or ""
                    if not step_logs:
                        # Fallback to truncated_debug_logs or debug_logs if logs missing
                        step_logs = step.get("truncated_debug_logs") or step.get("debug_logs") or ""
                    if not step_logs:
                        continue
                    # Normalize to str and ensure newline termination
                    if not step_logs.endswith("\n"):
                        step_logs = f"{step_logs}\n"
                    combined_lines.append(step_logs)
                if combined_lines:
                    clean_log_path = env_dir / f"run_{run_id}_logs.txt"
                    with open(clean_log_path, 'w') as f:
                        f.writelines(combined_lines)
                    logger.info(f"Saved combined run logs to {clean_log_path}")
                    total_logs_retrieved += 1
        
        # Summary
        logger.info("=" * 80)
        logger.info("Log retrieval complete!")
        logger.info(f"Processed {len(filtered_envs)} environments")
        logger.info(f"Found {total_runs} total runs")
        logger.info(f"Retrieved {total_logs_retrieved} log files")
        logger.info(f"Logs saved to: {self.output_dir.absolute()}")
        logger.info("=" * 80)


def main():
    """Main entry point"""
    # Get credentials from environment variables
    api_token = os.getenv("DBT_CLOUD_API_TOKEN")
    account_id = os.getenv("DBT_CLOUD_ACCOUNT_ID")
    base_url_env = os.getenv("DBT_CLOUD_BASE_URL")
    host_env = os.getenv("DBT_CLOUD_HOST")
    
    if not api_token:
        logger.error("DBT_CLOUD_API_TOKEN environment variable not set")
        sys.exit(1)
    
    if not account_id:
        logger.error("DBT_CLOUD_ACCOUNT_ID environment variable not set")
        sys.exit(1)
    
    # Determine base URL (support regional hosts)
    # Priority: DBT_CLOUD_BASE_URL (full URL) > DBT_CLOUD_HOST (domain only) > default
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
    client = DBTCloudClient(api_token=api_token, account_id=account_id, base_url=base_url)
    
    # Initialize retriever
    retriever = DBTLogRetriever(client=client, output_dir="dbt_logs")
    
    # Retrieve logs
    retriever.retrieve_logs(
        deployment_types=["staging", "production"],
        days_back=5
    )


if __name__ == "__main__":
    main()
