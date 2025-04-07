"""Downloader module for fetching data from the Oura API.

This module handles all interactions with the Oura API, including authentication,
rate limiting, and data extraction for different endpoints.
"""

import time
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Union
import logging
from pathlib import Path
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from oura_etl.utils.load_config import get_api_config
from oura_etl.config import OuraAPIConfig

logger = logging.getLogger(__name__)

class OuraDownloader:
    """Download data from the Oura API."""

    def __init__(self, access_token: str, output_dir: str):
        """Initialize the downloader with API token and output directory."""
        self.access_token = access_token
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json'
        }
        self.base_url = 'https://api.ouraring.com/v2/usercollection'

    def _make_request(self, endpoint: str, params: Dict[str, Any] = None) -> Optional[Dict[str, Any]]:
        """Make a request to the Oura API."""
        try:
            url = f"{self.base_url}/{endpoint}"
            response = requests.get(url, headers=self.headers, params=params)
            response.raise_for_status()
            # sleep to avoid rate limiting
            time.sleep(1)
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching {endpoint}: {str(e)}")
            return None

    def download_data(self, data_type: str, start_date: str, end_date: str) -> Optional[Path]:
        """Download data for a specific type and date range."""
        params = {
            'start_date': start_date,
            'end_date': end_date
        }

        # Special handling for heart rate data which uses different parameters
        if data_type == 'heart_rate':
            params = {
                'start_datetime': f"{start_date}T00:00:00Z",
                'end_datetime': f"{end_date}T23:59:59Z"
            }

        # Special handling for personal info which doesn't use date parameters
        if data_type == 'personal_info':
            params = {}

        # Special handling for ring configuration which doesn't use date parameters
        if data_type == 'ring_configuration':
            params = {}

        # Special handling for rest mode period which uses different parameters
        if data_type == 'rest_mode_period':
            params = {
                'start_time': f"{start_date}T00:00:00Z",
                'end_time': f"{end_date}T23:59:59Z"
            }

        data = self._make_request(data_type, params)
        if data is None:
            return None

        # Save to file with timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = self.output_dir / f"{data_type}_{timestamp}.json"
        
        with open(output_file, 'w') as f:
            json.dump(data, f, indent=2)
        
        logger.info(f"Downloaded {data_type} data to {output_file}")
        return output_file

    def download_recent_data(self, data_type: str, days: int = 7) -> Optional[Path]:
        """Download recent data for a specific type."""
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        return self.download_data(data_type, start_date, end_date)

    def download_all_data_types(self, start_date: str, end_date: str) -> List[Path]:
        """Download all available data types."""
        data_types = [
            'daily_activity',
            'daily_readiness',
            'daily_sleep',
            # 'sleep',
            # 'heart_rate',
            # 'personal_info',
            # 'sleep_time',
            # 'daily_hrv',
            # 'daily_spo2',
            # 'daily_stress',
            # 'daily_resilience',
            # 'daily_cardiovascular_age',
            # 'vo2_max',
            # 'workout',
            # 'session',
            # 'tag',
            # 'enhanced_tag',
            # 'rest_mode_period',
            # 'ring_configuration'
        ]

        downloaded_files = []
        for data_type in data_types:
            file = self.download_data(data_type, start_date, end_date)
            if file:
                downloaded_files.append(file)

        return downloaded_files
