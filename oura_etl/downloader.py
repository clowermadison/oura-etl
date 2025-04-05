"""Downloader module for fetching data from the Oura API.

This module handles all interactions with the Oura API, including authentication,
rate limiting, and data extraction for different endpoints.
"""

import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import logging
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

class OuraDownloader:
    """Class to handle downloading data from the Oura API."""
    
    BASE_URL = "https://api.ouraring.com/v2"
    ENDPOINTS = {
        "daily_sleep": "/usercollection/daily_sleep",
        "sleep": "/usercollection/sleep",
        "daily_activity": "/usercollection/daily_activity",
        "daily_readiness": "/usercollection/daily_readiness",
        "heart_rate": "/usercollection/heartrate",
        "daily_hrv": "/usercollection/daily_hrv",
    }
    
    def __init__(self, access_token: str, rate_limit_per_minute: int = 60):
        """Initialize the downloader with authentication and rate limiting.
        
        Args:
            access_token: Oura API personal access token
            rate_limit_per_minute: Maximum number of API calls per minute (default: 60)
        """
        self.access_token = access_token
        self.rate_limit = rate_limit_per_minute
        self.last_request_time = 0
        
        # Set up session with retry logic
        self.session = requests.Session()
        retry_strategy = Retry(
            total=3,  # number of retries
            backoff_factor=1,  # wait 1, 2, 4 seconds between retries
            status_forcelist=[429, 500, 502, 503, 504]  # HTTP status codes to retry on
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("https://", adapter)
        
        # Set up authentication headers
        self.headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
    
    def _rate_limit_wait(self):
        """Implement rate limiting by waiting if necessary."""
        current_time = time.time()
        elapsed = current_time - self.last_request_time
        wait_time = (60.0 / self.rate_limit) - elapsed
        
        if wait_time > 0:
            time.sleep(wait_time)
        
        self.last_request_time = time.time()
    
    def _make_request(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Make a rate-limited request to the Oura API.
        
        Args:
            endpoint: API endpoint to call
            params: Optional query parameters
            
        Returns:
            JSON response from the API
            
        Raises:
            requests.exceptions.RequestException: If the API request fails
        """
        self._rate_limit_wait()
        
        url = f"{self.BASE_URL}{endpoint}"
        try:
            response = self.session.get(url, headers=self.headers, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Error making request to {url}: {str(e)}")
            raise
    
    def get_sleep_data(self, start_date: str, end_date: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get detailed sleep data for a date range.
        
        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: Optional end date in YYYY-MM-DD format (defaults to today)
            
        Returns:
            List of sleep records
        """
        params = {"start_date": start_date}
        if end_date:
            params["end_date"] = end_date
            
        response = self._make_request(self.ENDPOINTS["sleep"], params)
        return response.get("data", [])
    
    def get_daily_activity(self, start_date: str, end_date: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get daily activity data for a date range.
        
        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: Optional end date in YYYY-MM-DD format (defaults to today)
            
        Returns:
            List of daily activity records
        """
        params = {"start_date": start_date}
        if end_date:
            params["end_date"] = end_date
            
        response = self._make_request(self.ENDPOINTS["daily_activity"], params)
        return response.get("data", [])
    
    def get_readiness_data(self, start_date: str, end_date: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get daily readiness data for a date range.
        
        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: Optional end date in YYYY-MM-DD format (defaults to today)
            
        Returns:
            List of readiness records
        """
        params = {"start_date": start_date}
        if end_date:
            params["end_date"] = end_date
            
        response = self._make_request(self.ENDPOINTS["daily_readiness"], params)
        return response.get("data", [])
    
    def get_heart_rate(self, start_date: str, end_date: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get heart rate data for a date range.
        
        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: Optional end date in YYYY-MM-DD format (defaults to today)
            
        Returns:
            List of heart rate measurements
        """
        params = {"start_date": start_date}
        if end_date:
            params["end_date"] = end_date
            
        response = self._make_request(self.ENDPOINTS["heart_rate"], params)
        return response.get("data", [])
    
    def get_hrv_data(self, start_date: str, end_date: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get HRV data for a date range.
        
        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: Optional end date in YYYY-MM-DD format (defaults to today)
            
        Returns:
            List of HRV measurements
        """
        params = {"start_date": start_date}
        if end_date:
            params["end_date"] = end_date
            
        response = self._make_request(self.ENDPOINTS["daily_hrv"], params)
        return response.get("data", [])
