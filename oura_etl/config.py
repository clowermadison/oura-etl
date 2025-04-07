"""Configuration settings for the Oura ETL pipeline."""

from dataclasses import dataclass, field
from typing import Dict, List, Optional

@dataclass
class OuraAPIConfig:
    """Configuration for the Oura API."""
    access_token: Optional[str] = None
    base_url: str = "https://api.ouraring.com"
    rate_limit_per_minute: int = 60
    retry_total: int = 3
    retry_backoff_factor: float = 0.1
    retry_status_forcelist: List[int] = field(default_factory=lambda: [500, 502, 503, 504])
    
    # API endpoints
    endpoints: Dict[str, str] = field(default_factory=lambda: {
        # Core data endpoints
        "personal_info": "/v2/usercollection/personal_info",
        "daily_activity": "/v2/usercollection/daily_activity",
        "daily_readiness": "/v2/usercollection/daily_readiness",
        "daily_sleep": "/v2/usercollection/daily_sleep",
        "sleep": "/v2/usercollection/sleep",
        "sleep_time": "/v2/usercollection/sleep_time",
        "heart_rate": "/v2/usercollection/heartrate",
        "daily_hrv": "/v2/usercollection/daily_hrv",
        
        # Health metrics
        "daily_spo2": "/v2/usercollection/daily_spo2",
        "daily_stress": "/v2/usercollection/daily_stress",
        "daily_resilience": "/v2/usercollection/daily_resilience",
        "daily_cardiovascular_age": "/v2/usercollection/daily_cardiovascular_age",
        "vo2_max": "/v2/usercollection/vo2_max",
        
        # Workout and session tracking
        "workout": "/v2/usercollection/workout",
        "session": "/v2/usercollection/session",
        
        # Tags and metadata
        "tag": "/v2/usercollection/tag",
        "enhanced_tag": "/v2/usercollection/enhanced_tag",
        "rest_mode_period": "/v2/usercollection/rest_mode_period",
        "ring_configuration": "/v2/usercollection/ring_configuration"
    })
    
    def __post_init__(self):
        """Initialize default endpoints if not provided."""
        if self.endpoints is None:
            self.endpoints = {
                # Core data endpoints
                "personal_info": "/v2/usercollection/personal_info",
                "daily_activity": "/v2/usercollection/daily_activity",
                "daily_readiness": "/v2/usercollection/daily_readiness",
                "daily_sleep": "/v2/usercollection/daily_sleep",
                "sleep": "/v2/usercollection/sleep",
                "sleep_time": "/v2/usercollection/sleep_time",
                "heart_rate": "/v2/usercollection/heartrate",
                "daily_hrv": "/v2/usercollection/daily_hrv",
                
                # Health metrics
                "daily_spo2": "/v2/usercollection/daily_spo2",
                "daily_stress": "/v2/usercollection/daily_stress",
                "daily_resilience": "/v2/usercollection/daily_resilience",
                "daily_cardiovascular_age": "/v2/usercollection/daily_cardiovascular_age",
                "vo2_max": "/v2/usercollection/vO2_max",
                
                # Workout and session tracking
                "workout": "/v2/usercollection/workout",
                "session": "/v2/usercollection/session",
                
                # Tags and metadata
                "tag": "/v2/usercollection/tag",
                "enhanced_tag": "/v2/usercollection/enhanced_tag",
                "rest_mode_period": "/v2/usercollection/rest_mode_period",
                "ring_configuration": "/v2/usercollection/ring_configuration"
            }

@dataclass
class DatabaseConfig:
    """Configuration for the SQLite database."""
    db_path: Optional[str] = None  # Path must be provided via config.toml or environment variables 