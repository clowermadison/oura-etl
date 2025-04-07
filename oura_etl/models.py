"""Data models for the Oura ETL pipeline.

This module defines the data structures used to represent Oura Ring data
throughout the ETL pipeline. These models correspond to both the API responses
and database schemas.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Optional, List, Dict, Any

@dataclass
class ActivityContributors:
    """Activity score contributors."""
    meet_daily_targets: Optional[int]
    move_every_hour: Optional[int]
    recovery_time: Optional[int]
    stay_active: Optional[int]
    training_frequency: Optional[int]
    training_volume: Optional[int]

@dataclass
class DailyActivity:
    """Daily activity data."""
    id: str
    day: str
    class_5_min: Optional[str]
    score: Optional[int]
    active_calories: int
    average_met_minutes: float
    contributors: ActivityContributors
    equivalent_walking_distance: int
    high_activity_met_minutes: int
    high_activity_time: int
    inactivity_alerts: int
    low_activity_met_minutes: int
    low_activity_time: int
    medium_activity_met_minutes: int
    medium_activity_time: int
    meters_to_target: int
    non_wear_time: int
    resting_time: int
    sedentary_met_minutes: int
    sedentary_time: int
    steps: int
    target_calories: int
    target_meters: int
    total_calories: int
    timestamp: str

@dataclass
class ReadinessContributors:
    """Readiness score contributors."""
    activity_balance: Optional[int]
    body_temperature: Optional[int]
    hrv_balance: Optional[int]
    previous_day_activity: Optional[int]
    previous_night: Optional[int]
    recovery_index: Optional[int]
    resting_heart_rate: Optional[int]
    sleep_balance: Optional[int]

@dataclass
class DailyReadiness:
    """Daily readiness data."""
    id: str
    contributors: ReadinessContributors
    day: str
    score: Optional[int]
    temperature_deviation: Optional[float]
    temperature_trend_deviation: Optional[float]
    timestamp: str

@dataclass
class SleepContributors:
    """Sleep score contributors."""
    deep_sleep: Optional[int]
    efficiency: Optional[int]
    latency: Optional[int]
    rem_sleep: Optional[int]
    restfulness: Optional[int]
    timing: Optional[int]
    total_sleep: Optional[int]

@dataclass
class DailySleep:
    """Daily sleep data."""
    id: str
    contributors: SleepContributors
    day: str
    score: Optional[int]
    timestamp: str

@dataclass
class Sleep:
    """Detailed sleep period data."""
    id: str
    average_breath: Optional[float]
    average_heart_rate: Optional[float]
    average_hrv: Optional[int]
    awake_time: Optional[int]
    bedtime_end: str
    bedtime_start: str
    day: str
    deep_sleep_duration: Optional[int]
    efficiency: Optional[int]
    latency: Optional[int]
    light_sleep_duration: Optional[int]
    low_battery_alert: bool
    lowest_heart_rate: Optional[int]
    movement_30_sec: Optional[str]
    period: int
    rem_sleep_duration: Optional[int]
    restless_periods: Optional[int]
    sleep_phase_5_min: Optional[str]
    sleep_score_delta: Optional[int]
    time_in_bed: int
    total_sleep_duration: Optional[int]
    type: str  # One of: deleted, sleep, long_sleep, late_nap, rest

@dataclass
class HeartRate:
    """Heart rate measurement."""
    id: str
    bpm: int
    source: str  # One of: awake, rest, sleep, session, live, workout
    timestamp: str

@dataclass
class Sample:
    """Time series sample data."""
    interval: float
    items: List[Optional[float]]
    timestamp: str

@dataclass
class PersonalInfo:
    """User personal information."""
    id: str
    age: Optional[int]
    weight: Optional[float]
    height: Optional[float]
    biological_sex: Optional[str]
    email: Optional[str] 