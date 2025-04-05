"""Database module for Oura ETL pipeline.

This module handles all database operations including initialization, connections,
and basic CRUD operations for Oura Ring data.
"""

import sqlite3
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any

class OuraDatabase:
    def __init__(self, db_path: str = "data/oura.db"):
        """Initialize database connection.
        
        Args:
            db_path: Path to SQLite database file
        """
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        
    @contextmanager
    def get_connection(self):
        """Context manager for database connections."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row  # Enable row factory for dict-like access
        try:
            yield conn
        finally:
            conn.close()

    def initialize_tables(self):
        """Create all necessary tables if they don't exist."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            # Sleep data table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS sleep (
                    id TEXT PRIMARY KEY,
                    day TEXT NOT NULL,
                    bedtime_start DATETIME,
                    bedtime_end DATETIME,
                    sleep_score INTEGER,
                    total_sleep_duration INTEGER,
                    deep_sleep_duration INTEGER,
                    rem_sleep_duration INTEGER,
                    light_sleep_duration INTEGER,
                    time_in_bed INTEGER,
                    restfulness_score INTEGER,
                    efficiency INTEGER,
                    latency INTEGER,
                    hr_lowest INTEGER,
                    hr_average REAL,
                    temperature_delta REAL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Daily Activity table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS daily_activity (
                    id TEXT PRIMARY KEY,
                    day TEXT NOT NULL,
                    activity_score INTEGER,
                    target_calories INTEGER,
                    total_calories INTEGER,
                    steps INTEGER,
                    daily_movement INTEGER,
                    inactive_time INTEGER,
                    rest_time INTEGER,
                    low_activity_time INTEGER,
                    medium_activity_time INTEGER,
                    high_activity_time INTEGER,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Readiness table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS readiness (
                    id TEXT PRIMARY KEY,
                    day TEXT NOT NULL,
                    score INTEGER,
                    temperature_deviation REAL,
                    previous_night_score INTEGER,
                    sleep_balance_score INTEGER,
                    previous_day_activity_score INTEGER,
                    activity_balance_score INTEGER,
                    resting_heart_rate INTEGER,
                    hrv_balance_score INTEGER,
                    recovery_index_score INTEGER,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Heart Rate table (for detailed HR data)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS heart_rate (
                    id TEXT PRIMARY KEY,
                    timestamp DATETIME NOT NULL,
                    bpm INTEGER,
                    source TEXT,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # HRV table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS hrv (
                    id TEXT PRIMARY KEY,
                    timestamp DATETIME NOT NULL,
                    rmssd INTEGER,
                    source TEXT,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)

            conn.commit()
    
    def insert_sleep_data(self, data: Dict[str, Any]):
        """Insert sleep data into the database."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT OR REPLACE INTO sleep (
                    id, day, bedtime_start, bedtime_end, sleep_score,
                    total_sleep_duration, deep_sleep_duration, rem_sleep_duration,
                    light_sleep_duration, time_in_bed, restfulness_score,
                    efficiency, latency, hr_lowest, hr_average, temperature_delta
                ) VALUES (
                    :id, :day, :bedtime_start, :bedtime_end, :sleep_score,
                    :total_sleep_duration, :deep_sleep_duration, :rem_sleep_duration,
                    :light_sleep_duration, :time_in_bed, :restfulness_score,
                    :efficiency, :latency, :hr_lowest, :hr_average, :temperature_delta
                )
            """, data)
            conn.commit()

    def insert_daily_activity(self, data: Dict[str, Any]):
        """Insert daily activity data into the database."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT OR REPLACE INTO daily_activity (
                    id, day, activity_score, target_calories, total_calories,
                    steps, daily_movement, inactive_time, rest_time,
                    low_activity_time, medium_activity_time, high_activity_time
                ) VALUES (
                    :id, :day, :activity_score, :target_calories, :total_calories,
                    :steps, :daily_movement, :inactive_time, :rest_time,
                    :low_activity_time, :medium_activity_time, :high_activity_time
                )
            """, data)
            conn.commit()

    def insert_readiness(self, data: Dict[str, Any]):
        """Insert readiness data into the database."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT OR REPLACE INTO readiness (
                    id, day, score, temperature_deviation, previous_night_score,
                    sleep_balance_score, previous_day_activity_score,
                    activity_balance_score, resting_heart_rate, hrv_balance_score,
                    recovery_index_score
                ) VALUES (
                    :id, :day, :score, :temperature_deviation, :previous_night_score,
                    :sleep_balance_score, :previous_day_activity_score,
                    :activity_balance_score, :resting_heart_rate, :hrv_balance_score,
                    :recovery_index_score
                )
            """, data)
            conn.commit()

    def insert_heart_rate(self, data: Dict[str, Any]):
        """Insert heart rate data into the database."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT OR REPLACE INTO heart_rate (
                    id, timestamp, bpm, source
                ) VALUES (
                    :id, :timestamp, :bpm, :source
                )
            """, data)
            conn.commit()

    def insert_hrv(self, data: Dict[str, Any]):
        """Insert HRV data into the database."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT OR REPLACE INTO hrv (
                    id, timestamp, rmssd, source
                ) VALUES (
                    :id, :timestamp, :rmssd, :source
                )
            """, data)
            conn.commit() 