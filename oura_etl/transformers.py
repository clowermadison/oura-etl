"""Data transformation utilities for the Oura ETL pipeline.

This module transforms raw JSON data from the Oura API into parquet files
using Polars for efficient data manipulation.
"""

import json
import logging
import uuid
from pathlib import Path
from typing import Dict, List, Any, Optional, Union, Tuple
from datetime import datetime

import polars as pl
import pandas as pd
import numpy as np

from oura_etl.models import (
    ActivityContributors,
    DailyActivity,
    ReadinessContributors,
    DailyReadiness,
    SleepContributors,
    DailySleep,
    Sleep,
    HeartRate,
    Sample,
    PersonalInfo
)
from oura_etl.database import MODEL_MAP

logger = logging.getLogger(__name__)

class OuraTransformer:
    """Class to handle transformation of Oura API data."""
    
    def __init__(self, processed_dir: str = "data/processed"):
        """Initialize the transformer.
        
        Args:
            processed_dir: Directory for storing processed parquet files
        """
        self.processed_dir = Path(processed_dir)
        self.processed_dir.mkdir(parents=True, exist_ok=True)
    
    def _normalize_activity_data(self, data: Dict[str, Any]) -> Tuple[Dict, List[Dict], List[Dict]]:
        """Normalize activity data into separate tables.
        
        Args:
            data: Raw activity data
            
        Returns:
            Tuple of (activity_data, contributors_data, metrics_data)
        """
        activity_data = {
            k: v for k, v in data.items() 
            if not isinstance(v, (dict, list)) and k not in ['contributors', 'met']
        }
        
        # Handle contributors
        contributors_data = None
        if 'contributors' in data:
            contributors = data['contributors']
            contributors_data = {
                'id': str(uuid.uuid4()),
                'daily_activity_id': data['id'],
                **contributors
            }
        
        # Handle metrics
        metrics_data = []
        if 'met' in data and isinstance(data['met'], dict):
            met = data['met']
            timestamp = met.get('timestamp')
            interval = met.get('interval')
            items = met.get('items', [])
            
            if isinstance(items, np.ndarray):
                items = items.tolist()
            
            for i, value in enumerate(items):
                if value is not None and not np.isnan(value):
                    metrics_data.append({
                        'id': str(uuid.uuid4()),
                        'daily_activity_id': data['id'],
                        'timestamp': timestamp,
                        'interval': interval,
                        'value': float(value)
                    })
        
        return activity_data, contributors_data, metrics_data
    
    def _normalize_sleep_data(self, data: Dict[str, Any]) -> Tuple[Dict, List[Dict], List[Dict]]:
        """Normalize sleep data into separate tables.
        
        Args:
            data: Raw sleep data
            
        Returns:
            Tuple of (sleep_data, heart_rate_samples, hrv_samples)
        """
        sleep_data = {
            k: v for k, v in data.items() 
            if not isinstance(v, (dict, list)) and k not in ['heart_rate', 'hrv']
        }
        
        # Handle heart rate samples
        heart_rate_samples = []
        if 'heart_rate' in data and isinstance(data['heart_rate'], dict):
            hr = data['heart_rate']
            timestamp = hr.get('timestamp')
            interval = hr.get('interval')
            items = hr.get('items', [])
            
            if isinstance(items, np.ndarray):
                items = items.tolist()
            
            for i, value in enumerate(items):
                if value is not None and not np.isnan(value):
                    heart_rate_samples.append({
                        'id': str(uuid.uuid4()),
                        'sleep_id': data['id'],
                        'timestamp': timestamp,
                        'interval': interval,
                        'value': float(value)
                    })
        
        # Handle HRV samples
        hrv_samples = []
        if 'hrv' in data and isinstance(data['hrv'], dict):
            hrv = data['hrv']
            timestamp = hrv.get('timestamp')
            interval = hrv.get('interval')
            items = hrv.get('items', [])
            
            if isinstance(items, np.ndarray):
                items = items.tolist()
            
            for i, value in enumerate(items):
                if value is not None and not np.isnan(value):
                    hrv_samples.append({
                        'id': str(uuid.uuid4()),
                        'sleep_id': data['id'],
                        'timestamp': timestamp,
                        'interval': interval,
                        'value': float(value)
                    })
        
        return sleep_data, heart_rate_samples, hrv_samples
    
    def _normalize_readiness_data(self, data: Dict[str, Any]) -> Tuple[Dict, Dict]:
        """Normalize readiness data into separate tables.
        
        Args:
            data: Raw readiness data
            
        Returns:
            Tuple of (readiness_data, contributors_data)
        """
        readiness_data = {
            k: v for k, v in data.items() 
            if not isinstance(v, (dict, list)) and k != 'contributors'
        }
        
        # Handle contributors
        contributors_data = None
        if 'contributors' in data:
            contributors = data['contributors']
            contributors_data = {
                'id': str(uuid.uuid4()),
                'daily_readiness_id': data['id'],
                **contributors
            }
        
        return readiness_data, contributors_data

    def _normalize_spo2_data(self, data: Dict[str, Any]) -> Tuple[Dict, Dict]:
        """Normalize SPO2 data into separate tables.
        
        Args:
            data: Raw SPO2 data
            
        Returns:
            Tuple of (spo2_data, percentage_data)
        """
        spo2_data = {
            k: v for k, v in data.items() 
            if not isinstance(v, (dict, list)) and k != 'spo2_percentage'
        }
        
        # Handle SPO2 percentage
        percentage_data = None
        if 'spo2_percentage' in data and isinstance(data['spo2_percentage'], dict):
            percentage = data['spo2_percentage']
            percentage_data = {
                'id': str(uuid.uuid4()),
                'daily_spo2_id': data['id'],
                'average': percentage.get('average'),
                'timestamp': data.get('timestamp')
            }
        
        return spo2_data, percentage_data

    def _normalize_resilience_data(self, data: Dict[str, Any]) -> Tuple[Dict, Dict]:
        """Normalize resilience data into separate tables.
        
        Args:
            data: Raw resilience data
            
        Returns:
            Tuple of (resilience_data, contributors_data)
        """
        resilience_data = {
            k: v for k, v in data.items() 
            if not isinstance(v, (dict, list)) and k != 'contributors'
        }
        
        # Handle contributors
        contributors_data = None
        if 'contributors' in data:
            contributors = data['contributors']
            contributors_data = {
                'id': str(uuid.uuid4()),
                'daily_resilience_id': data['id'],
                'sleep_recovery': float(contributors.get('sleep_recovery', 0)),
                'daytime_recovery': float(contributors.get('daytime_recovery', 0)),
                'stress': float(contributors.get('stress', 0))
            }
        
        return resilience_data, contributors_data

    def _normalize_daily_sleep(self, data: Dict[str, Any]) -> Tuple[Dict, Dict]:
        """Normalize daily sleep data into separate tables.
        
        Args:
            data: Raw daily sleep data
            
        Returns:
            Tuple of (sleep_data, contributors_data)
        """
        sleep_data = {
            k: v for k, v in data.items() 
            if not isinstance(v, (dict, list)) and k != 'contributors'
        }
        
        # Handle contributors
        contributors_data = None
        if 'contributors' in data:
            contributors = data['contributors']
            contributors_data = {
                'id': str(uuid.uuid4()),
                'daily_sleep_id': data['id'],
                **contributors
            }
        
        return sleep_data, contributors_data

    def transform_to_parquet(self, data_type: str, raw_data: Dict[str, Any]) -> List[Path]:
        """Transform raw data and save to parquet.
        
        Args:
            data_type: Type of data to transform
            raw_data: Raw JSON data from the API
            
        Returns:
            List of paths to saved parquet files
        """
        # Create data type directory
        data_dir = self.processed_dir / data_type
        data_dir.mkdir(exist_ok=True)
        
        # Generate timestamp for filenames
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        saved_files = []
        
        # Extract data items
        items = raw_data.get('data', [])
        if not items:
            logger.warning(f"No data items found for {data_type}")
            return saved_files
        
        if data_type == 'daily_activity':
            # Process main activity data
            activity_records = []
            contributor_records = []
            metric_records = []
            
            for item in items:
                activity, contributors, metrics = self._normalize_activity_data(item)
                activity_records.append(activity)
                if contributors:
                    contributor_records.append(contributors)
                metric_records.extend(metrics)
            
            # Save activity data
            if activity_records:
                file_path = data_dir / f"{data_type}_{timestamp}.parquet"
                pl.DataFrame(activity_records).write_parquet(file_path, compression="snappy")
                saved_files.append(file_path)
            
            # Save contributors
            if contributor_records:
                file_path = data_dir / f"activity_contributors_{timestamp}.parquet"
                pl.DataFrame(contributor_records).write_parquet(file_path, compression="snappy")
                saved_files.append(file_path)
            
            # Save metrics
            if metric_records:
                file_path = data_dir / f"activity_metrics_{timestamp}.parquet"
                pl.DataFrame(metric_records).write_parquet(file_path, compression="snappy")
                saved_files.append(file_path)
                
        elif data_type == 'sleep':
            # Process main sleep data
            sleep_records = []
            heart_rate_records = []
            hrv_records = []
            
            for item in items:
                sleep, heart_rates, hrvs = self._normalize_sleep_data(item)
                sleep_records.append(sleep)
                heart_rate_records.extend(heart_rates)
                hrv_records.extend(hrvs)
            
            # Save sleep data
            if sleep_records:
                file_path = data_dir / f"{data_type}_{timestamp}.parquet"
                pl.DataFrame(sleep_records).write_parquet(file_path, compression="snappy")
                saved_files.append(file_path)
            
            # Save heart rate samples
            if heart_rate_records:
                file_path = data_dir / f"heart_rate_samples_{timestamp}.parquet"
                pl.DataFrame(heart_rate_records).write_parquet(file_path, compression="snappy")
                saved_files.append(file_path)
            
            # Save HRV samples
            if hrv_records:
                file_path = data_dir / f"hrv_samples_{timestamp}.parquet"
                pl.DataFrame(hrv_records).write_parquet(file_path, compression="snappy")
                saved_files.append(file_path)

        elif data_type == 'daily_readiness':
            # Process readiness data
            readiness_records = []
            contributor_records = []
            
            for item in items:
                readiness, contributors = self._normalize_readiness_data(item)
                readiness_records.append(readiness)
                if contributors:
                    contributor_records.append(contributors)
            
            # Save readiness data
            if readiness_records:
                file_path = data_dir / f"{data_type}_{timestamp}.parquet"
                pl.DataFrame(readiness_records).write_parquet(file_path, compression="snappy")
                saved_files.append(file_path)
            
            # Save contributors
            if contributor_records:
                file_path = data_dir / f"readiness_contributors_{timestamp}.parquet"
                pl.DataFrame(contributor_records).write_parquet(file_path, compression="snappy")
                saved_files.append(file_path)

        elif data_type == 'daily_sleep':
            # Process daily sleep data
            sleep_records = []
            contributor_records = []
            
            for item in items:
                sleep, contributors = self._normalize_daily_sleep(item)
                sleep_records.append(sleep)
                if contributors:
                    contributor_records.append(contributors)
            
            # Save sleep data
            if sleep_records:
                file_path = data_dir / f"{data_type}_{timestamp}.parquet"
                pl.DataFrame(sleep_records).write_parquet(file_path, compression="snappy")
                saved_files.append(file_path)
            
            # Save contributors - using 'sleep_contributors' to match the database table name
            if contributor_records:
                # Save in the daily_sleep directory but with correct table name prefix
                file_path = data_dir / f"daily_sleep_contributors_{timestamp}.parquet"
                pl.DataFrame(contributor_records).write_parquet(file_path, compression="snappy")
                saved_files.append(file_path)

        elif data_type == 'daily_spo2':
            # Process SPO2 data
            spo2_records = []
            percentage_records = []
            
            for item in items:
                spo2, percentage = self._normalize_spo2_data(item)
                spo2_records.append(spo2)
                if percentage:
                    percentage_records.append(percentage)
            
            # Save SPO2 data
            if spo2_records:
                file_path = data_dir / f"{data_type}_{timestamp}.parquet"
                pl.DataFrame(spo2_records).write_parquet(file_path, compression="snappy")
                saved_files.append(file_path)
            
            # Save percentage data
            if percentage_records:
                file_path = data_dir / f"spo2_percentage_{timestamp}.parquet"
                pl.DataFrame(percentage_records).write_parquet(file_path, compression="snappy")
                saved_files.append(file_path)

        elif data_type == 'daily_resilience':
            # Process resilience data
            resilience_records = []
            contributor_records = []
            
            for item in items:
                resilience, contributors = self._normalize_resilience_data(item)
                resilience_records.append(resilience)
                if contributors:
                    contributor_records.append(contributors)
            
            # Save resilience data
            if resilience_records:
                file_path = data_dir / f"{data_type}_{timestamp}.parquet"
                pl.DataFrame(resilience_records).write_parquet(file_path, compression="snappy")
                saved_files.append(file_path)
            
            # Save contributors
            if contributor_records:
                file_path = data_dir / f"resilience_contributors_{timestamp}.parquet"
                pl.DataFrame(contributor_records).write_parquet(file_path, compression="snappy")
                saved_files.append(file_path)
        
        else:
            # Handle other data types normally
            flattened_data = [
                {k: v for k, v in item.items() if not isinstance(v, (dict, list))}
                for item in items
            ]
            
            file_path = data_dir / f"{data_type}_{timestamp}.parquet"
            pl.DataFrame(flattened_data).write_parquet(file_path, compression="snappy")
            saved_files.append(file_path)
        
        logger.info(f"Saved {len(saved_files)} processed files for {data_type}")
        return saved_files
    
    def transform_file(self, data_type: str, raw_file: Union[str, Path]) -> List[Path]:
        """Transform data from a raw JSON file and save as parquet.
        
        Args:
            data_type: Type of data to transform
            raw_file: Path to the raw data file
            
        Returns:
            List of paths to saved parquet files
        """
        raw_data = self.load_json_file(raw_file)
        return self.transform_to_parquet(data_type, raw_data)
    
    def transform_directory(self, raw_dir: Union[str, Path]) -> None:
        """Transform all raw data files in a directory to parquet.
        
        Args:
            raw_dir: Directory containing raw data files
        """
        raw_dir = Path(raw_dir)
        if not raw_dir.exists():
            raise ValueError(f"Directory not found: {raw_dir}")
        
        logger.info(f"Starting transformation of files in {raw_dir}")
        
        for data_type in MODEL_MAP:
            logger.info(f"Processing data type: {data_type}")
            
            # Create data type directory in processed dir
            processed_data_dir = self.processed_dir / data_type
            processed_data_dir.mkdir(parents=True, exist_ok=True)
            logger.debug(f"Created/verified processed directory: {processed_data_dir}")
            
            # Try nested directory structure first
            raw_data_dir = raw_dir / data_type
            json_files = []
            
            if raw_data_dir.exists():
                # If nested directory exists, get files from there
                json_files.extend(raw_data_dir.glob("*.json"))
            
            # Also check for files in the root directory matching the data type
            json_files.extend(raw_dir.glob(f"{data_type}_*.json"))
            
            if not json_files:
                logger.warning(f"No JSON files found for {data_type}")
                continue
                
            logger.info(f"Found {len(json_files)} JSON files for {data_type}")
            
            # Process all JSON files
            for json_file in json_files:
                logger.info(f"Processing file: {json_file}")
                try:
                    # Load JSON data
                    raw_data = self.load_json_file(json_file)
                    
                    # Extract data items
                    items = raw_data.get('data', [])
                    logger.info(f"Found {len(items)} items in {json_file}")
                    
                    if not items:
                        logger.warning(f"No data items found in {json_file}")
                        continue
                    
                    # Transform and save data
                    saved_files = self.transform_to_parquet(data_type, raw_data)
                    logger.info(f"Saved {len(saved_files)} files for {data_type}")
                    
                except Exception as e:
                    logger.error(f"Failed to transform {json_file}: {str(e)}", exc_info=True)
                    continue
        
        logger.info("Completed directory transformation")
    
    @staticmethod
    def load_json_file(file_path: Union[str, Path]) -> Dict[str, Any]:
        """Load JSON data from a file.
        
        Args:
            file_path: Path to the JSON file
            
        Returns:
            Dictionary containing the JSON data
        """
        with open(file_path, 'r') as f:
            return json.load(f)

def load_json_file(file_path: Union[str, Path]) -> Dict[str, Any]:
    """Load JSON data from a file.
    
    Args:
        file_path: Path to the JSON file
        
    Returns:
        Dictionary containing the JSON data
    """
    with open(file_path, 'r') as f:
        return json.load(f)

def transform_activity_contributors(data: Dict[str, Any]) -> ActivityContributors:
    """Transform activity contributors data."""
    return ActivityContributors(
        meet_daily_targets=data.get('meet_daily_targets'),
        move_every_hour=data.get('move_every_hour'),
        recovery_time=data.get('recovery_time'),
        stay_active=data.get('stay_active'),
        training_frequency=data.get('training_frequency'),
        training_volume=data.get('training_volume')
    )

def transform_daily_activity(data: Dict[str, Any]) -> DailyActivity:
    """Transform daily activity data."""
    return DailyActivity(
        id=data['id'],
        day=data['day'],
        class_5_min=data.get('class_5_min'),
        score=data.get('score'),
        active_calories=data['active_calories'],
        average_met_minutes=data['average_met_minutes'],
        contributors=transform_activity_contributors(data['contributors']),
        equivalent_walking_distance=data['equivalent_walking_distance'],
        high_activity_met_minutes=data['high_activity_met_minutes'],
        high_activity_time=data['high_activity_time'],
        inactivity_alerts=data['inactivity_alerts'],
        low_activity_met_minutes=data['low_activity_met_minutes'],
        low_activity_time=data['low_activity_time'],
        medium_activity_met_minutes=data['medium_activity_met_minutes'],
        medium_activity_time=data['medium_activity_time'],
        meters_to_target=data['meters_to_target'],
        non_wear_time=data['non_wear_time'],
        resting_time=data['resting_time'],
        sedentary_met_minutes=data['sedentary_met_minutes'],
        sedentary_time=data['sedentary_time'],
        steps=data['steps'],
        target_calories=data['target_calories'],
        target_meters=data['target_meters'],
        total_calories=data['total_calories'],
        timestamp=data['timestamp']
    )

def transform_readiness_contributors(data: Dict[str, Any]) -> ReadinessContributors:
    """Transform readiness contributors data."""
    return ReadinessContributors(
        activity_balance=data.get('activity_balance'),
        body_temperature=data.get('body_temperature'),
        hrv_balance=data.get('hrv_balance'),
        previous_day_activity=data.get('previous_day_activity'),
        previous_night=data.get('previous_night'),
        recovery_index=data.get('recovery_index'),
        resting_heart_rate=data.get('resting_heart_rate'),
        sleep_balance=data.get('sleep_balance')
    )

def transform_daily_readiness(data: Dict[str, Any]) -> DailyReadiness:
    """Transform daily readiness data."""
    return DailyReadiness(
        id=data['id'],
        contributors=transform_readiness_contributors(data['contributors']),
        day=data['day'],
        score=data.get('score'),
        temperature_deviation=data.get('temperature_deviation'),
        temperature_trend_deviation=data.get('temperature_trend_deviation'),
        timestamp=data['timestamp']
    )

def transform_sleep_contributors(data: Dict[str, Any]) -> SleepContributors:
    """Transform sleep contributors data."""
    return SleepContributors(
        deep_sleep=data.get('deep_sleep'),
        efficiency=data.get('efficiency'),
        latency=data.get('latency'),
        rem_sleep=data.get('rem_sleep'),
        restfulness=data.get('restfulness'),
        timing=data.get('timing'),
        total_sleep=data.get('total_sleep')
    )

def transform_daily_sleep(data: Dict[str, Any]) -> DailySleep:
    """Transform daily sleep data."""
    return DailySleep(
        id=data['id'],
        contributors=transform_sleep_contributors(data['contributors']),
        day=data['day'],
        score=data.get('score'),
        timestamp=data['timestamp']
    )

def transform_sleep(data: Dict[str, Any]) -> Sleep:
    """Transform detailed sleep data."""
    return Sleep(
        id=data['id'],
        average_breath=data.get('average_breath'),
        average_heart_rate=data.get('average_heart_rate'),
        average_hrv=data.get('average_hrv'),
        awake_time=data.get('awake_time'),
        bedtime_end=data['bedtime_end'],
        bedtime_start=data['bedtime_start'],
        day=data['day'],
        deep_sleep_duration=data.get('deep_sleep_duration'),
        efficiency=data.get('efficiency'),
        latency=data.get('latency'),
        light_sleep_duration=data.get('light_sleep_duration'),
        low_battery_alert=data['low_battery_alert'],
        lowest_heart_rate=data.get('lowest_heart_rate'),
        movement_30_sec=data.get('movement_30_sec'),
        period=data['period'],
        rem_sleep_duration=data.get('rem_sleep_duration'),
        restless_periods=data.get('restless_periods'),
        sleep_phase_5_min=data.get('sleep_phase_5_min'),
        sleep_score_delta=data.get('sleep_score_delta'),
        time_in_bed=data['time_in_bed'],
        total_sleep_duration=data.get('total_sleep_duration'),
        type=data['type']
    )

def transform_heart_rate(data: Dict[str, Any]) -> HeartRate:
    """Transform heart rate data."""
    return HeartRate(
        id=data['id'],
        bpm=data['bpm'],
        source=data['source'],
        timestamp=data['timestamp']
    )

def transform_sample(data: Dict[str, Any]) -> Sample:
    """Transform sample data."""
    return Sample(
        interval=data['interval'],
        items=data['items'],
        timestamp=data['timestamp']
    )

def transform_personal_info(data: Dict[str, Any]) -> PersonalInfo:
    """Transform personal info data."""
    return PersonalInfo(
        id=data['id'],
        age=data.get('age'),
        weight=data.get('weight'),
        height=data.get('height'),
        biological_sex=data.get('biological_sex'),
        email=data.get('email')
    )

def transform_data(data_type: str, raw_data: Dict[str, Any]) -> List[Any]:
    """Transform raw JSON data into appropriate data models.
    
    Args:
        data_type: Type of data to transform (e.g., 'daily_activity', 'sleep')
        raw_data: Raw JSON data from the API
        
    Returns:
        List of transformed data objects
    """
    # Handle pagination response format
    items = raw_data.get('data', [])
    
    transform_functions = {
        'daily_activity': transform_daily_activity,
        'daily_readiness': transform_daily_readiness,
        'daily_sleep': transform_daily_sleep,
        'sleep': transform_sleep,
        'heart_rate': transform_heart_rate,
        'personal_info': transform_personal_info
    }
    
    transform_func = transform_functions.get(data_type)
    if not transform_func:
        raise ValueError(f"Unsupported data type: {data_type}")
        
    return [transform_func(item) for item in items] 