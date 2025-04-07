"""Database loader module for the Oura ETL pipeline.

This module handles loading processed parquet files into the database.
"""

import logging
import json
from pathlib import Path
from typing import List, Any, Dict, Optional, Union

import polars as pl
import pandas as pd
from sqlalchemy import create_engine

from oura_etl.database import Database, MODEL_MAP
from oura_etl.utils.load_config import get_db_config

logger = logging.getLogger(__name__)

class OuraLoader:
    """Class to handle loading processed data into the database."""
    
    def __init__(self, db_path: Optional[str] = None):
        """Initialize the loader.
        
        Args:
            db_path: Optional path to the SQLite database file. If not provided, uses config.toml setting.
        """
        if db_path is None:
            db_config = get_db_config()
            db_path = db_config.db_path
            
        self.db = Database(f"sqlite:///{db_path}")
        self.db.create_tables()
        self.engine = create_engine(f"sqlite:///{db_path}")
    
    def _prepare_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Prepare dataframe for database insertion by handling JSON fields.
        
        Args:
            df: Input pandas DataFrame
            
        Returns:
            Processed DataFrame with JSON fields converted to strings
        """
        for column in df.columns:
            # Check if column contains dictionaries or lists
            if df[column].dtype == 'object':
                mask = df[column].apply(lambda x: isinstance(x, (dict, list)))
                if mask.any():
                    # Convert dictionaries and lists to JSON strings
                    df.loc[mask, column] = df.loc[mask, column].apply(json.dumps)
        return df
    
    def load_parquet(self, data_type: str, parquet_files: List[Path]) -> None:
        """Load parquet files into the database.
        
        Args:
            data_type: Type of data being loaded
            parquet_files: List of parquet files to load
        """
        for file_path in parquet_files:
            logger.info(f"Loading {file_path}")
            
            # Read parquet file into polars DataFrame
            df = pl.read_parquet(file_path)
            
            # Convert to pandas for SQLAlchemy compatibility
            pdf = df.to_pandas()
            
            # Determine table based on filename
            filename = file_path.name
            if filename.startswith('activity_contributors_'):
                table = 'activity_contributors'
            elif filename.startswith('activity_metrics_'):
                table = 'activity_metrics'
            elif filename.startswith('heart_rate_samples_'):
                table = 'heart_rate_samples'
            elif filename.startswith('hrv_samples_'):
                table = 'hrv_samples'
            elif filename.startswith('readiness_contributors_'):
                table = 'readiness_contributors'
            elif filename.startswith('daily_sleep_contributors_'):
                table = 'sleep_contributors'
            elif filename.startswith('spo2_percentage_'):
                table = 'spo2_percentage'
            elif filename.startswith('resilience_contributors_'):
                table = 'resilience_contributors'
            else:
                table = data_type
            
            # Load into database
            try:
                pdf.to_sql(
                    table,
                    self.engine,
                    if_exists='append',
                    index=False
                )
                logger.info(f"Successfully loaded {len(pdf)} records into {table}")
            except Exception as e:
                logger.error(f"Error loading {file_path} into {table}: {str(e)}")
                raise
    
    def load_from_directory(self, processed_dir: Union[str, Path]) -> None:
        """Load all processed parquet files from a directory.
        
        Args:
            processed_dir: Directory containing processed parquet files
        """
        processed_dir = Path(processed_dir)
        if not processed_dir.exists():
            raise ValueError(f"Directory not found: {processed_dir}")
        
        for data_type in MODEL_MAP:
            data_dir = processed_dir / data_type
            if not data_dir.exists():
                continue
            
            for file_path in data_dir.glob("*.parquet"):
                logger.info(f"Loading {file_path}")
                try:
                    self.load_parquet(data_type, [file_path])
                except Exception as e:
                    logger.error(f"Failed to load {file_path}: {str(e)}")
                    continue 