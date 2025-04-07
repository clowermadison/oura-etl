"""Controller module for the Oura ETL pipeline.

This module orchestrates the ETL process in three distinct stages:
1. Extract: Raw JSON from API → data/raw/{data_type}/*.json
2. Process: Raw JSON → data/processed/{data_type}/*.parquet
3. Load: Parquet files → SQLite database
"""

import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional, Dict, Any

from oura_etl.downloader import OuraDownloader
from oura_etl.transformers import OuraTransformer
from oura_etl.loader import OuraLoader
from oura_etl.database import MODEL_MAP
from oura_etl.utils.load_config import get_api_config, get_db_config

logger = logging.getLogger(__name__)

class OuraController:
    """Controller class for orchestrating the ETL pipeline."""
    
    def __init__(
        self,
        access_token: str,
        raw_dir: str = "data/raw",
        processed_dir: str = "data/processed",
        config_path: Optional[str] = None
    ):
        """Initialize the controller.
        
        Args:
            access_token: Oura API access token
            raw_dir: Directory for storing raw JSON data
            processed_dir: Directory for storing processed parquet files
            config_path: Path to configuration file
        """
        self.downloader = OuraDownloader(
            access_token=access_token,
            output_dir=raw_dir
        )
        self.transformer = OuraTransformer(processed_dir)
        self.loader = OuraLoader()  # Let the loader use the config for db_path
        
        self.raw_dir = Path(raw_dir)
        self.processed_dir = Path(processed_dir)
        self.config_path = config_path
        
        # Create data directories
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.processed_dir.mkdir(parents=True, exist_ok=True)
        
    def extract_data(
        self,
        data_types: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> None:
        """Extract data from Oura API to raw JSON files.
        
        Args:
            data_types: List of data types to extract
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
        """
        if not data_types:
            data_types = list(MODEL_MAP.keys())
            
        for data_type in data_types:
            logger.info(f"Extracting {data_type} data")
            try:
                self.downloader.download_data(
                    data_type=data_type,
                    start_date=start_date,
                    end_date=end_date
                )
            except Exception as e:
                logger.error(f"Failed to extract {data_type} data: {str(e)}")
                continue
                
    def extract_recent_data(
        self,
        data_types: Optional[List[str]] = None,
        days: int = 7
    ) -> None:
        """Extract recent data from Oura API.
        
        Args:
            data_types: List of data types to extract
            days: Number of days to extract
        """
        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
        self.extract_data(data_types, start_date, end_date)
        
    def extract_all_data(self) -> None:
        """Extract all available data types."""
        self.extract_data(list(MODEL_MAP.keys()))
        
    def process_data(self) -> None:
        """Process raw JSON files into parquet format."""
        logger.info("Processing raw data to parquet")
        try:
            self.transformer.transform_directory(self.raw_dir)
        except Exception as e:
            logger.error(f"Failed to process data: {str(e)}")
            raise
            
    def load_data(self) -> None:
        """Load processed parquet files into the database."""
        logger.info("Loading processed data into database")
        try:
            self.loader.load_from_directory(self.processed_dir)
        except Exception as e:
            logger.error(f"Failed to load data: {str(e)}")
            raise
            
    def run_pipeline(
        self,
        data_types: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        days: Optional[int] = None,
        steps: Optional[List[str]] = None
    ) -> None:
        """Run the ETL pipeline with specified steps.
        
        Args:
            data_types: List of data types to process
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            days: Number of recent days to process
            steps: List of steps to run ('extract', 'process', 'load')
                  If None, runs all steps
        """
        if steps is None:
            steps = ['extract', 'process', 'load']
            
        try:
            if 'extract' in steps:
                logger.info("Starting extraction step")
                if days is not None:
                    self.extract_recent_data(data_types, days)
                else:
                    self.extract_data(data_types, start_date, end_date)
            
            if 'process' in steps:
                logger.info("Starting processing step")
                self.process_data()
            
            if 'load' in steps:
                logger.info("Starting loading step")
                self.load_data()
            
            logger.info("Pipeline completed successfully")
            
        except Exception as e:
            logger.error(f"Pipeline failed: {str(e)}")
            raise
