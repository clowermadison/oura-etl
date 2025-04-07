"""Launcher module for the Oura ETL pipeline.

This module provides command-line interface for running the ETL pipeline.
"""

import os
import argparse
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Optional

from oura_etl.controller import OuraController
from oura_etl.utils.load_config import get_api_config
from oura_etl.utils.logging import setup_logging
from oura_etl.database import MODEL_MAP

# Set up logging
log_file = setup_logging()
logger = logging.getLogger(__name__)
logger.info(f"Logging to {log_file}")

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Oura Ring ETL Pipeline")
    
    # Required arguments
    parser.add_argument(
        "--data-types",
        type=str,
        nargs="+",
        choices=list(MODEL_MAP.keys()),
        help=f"Data types to extract (available types: {', '.join(MODEL_MAP.keys())})"
    )
    
    # Optional arguments
    parser.add_argument(
        "--config",
        type=str,
        help="Path to configuration file"
    )
    parser.add_argument(
        "--raw-dir",
        type=str,
        default="data/raw",
        help="Directory for raw data (default: data/raw)"
    )
    parser.add_argument(
        "--processed-dir",
        type=str,
        default="data/processed",
        help="Directory for processed data (default: data/processed)"
    )
    parser.add_argument(
        "--start-date",
        type=str,
        help="Start date in YYYY-MM-DD format"
    )
    parser.add_argument(
        "--end-date",
        type=str,
        help="End date in YYYY-MM-DD format"
    )
    parser.add_argument(
        "--days",
        type=int,
        default=7,
        help="Number of recent days to extract (default: 7)"
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Extract all available data types"
    )
    parser.add_argument(
        "--steps",
        type=str,
        nargs="+",
        choices=['extract', 'process', 'load'],
        default=['extract', 'process', 'load'],
        help="Pipeline steps to run (default: all steps)"
    )
    
    return parser.parse_args()

def validate_dates(start_date: Optional[str], end_date: Optional[str]) -> None:
    """Validate date formats and ranges.
    
    Args:
        start_date: Start date string in YYYY-MM-DD format
        end_date: End date string in YYYY-MM-DD format
        
    Raises:
        ValueError: If dates are invalid
    """
    if start_date:
        try:
            start = datetime.strptime(start_date, "%Y-%m-%d")
        except ValueError:
            raise ValueError(f"Invalid start date format: {start_date}. Use YYYY-MM-DD")
    
    if end_date:
        try:
            end = datetime.strptime(end_date, "%Y-%m-%d")
        except ValueError:
            raise ValueError(f"Invalid end date format: {end_date}. Use YYYY-MM-DD")
    
    if start_date and end_date:
        if start > end:
            raise ValueError("Start date must be before end date")

def run_pipeline(
    data_types: Optional[List[str]] = None,
    config_path: Optional[str] = None,
    raw_dir: str = "data/raw",
    processed_dir: str = "data/processed",
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    days: int = 7,
    extract_all: bool = False,
    steps: List[str] = ['extract', 'process', 'load']
) -> None:
    """Run the ETL pipeline with specified parameters.
    
    Args:
        data_types: List of data types to extract
        config_path: Path to configuration file
        raw_dir: Directory for raw data
        processed_dir: Directory for processed data
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        days: Number of recent days to extract
        extract_all: Whether to extract all available data types
        steps: List of pipeline steps to run
    """
    # Get access token from config or environment
    api_config = get_api_config(config_path)
    access_token = api_config.access_token
    
    # Fall back to environment variable if not in config
    if not access_token:
        access_token = os.getenv("OURA_ACCESS_TOKEN")
        
    if not access_token:
        raise ValueError("Access token must be set in config file or OURA_ACCESS_TOKEN environment variable")
    
    # Initialize controller
    controller = OuraController(
        access_token=access_token,
        raw_dir=raw_dir,
        processed_dir=processed_dir,
        config_path=config_path
    )
    
    try:
        # Extract data
        if 'extract' in steps:
            logger.info("Starting extraction step")
            if extract_all:
                controller.extract_all_data()
            elif start_date or end_date:
                validate_dates(start_date, end_date)
                controller.extract_data(data_types, start_date, end_date)
            else:
                controller.extract_recent_data(data_types, days)
        
        # Process data
        if 'process' in steps:
            logger.info("Starting processing step")
            controller.process_data()
        
        # Load data
        if 'load' in steps:
            logger.info("Starting loading step")
            controller.load_data()
            
    except Exception as e:
        logger.error(f"Pipeline step failed: {str(e)}")
        raise

def main():
    """Main entry point for the launcher."""
    try:
        args = parse_args()
        
        if not args.all and not args.data_types:
            raise ValueError("Must specify --data-types or --all")
        
        run_pipeline(
            data_types=args.data_types,
            config_path=args.config,
            raw_dir=args.raw_dir,
            processed_dir=args.processed_dir,
            start_date=args.start_date,
            end_date=args.end_date,
            days=args.days,
            extract_all=args.all,
            steps=args.steps
        )
        
        logger.info("Pipeline completed successfully")
        
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()
