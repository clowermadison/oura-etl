"""Logging configuration utility for the Oura ETL pipeline."""

import logging
from datetime import datetime
from pathlib import Path
from typing import Union

def setup_logging(log_dir: Union[str, Path] = "logs") -> Path:
    """Set up logging configuration.
    
    Args:
        log_dir: Directory to store log files (default: logs)
        
    Returns:
        Path object pointing to the created log file
    """
    # Create logs directory if it doesn't exist
    log_dir = Path(log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)
    
    # Generate log filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = log_dir / f"oura_etl_{timestamp}.log"
    
    # Configure logging to write to both file and console
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    
    return log_file 