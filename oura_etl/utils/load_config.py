"""Configuration loader utility for the Oura ETL pipeline."""

import os
import toml
from pathlib import Path
from typing import Optional, Dict, Any
from dataclasses import asdict

from oura_etl.config import OuraAPIConfig, DatabaseConfig

def load_config(config_path: Optional[str] = None) -> Dict[str, Any]:
    """Load configuration from environment variables and optionally from a TOML file.
    
    Args:
        config_path: Optional path to a TOML configuration file
        
    Returns:
        Dictionary containing the merged configuration
        
    Raises:
        ValueError: If no database path is configured in either config.toml or environment
    """
    # Start with default configurations
    api_config = OuraAPIConfig()
    db_config = DatabaseConfig()
    
    # Load from TOML file if provided
    if config_path:
        config_path = Path(config_path)
        if config_path.exists():
            toml_config = toml.load(config_path)
            
            # Check for uppercase API section first
            if "API" in toml_config:
                api_dict = toml_config["API"]
                if "TOKEN" in api_dict:
                    api_config.access_token = api_dict["TOKEN"]
                if "BASE_URL" in api_dict:
                    api_config.base_url = api_dict["BASE_URL"]
            
            # Update API config if present
            if "api" in toml_config:
                api_dict = toml_config["api"]
                # Only update fields that exist in the config class
                for key in api_dict:
                    if hasattr(api_config, key):
                        setattr(api_config, key, api_dict[key])
            
            # Update database config if present
            if "database" in toml_config:
                db_dict = toml_config["database"]
                for key in db_dict:
                    if hasattr(db_config, key):
                        setattr(db_config, key, db_dict[key])
    
    # Environment variables take precedence
    if "OURA_ACCESS_TOKEN" in os.environ:
        api_config.access_token = os.environ["OURA_ACCESS_TOKEN"]
        
    if "OURA_API_URL" in os.environ:
        api_config.base_url = os.environ["OURA_API_URL"]
    
    if "OURA_RATE_LIMIT" in os.environ:
        try:
            api_config.rate_limit_per_minute = int(os.environ["OURA_RATE_LIMIT"])
        except ValueError:
            pass  # Keep default if invalid
    
    if "OURA_DB_PATH" in os.environ:
        db_config.db_path = os.environ["OURA_DB_PATH"]
    
    # Ensure database path is set
    if db_config.db_path is None:
        raise ValueError(
            "Database path must be configured either in config.toml [database].db_path "
            "or in OURA_DB_PATH environment variable"
        )
    
    # Return merged configuration
    return {
        "api": asdict(api_config),
        "database": asdict(db_config)
    }

def get_api_config(config_path: Optional[str] = None) -> OuraAPIConfig:
    """Get API configuration.
    
    Args:
        config_path: Optional path to a TOML configuration file
        
    Returns:
        OuraAPIConfig instance
    """
    config = load_config(config_path)
    return OuraAPIConfig(**config["api"])

def get_db_config(config_path: Optional[str] = None) -> DatabaseConfig:
    """Get database configuration.
    
    Args:
        config_path: Optional path to a TOML configuration file
        
    Returns:
        DatabaseConfig instance
    """
    config = load_config(config_path)
    return DatabaseConfig(**config["database"]) 