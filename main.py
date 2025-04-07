"""Main entry point for the Oura ETL pipeline.

This script provides the command-line interface for running the ETL pipeline.
Example usage:
    python main.py --data-types sleep daily_activity --days 7
    python main.py --all --start-date 2024-01-01 --end-date 2024-01-31
"""

from oura_etl.launcher import main

if __name__ == "__main__":
    main()
