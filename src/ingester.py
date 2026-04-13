import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import Optional, List
from src.pipeline import Bi5Parser

class MicrostructureIngester:
    """
    Orchestrates the ingestion pipeline: from raw binary storage to 
    analytical DataFrames.
    
    This class acts as the bridge between storage (raw .bi5 files) 
    and the MicrostructureEngine.
    """

    def __init__(self, raw_data_dir: str):
        self.raw_data_dir = Path(raw_data_dir)
        if not self.raw_data_dir.exists():
            raise FileNotFoundError(f"Raw data directory not found: {raw_data_dir}")

    def get_event_data(self, event_timestamp: pd.Timestamp) -> pd.DataFrame:
        """
        Retrieves and parses the specific data hour required for a given event.
        
        Args:
            event_timestamp: The exact time of the Riksbank announcement.
            
        Returns:
            A parsed DataFrame containing the full hour of tick data.
        """
        # Dukascopy files are stored by the start of the hour (UTC)
        target_hour = event_timestamp.replace(minute=0, second=0, microsecond=0)
        file_name = f"EURSEK_{target_hour.strftime('%Y%m%d_%H00')}.bi5"
        file_path = self.raw_data_dir / file_name

        try:
            # We delegate the binary heavy-lifting to Bi5Parser
            df = Bi5Parser.parse_file(file_path, target_hour)
            return df
        except FileNotFoundError:
            print(f"Warning: Data file for {target_hour} is missing.")
            return pd.DataFrame()
        except Exception as e:
            print(f"Error ingesting {file_name}: {e}")
            return pd.DataFrame()

    def batch_ingest(self, event_list: List[pd.Timestamp]) -> dict:
        """
        Processes multiple events into a dictionary of DataFrames.
        """
        registry = {}
        for event in event_list:
            df = self.get_event_data(event)
            if not df.empty:
                registry[event.strftime("%Y-%m-%d %H:%M")] = df
        return registry

if __name__ == "__main__":
    print("Module 1 (Ingester) - Data Flow Controller Ready.")