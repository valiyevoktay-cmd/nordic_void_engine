import urllib.request
import urllib.error
from pathlib import Path
from datetime import datetime
import pandas as pd

class DukascopyDownloader:
    """
    Automated fetcher for historical tick data from Dukascopy datafeed.
    
    Microstructure Rationale:
    To analyze the Riksbank Liquidity Vacuum, we need continuous L2 order book updates
    exactly during the macro event window. This module programmatically constructs
    the correct HTTP requests to Dukascopy's servers, accounting for their 
    zero-indexed month formatting, to guarantee temporal accuracy.
    """
    
    BASE_URL = "https://datafeed.dukascopy.com/datafeed"

    @staticmethod
    def fetch_hour(instrument: str, target_time: datetime, raw_data_dir: Path) -> Path:
        """
        Downloads a specific 1-hour .bi5 tick file.
        
        Args:
            instrument: Currency pair string, e.g., 'EURSEK'
            target_time: UTC datetime object representing the hour to download.
            raw_data_dir: Destination directory for the raw .bi5 file.
            
        Returns:
            Path: The local path to the downloaded file.
        """
        # Dukascopy Trap: Months are 0-indexed (00=Jan, 11=Dec)
        year_str = str(target_time.year)
        month_str = f"{target_time.month - 1:02d}" 
        day_str = f"{target_time.day:02d}"
        hour_str = f"{target_time.hour:02d}"

        # Construct the Dukascopy URL
        # Format: https://datafeed.dukascopy.com/datafeed/EURSEK/2023/08/21/09h_ticks.bi5
        url = f"{DukascopyDownloader.BASE_URL}/{instrument}/{year_str}/{month_str}/{day_str}/{hour_str}h_ticks.bi5"
        
        # Ensure the target directory exists
        raw_data_dir.mkdir(parents=True, exist_ok=True)
        
        # Local filename format: EURSEK_20230921_0900.bi5
        # Notice we use the ACTUAL month for our local filename to avoid confusion
        local_filename = f"{instrument}_{target_time.strftime('%Y%m%d_%H00')}.bi5"
        file_path = raw_data_dir / local_filename

        # If we already downloaded it, skip to save network I/O
        if file_path.exists():
            print(f"File already exists locally: {file_path}")
            return file_path

        try:
            print(f"Fetching {instrument} order flow for {target_time} UTC...")
            urllib.request.urlretrieve(url, file_path)
            print(f"Successfully downloaded to: {file_path}")
            return file_path
        except urllib.error.HTTPError as e:
            if e.code == 404:
                raise FileNotFoundError(f"Data not found on Dukascopy for {url}. Check weekend/holiday logic.")
            raise e

if __name__ == "__main__":
    # Integration Smoke Test:
    test_time = datetime(2023, 9, 21, 7, 0, 0) # 07:00 AM UTC
    
    # Bulletproof path resolution:
    # __file__ is downloader.py
    # .resolve().parent is src/
    # .parent.parent is riksbank_liquidity_vacuum/
    project_root = Path(__file__).resolve().parent.parent
    data_dir = project_root / "data" / "raw"
    
    try:
        downloaded_file = DukascopyDownloader.fetch_hour(
            instrument="EURSEK", 
            target_time=test_time, 
            raw_data_dir=data_dir
        )
        print("Module 2 (Downloader) successfully tested.")
    except Exception as e:
        print(f"Test failed: {e}")