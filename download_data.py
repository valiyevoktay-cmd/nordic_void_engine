import requests
from pathlib import Path
import time

# List of files required for both the N=20 study and the Placebo Test
MISSING_FILES = [
    # --- Placebo Test Files (T-2 hours) ---
    "EURSEK_20201126_0600.bi5", "EURSEK_20210210_0600.bi5",
    "EURSEK_20210427_0500.bi5", "EURSEK_20210701_0500.bi5",
    "EURSEK_20210921_0500.bi5", "EURSEK_20211125_0600.bi5",
    "EURSEK_20220210_0600.bi5", "EURSEK_20220428_0500.bi5",
    "EURSEK_20220630_0500.bi5", "EURSEK_20220920_0500.bi5",
    "EURSEK_20221124_0600.bi5", "EURSEK_20230209_0600.bi5",
    "EURSEK_20230426_0500.bi5", "EURSEK_20230629_0500.bi5",
    "EURSEK_20230921_0500.bi5", "EURSEK_20231123_0600.bi5",
    "EURSEK_20240201_0600.bi5", "EURSEK_20240508_0500.bi5",
    "EURSEK_20240627_0500.bi5", "EURSEK_20240820_0500.bi5",
    "EURSEK_20140213_0800.bi5", "EURSEK_20140409_0700.bi5", 
    "EURSEK_20140703_0700.bi5", "EURSEK_20140904_0700.bi5", 
    "EURSEK_20141028_0800.bi5", "EURSEK_20141216_0800.bi5",
    # Placebo Hour (-2h)
    "EURSEK_20140213_0600.bi5", "EURSEK_20140409_0500.bi5", 
    "EURSEK_20140703_0500.bi5", "EURSEK_20140904_0500.bi5", 
    "EURSEK_20141028_0600.bi5", "EURSEK_20141216_0600.bi5",
    "EURSEK_20150212_0800.bi5", "EURSEK_20150429_0700.bi5", 
    "EURSEK_20150702_0700.bi5", "EURSEK_20150903_0700.bi5", 
    "EURSEK_20151028_0800.bi5", "EURSEK_20151215_0800.bi5",
    # Placebo Hour (-2h)
    "EURSEK_20150212_0600.bi5", "EURSEK_20150429_0500.bi5", 
    "EURSEK_20150702_0500.bi5", "EURSEK_20150903_0500.bi5", 
    "EURSEK_20151028_0600.bi5", "EURSEK_20151215_0600.bi5",

    # --- 2016 ---
    # News Hour
    "EURSEK_20160211_0800.bi5", "EURSEK_20160421_0700.bi5", 
    "EURSEK_20160706_0700.bi5", "EURSEK_20160907_0700.bi5", 
    "EURSEK_20161027_0800.bi5", "EURSEK_20161221_0800.bi5",
    # Placebo Hour (-2h)
    "EURSEK_20160211_0600.bi5", "EURSEK_20160421_0500.bi5", 
    "EURSEK_20160706_0500.bi5", "EURSEK_20160907_0500.bi5", 
    "EURSEK_20161027_0600.bi5", "EURSEK_20161221_0600.bi5",

    # --- 2017 ---
    # News Hour
    "EURSEK_20170215_0800.bi5", "EURSEK_20170427_0700.bi5", 
    "EURSEK_20170704_0700.bi5", "EURSEK_20170907_0700.bi5", 
    "EURSEK_20171026_0800.bi5", "EURSEK_20171220_0800.bi5",
    # Placebo Hour (-2h)
    "EURSEK_20170215_0600.bi5", "EURSEK_20170427_0500.bi5", 
    "EURSEK_20170704_0500.bi5", "EURSEK_20170907_0500.bi5", 
    "EURSEK_20171026_0600.bi5", "EURSEK_20171220_0600.bi5",

    # --- 2018 ---
    # News Hour
    "EURSEK_20180214_0800.bi5", "EURSEK_20180426_0700.bi5", 
    "EURSEK_20180703_0700.bi5", "EURSEK_20180906_0700.bi5", 
    "EURSEK_20181024_0800.bi5", "EURSEK_20181220_0800.bi5",
    # Placebo Hour (-2h)
    "EURSEK_20180214_0600.bi5", "EURSEK_20180426_0500.bi5", 
    "EURSEK_20180703_0500.bi5", "EURSEK_20180906_0500.bi5", 
    "EURSEK_20181024_0600.bi5", "EURSEK_20181220_0600.bi5",

    # --- 2019 ---
    # News Hour
    "EURSEK_20190213_0800.bi5", "EURSEK_20190425_0700.bi5", 
    "EURSEK_20190703_0700.bi5", "EURSEK_20190905_0700.bi5", 
    "EURSEK_20191024_0800.bi5", "EURSEK_20191219_0800.bi5",
    # Placebo Hour (-2h)
    "EURSEK_20190213_0600.bi5", "EURSEK_20190425_0500.bi5", 
    "EURSEK_20190703_0500.bi5", "EURSEK_20190905_0500.bi5", 
    "EURSEK_20191024_0600.bi5", "EURSEK_20191219_0600.bi5",

    # --- Original N=33 Study Files (News Hour) ---
    "EURSEK_20201126_0800.bi5", "EURSEK_20210210_0800.bi5",
    "EURSEK_20210427_0700.bi5", "EURSEK_20210701_0700.bi5",
    "EURSEK_20210921_0700.bi5", "EURSEK_20211125_0800.bi5",
    "EURSEK_20220210_0800.bi5", "EURSEK_20220428_0700.bi5",
    "EURSEK_20220630_0700.bi5", "EURSEK_20220920_0700.bi5",
    "EURSEK_20221124_0800.bi5", "EURSEK_20230209_0800.bi5",
    "EURSEK_20230426_0700.bi5", "EURSEK_20230629_0700.bi5",
    "EURSEK_20230921_0700.bi5", "EURSEK_20231123_0800.bi5",
    "EURSEK_20240201_0800.bi5", "EURSEK_20240508_0700.bi5",
    "EURSEK_20240627_0700.bi5", "EURSEK_20240820_0700.bi5"
    # --- 2024 (Late) ---
    "EURSEK_20240925_0700.bi5", "EURSEK_20240925_0500.bi5", 
    "EURSEK_20241107_0800.bi5", "EURSEK_20241107_0600.bi5",
    "EURSEK_20241219_0800.bi5", "EURSEK_20241219_0600.bi5",
    # --- 2025 ---
    "EURSEK_20250206_0800.bi5", "EURSEK_20250206_0600.bi5",
    "EURSEK_20250327_0700.bi5", "EURSEK_20250327_0500.bi5",
    "EURSEK_20250514_0700.bi5", "EURSEK_20250514_0500.bi5",
    "EURSEK_20250624_0700.bi5", "EURSEK_20250624_0500.bi5",
    "EURSEK_20250820_0700.bi5", "EURSEK_20250820_0500.bi5",
    "EURSEK_20250925_0700.bi5", "EURSEK_20250925_0500.bi5",
    "EURSEK_20251106_0800.bi5", "EURSEK_20251106_0600.bi5",
    "EURSEK_20251218_0800.bi5", "EURSEK_20251218_0600.bi5",
    # --- 2026 ---
    "EURSEK_20260205_0800.bi5", "EURSEK_20260205_0600.bi5",
    "EURSEK_20260326_0700.bi5", "EURSEK_20260326_0500.bi5"
]

def download_dukascopy_bi5(filename: str, target_dir: Path):
    """
    Downloads a .bi5 file directly from the Dukascopy CDN.
    Filename format expected: EURSEK_YYYYMMDD_HH00.bi5
    """
    # 1. Parse the filename
    symbol = filename[:6]  # EURSEK
    year = filename[7:11]  # e.g., 2023
    
    # QUANT SECRET: Dukascopy server months are 0-indexed! (00 = January)
    real_month = int(filename[11:13])
    dukascopy_month = str(real_month - 1).zfill(2) 
    
    day = filename[13:15]  # e.g., 23
    hour = filename[16:18] # e.g., 08
    
    # 2. Construct the URL to the broker's hidden CDN
    url = f"https://datafeed.dukascopy.com/datafeed/{symbol}/{year}/{dukascopy_month}/{day}/{hour}h_ticks.bi5"
    
    print(f"[INFO] Requesting from CDN: {url}")
    
    # Add User-Agent to bypass basic anti-bot firewall rules
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    # 3. Execute the download request
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        file_path = target_dir / filename
        with open(file_path, 'wb') as f:
            f.write(response.content)
        print(f"[SUCCESS] Downloaded: {file_path}")
    else:
        print(f"[ERROR] Failed to fetch {filename}. HTTP Status: {response.status_code}")

if __name__ == "__main__":
    # Ensure the target directory exists
    target_directory = Path("data/raw")
    target_directory.mkdir(parents=True, exist_ok=True)
    
    print("Initializing Dukascopy Institutional Data Downloader...")
    
    for file_name in MISSING_FILES:
        # Check if file already exists to save bandwidth and prevent re-downloading
        if (target_directory / file_name).exists():
            print(f"[SKIP] File already exists: {file_name}")
            continue
            
        download_dukascopy_bi5(file_name, target_directory)
        # Enforce a 1-second delay to prevent IP rate-limiting
        time.sleep(1)
        
    print("Data acquisition complete. Please refresh the Streamlit dashboard.")