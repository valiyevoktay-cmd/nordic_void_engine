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