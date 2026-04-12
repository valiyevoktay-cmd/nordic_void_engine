import pandas as pd
from pathlib import Path
from datetime import datetime
from src.pipeline import Bi5Parser
from src.engine import MicrostructureEngine

# 1. Define paths and target time
project_root = Path(__file__).resolve().parent
file_path = project_root / "data" / "raw" / "EURSEK_20230921_0700.bi5"
target_hour_utc = datetime(2023, 9, 21, 7, 0, 0)

print("Parsing .bi5 file...")
df_ticks = Bi5Parser.parse_file(file_path, target_hour_utc)

# 2. Calibration: Increase the rolling window to establish a stable "normal" baseline
print("Calculating Spread Dynamics (60s baseline)...")
# Using a 60-second window so the mean doesn't adapt too quickly to the vacuum
df_stats = MicrostructureEngine.compute_spread_dynamics(df_ticks, window='60s')

# 3. Detect the Vacuum
event_time = pd.Timestamp("2023-09-21 07:30:00+00:00").tz_localize(None)

# Calibration: Look at the 5 minutes (300 seconds) prior to the announcement
pre_event_window = df_stats.loc[event_time - pd.Timedelta(seconds=300) : event_time]

# Calibration: Lower Z-threshold to a standard 3.0 (99.7% confidence interval)
z_thresh = 3.0
vacuum_start = MicrostructureEngine.detect_vacuum(pre_event_window, z_threshold=z_thresh)

if vacuum_start:
    print(f"\n[ALERT] Liquidity Vacuum Detected!")
    print(f"Spread deviated by > {z_thresh} Std Devs at exactly: {vacuum_start}")
    delta_t = (event_time - vacuum_start).total_seconds()
    print(f"Market Makers withdrew {delta_t} seconds BEFORE the central bank announcement.")
else:
    print("\nStill no vacuum detected. We need visual inspection.")