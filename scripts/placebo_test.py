import pandas as pd
import sys
from pathlib import Path

# Project root integration
project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))

from src.pipeline import Bi5Parser
from src.engine import MicrostructureEngine

# Dataset: Identical 20 dates, shifted -2 hours (Placebo Window)
placebo_schedule = [
    (pd.Timestamp("2020-11-26 08:30:00"), pd.Timestamp("2020-11-26 06:30:00")),
    (pd.Timestamp("2021-02-10 08:30:00"), pd.Timestamp("2021-02-10 06:30:00")),
    (pd.Timestamp("2021-04-27 07:30:00"), pd.Timestamp("2021-04-27 05:30:00")),
    (pd.Timestamp("2021-07-01 07:30:00"), pd.Timestamp("2021-07-01 05:30:00")),
    (pd.Timestamp("2021-09-21 07:30:00"), pd.Timestamp("2021-09-21 05:30:00")),
    (pd.Timestamp("2021-11-25 08:30:00"), pd.Timestamp("2021-11-25 06:30:00")),
    (pd.Timestamp("2022-02-10 08:30:00"), pd.Timestamp("2022-02-10 06:30:00")),
    (pd.Timestamp("2022-04-28 07:30:00"), pd.Timestamp("2022-04-28 05:30:00")),
    (pd.Timestamp("2022-06-30 07:30:00"), pd.Timestamp("2022-06-30 05:30:00")),
    (pd.Timestamp("2022-09-20 07:30:00"), pd.Timestamp("2022-09-20 05:30:00")),
    (pd.Timestamp("2022-11-24 08:30:00"), pd.Timestamp("2022-11-24 06:30:00")),
    (pd.Timestamp("2023-02-09 08:30:00"), pd.Timestamp("2023-02-09 06:30:00")),
    (pd.Timestamp("2023-04-26 07:30:00"), pd.Timestamp("2023-04-26 05:30:00")),
    (pd.Timestamp("2023-06-29 07:30:00"), pd.Timestamp("2023-06-29 05:30:00")),
    (pd.Timestamp("2023-09-21 07:30:00"), pd.Timestamp("2023-09-21 05:30:00")),
    (pd.Timestamp("2023-11-23 08:30:00"), pd.Timestamp("2023-11-23 06:30:00")),
    (pd.Timestamp("2024-02-01 08:30:00"), pd.Timestamp("2024-02-01 06:30:00")),
    (pd.Timestamp("2024-05-08 07:30:00"), pd.Timestamp("2024-05-08 05:30:00")),
    (pd.Timestamp("2024-06-27 07:30:00"), pd.Timestamp("2024-06-27 05:30:00")),
    (pd.Timestamp("2024-08-20 07:30:00"), pd.Timestamp("2024-08-20 05:30:00")),
    # Новые даты 2024-2026
    (pd.Timestamp("2024-09-25 07:30:00"), pd.Timestamp("2024-09-25 05:30:00")),
    (pd.Timestamp("2024-11-07 08:30:00"), pd.Timestamp("2024-11-07 06:30:00")),
    (pd.Timestamp("2024-12-19 08:30:00"), pd.Timestamp("2024-12-19 06:30:00")),
    (pd.Timestamp("2025-02-06 08:30:00"), pd.Timestamp("2025-02-06 06:30:00")),
    (pd.Timestamp("2025-03-27 07:30:00"), pd.Timestamp("2025-03-27 05:30:00")),
    (pd.Timestamp("2025-05-14 07:30:00"), pd.Timestamp("2025-05-14 05:30:00")),
    (pd.Timestamp("2025-06-24 07:30:00"), pd.Timestamp("2025-06-24 05:30:00")),
    (pd.Timestamp("2025-08-20 07:30:00"), pd.Timestamp("2025-08-20 05:30:00")),
    (pd.Timestamp("2025-09-25 07:30:00"), pd.Timestamp("2025-09-25 05:30:00")),
    (pd.Timestamp("2025-11-06 08:30:00"), pd.Timestamp("2025-11-06 06:30:00")),
    (pd.Timestamp("2025-12-18 08:30:00"), pd.Timestamp("2025-12-18 06:30:00")),
    (pd.Timestamp("2026-02-05 08:30:00"), pd.Timestamp("2026-02-05 06:30:00")),
    (pd.Timestamp("2026-03-26 07:30:00"), pd.Timestamp("2026-03-26 05:30:00"))

]

def run_falsification_study(z_threshold=4):
    print(f"\n{'='*75}")
    print(f"{'ROBUSTNESS CHECK: PLACEBO TEST (N=33)':^75}")
    print(f"{'='*75}")
    print(f"{'Date':<12} | {'Original Shock':<15} | {'Placebo Time':<15} | {'Result'}")
    print(f"{'-'*75}")
    
    false_positives = 0
    total_processed = 0
    
    for original, placebo in placebo_schedule:
        target_hour = placebo.replace(minute=0, second=0)
        file_name = f"EURSEK_{target_hour.strftime('%Y%m%d_%H00')}.bi5"
        file_path = project_root / "data" / "raw" / file_name
        
        if not file_path.exists():
            print(f"{placebo.strftime('%Y-%m-%d'):<12} | {original.strftime('%H:%M:%S'):<15} | {placebo.strftime('%H:%M:%S'):<15} | [FILE MISSING]")
            continue
            
        total_processed += 1
        
        try:
            df_ticks = Bi5Parser.parse_file(file_path, target_hour)
            df_stats = MicrostructureEngine.compute_spread_dynamics(df_ticks, window='60s')
            
            focus_window = df_stats.loc[placebo - pd.Timedelta(seconds=300) : placebo]
            vacuum_start = MicrostructureEngine.detect_vacuum(focus_window, z_threshold=z_threshold)
            
            if vacuum_start:
                false_positives += 1
                status = "FAIL ❌ (Detected)"
            else:
                status = "PASS ✅ (Clear)"
                
            print(f"{placebo.strftime('%Y-%m-%d'):<12} | {original.strftime('%H:%M:%S'):<15} | {placebo.strftime('%H:%M:%S'):<15} | {status}")
            
        except Exception as e:
            print(f"{placebo.strftime('%Y-%m-%d'):<12} | ERROR: {str(e)}")

    hit_rate = (false_positives / total_processed * 100) if total_processed > 0 else 0
    print(f"{'-'*75}")
    print(f"TOTAL SAMPLES PROCESSED: {total_processed}")
    print(f"FALSE POSITIVES FOUND:  {false_positives}")
    print(f"PLACEBO HIT RATE:       {hit_rate:.2f}% (Target: 0.00%)")
    print(f"{'='*75}\n")

if __name__ == "__main__":
    run_falsification_study()