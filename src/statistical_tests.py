import pandas as pd
from scipy import stats
import numpy as np
from pathlib import Path
import sys

# --- 1. Path Configuration ---
# Resolves the absolute path dynamically regardless of execution directory
script_path = Path(__file__).resolve()
base_path = script_path.parent.parent
data_dir = base_path / "data"

# Target the main events registry, NOT the raw VSTOXX index.
# The VSTOXX file does not contain the Riksbank event hit rates.
input_path = data_dir / "final_regression_input.csv" 

if not input_path.exists():
    # Fallback to the raw export if the regression input is missing
    input_path = data_dir / "2026-05-07T11-14_export.csv"
    if not input_path.exists():
        print(f"[CRITICAL] Main event registry not found in {data_dir}")
        sys.exit(1)

# --- 2. Data Ingestion & Pre-processing ---
df = pd.read_csv(input_path) 
df['Date'] = pd.to_datetime(df['Date'])

# Dynamically generate the 'Detected' binary column if it doesn't exist
if 'Detected' not in df.columns:
    if 'Penalty (BPS)' in df.columns:
        # A penalty > 0 indicates a liquidity vacuum was detected
        df['Detected'] = (df['Penalty (BPS)'] > 0).astype(int)
        print(f"[INFO] Processing {input_path.name}")
        print("[INFO] 'Detected' column generated dynamically from 'Penalty (BPS)'.")
    else:
        print("[CRITICAL] Column 'Detected' or 'Penalty (BPS)' not found in dataset.")
        sys.exit(1)

# --- 3. Isolate regimes based on external chronological boundaries ---
regime_a = df[(df['Date'].dt.year >= 2014) & (df['Date'].dt.year <= 2019)]
regime_c = df[(df['Date'].dt.year >= 2022) & (df['Date'].dt.year <= 2024)]
regime_d = df[(df['Date'].dt.year >= 2025) & (df['Date'].dt.year <= 2026)]

# Extract binary hit arrays (1 for vacuum, 0 for stable)
hit_a = regime_a['Detected'].astype(int)
hit_c = regime_c['Detected'].astype(int)
hit_d = regime_d['Detected'].astype(int)

# --- 4. Perform Fisher's Exact Test for Binary Proportions ---
from scipy.stats import fisher_exact

def get_contingency_table(group1, group2):
    # Builds a 2x2 table: [[Vacuum_1, Stable_1], [Vacuum_2, Stable_2]]
    return [
        [group1.sum(), len(group1) - group1.sum()],
        [group2.sum(), len(group2) - group2.sum()]
    ]

odds_ca, p_ca = fisher_exact(get_contingency_table(hit_c, hit_a), alternative='two-sided')
odds_cd, p_cd = fisher_exact(get_contingency_table(hit_c, hit_d), alternative='two-sided')
odds_ad, p_ad = fisher_exact(get_contingency_table(hit_a, hit_d), alternative='two-sided')

# --- 5. Output Results ---
print("\n=== FISHER'S EXACT TEST MATRIX (CATEGORICAL) ===")
print(f"Regime A (ZIRP) Hit Rate:        {(hit_a.mean()*100):.1f}% (N={len(regime_a)})")
print(f"Regime C (Inflation) Hit Rate:   {(hit_c.mean()*100):.1f}% (N={len(regime_c)})")
print(f"Regime D (Normalization) Rate:   {(hit_d.mean()*100):.1f}% (N={len(regime_d)})")
print("-" * 55)
print(f"[A vs C] ZIRP vs Inflation   -> Odds Ratio: {odds_ca:.2f} | P-value: {p_ca:.5f}")
print(f"[C vs D] Inflation vs Normal -> Odds Ratio: {odds_cd:.2f} | P-value: {p_cd:.5f}")
print(f"[A vs D] ZIRP vs Normal      -> Odds Ratio: {odds_ad:.2f} | P-value: {p_ad:.5f}")