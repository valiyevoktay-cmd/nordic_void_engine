import pandas as pd
import numpy as np
from pathlib import Path
import sys

def main():
    # --- 1. Path Configuration ---
    script_path = Path(__file__).resolve()
    base_path = script_path.parent
    data_dir = base_path / "data"
    input_path = data_dir / "final_regression_input.csv"
    vstoxx_path = data_dir / "VSTOXX.csv"

    if not input_path.exists() or not vstoxx_path.exists():
        print(f"[CRITICAL] Data files not found in {data_dir}")
        sys.exit(1)

    # --- 2. Data Ingestion ---
    df = pd.read_csv(input_path)
    vstoxx = pd.read_csv(vstoxx_path)
    
    # Cleaning dates to avoid UserWarnings
    df['Date'] = pd.to_datetime(df['Date'])
    df['Merge_Date'] = df['Date'].dt.normalize()
    
    vstoxx['Date'] = pd.to_datetime(vstoxx['Date'], dayfirst=False, errors='coerce')
    vstoxx = vstoxx.dropna(subset=['Date'])
    vstoxx['Merge_Date'] = vstoxx['Date'].dt.normalize()

    # Dynamic column detection (finding the price column)
    price_col = next((c for c in vstoxx.columns if 'PRICE' in c.upper() or 'LAST' in c.upper()), vstoxx.columns[1])
    print(f"[INFO] Using '{price_col}' as the VSTOXX price source.")

    # --- 3. Merge & Feature Creation ---
    df_final = pd.merge(df, vstoxx[['Merge_Date', price_col]], on='Merge_Date', how='inner')
    df_final.rename(columns={price_col: 'VSTOXX_T_minus_1'}, inplace=True)

    # Creating the shock flag (10bps threshold)
    df_final['Is_Big_Surprise'] = (df_final['Policy_Surprise'].abs() >= 0.10).astype(int)

    # --- 4. The Reviewer Audit ---
    shock_counts = df_final['Is_Big_Surprise'].value_counts()
    shock_percentage = df_final['Is_Big_Surprise'].value_counts(normalize=True) * 100

    print("\n" + "="*45)
    print(" POLICY SURPRISE DISTRIBUTION AUDIT ")
    print("="*45)
    print(f"Total Observations (N): {len(df_final)}")
    
    num_shocks = shock_counts.get(1, 0)
    num_normal = shock_counts.get(0, 0)
    
    print(f"Normal Events (< 10bps):  {num_normal} ({shock_percentage.get(0, 0):.1f}%)")
    print(f"Big Surprise (>= 10bps): {num_shocks} ({shock_percentage.get(1, 0):.1f}%)")

    # VSTOXX Analysis within groups
    print("\n" + "="*45)
    print(" VSTOXX VARIANCE BY REGIME ")
    print("="*45)
    vstoxx_stats = df_final.groupby('Is_Big_Surprise')['VSTOXX_T_minus_1'].agg(['mean', 'std', 'min', 'max'])
    print(vstoxx_stats)

    # Check for collinearity risk
    if num_shocks > 0:
        correlation = df_final['Is_Big_Surprise'].corr(df_final['VSTOXX_T_minus_1'])
        print(f"\n[DIAGNOSTIC] Correlation (Shock vs VSTOXX): {correlation:.4f}")

if __name__ == "__main__":
    main()