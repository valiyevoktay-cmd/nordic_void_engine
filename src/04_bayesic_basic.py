import pandas as pd
import numpy as np
import bambi as bmb
import arviz as az
import matplotlib.pyplot as plt
from pathlib import Path
import sys

def clean_numeric(series):
    """
    Cleans financial string data by removing commas/spaces 
    and converting to float.
    """
    if series.dtype == 'object':
        return pd.to_numeric(series.str.replace(',', '').str.replace(' ', ''), errors='coerce')
    return series

def main():
    # --- Path Configuration ---
    script_path = Path(__file__).resolve()
    base_path = script_path.parent.parent
    data_dir = base_path / "data"
    
    input_path = data_dir / "final_regression_input.csv"
    vstoxx_path = data_dir / "VSTOXX.csv"

    if not input_path.exists() or not vstoxx_path.exists():
        print(f"[CRITICAL] Data files not found in {data_dir}")
        sys.exit(1)

    # --- 1. Data Ingestion & Pre-processing ---
    df = pd.read_csv(input_path)
    df['Date'] = pd.to_datetime(df['Date'])
    df['Merge_Date'] = df['Date'].dt.normalize()

    vstoxx = pd.read_csv(vstoxx_path)
    vstoxx['Date'] = pd.to_datetime(vstoxx['Date'], dayfirst=False, errors='coerce')
    vstoxx = vstoxx.sort_values('Date').dropna(subset=['Date'])
    
    price_col = next((c for c in vstoxx.columns if 'PRICE' in c.upper()), vstoxx.columns[1])
    vstoxx['VSTOXX_T_minus_1'] = clean_numeric(vstoxx[price_col]).shift(1)
    vstoxx['Merge_Date'] = vstoxx['Date'].dt.normalize()

    # Inner join to ensure temporal synchronization
    df_final = pd.merge(df, vstoxx[['Merge_Date', 'VSTOXX_T_minus_1']], on='Merge_Date', how='inner')
    
    # IHS Transformation (Inverse Hyperbolic Sine) to handle heavy-tailed microstructure data
    df_final['IHS_Penalty_BPS'] = np.arcsinh(df_final['Penalty (BPS)'])
    df_final['Is_Thursday'] = (df_final['Date'].dt.dayofweek == 3).astype(int)
    
    # Binary indicator for shocks exceeding the 10bps threshold (Using absolute surprise)
    df_final['Is_Big_Surprise'] = (df_final['Policy_Surprise'].abs() >= 0.10).astype(int)

    # --- STEP 1: Data Refinement (Handling Missing Values & Scaling) ---
    regression_cols = ['IHS_Penalty_BPS', 'VSTOXX_T_minus_1', 'EURNOK_Penalty_BPS', 'Is_Big_Surprise']
    df_final = df_final.dropna(subset=regression_cols).copy()

    # Z-score standardization for continuous variables (Crucial for NUTS sampler stability)
    for col in ['VSTOXX_T_minus_1', 'EURNOK_Penalty_BPS']:
        df_final[f'{col}_std'] = (df_final[col] - df_final[col].mean()) / df_final[col].std()

    # --- STEP 2: Bayesian Model Specification ---
    formula = "IHS_Penalty_BPS ~ VSTOXX_T_minus_1_std * Is_Big_Surprise + EURNOK_Penalty_BPS_std + Is_Thursday"
    
    model = bmb.Model(formula, df_final)
    print("\n[INFO] Bayesian Threshold Interaction Model Initialized.")

    # --- STEP 3: MCMC Sampling (NUTS Algorithm) ---
    print("[INFO] Executing NUTS Sampler (Draws: 2000, Tune: 1000, Chains: 4)...")
    inference_data = model.fit(draws=2000, tune=1000, chains=4, random_seed=42)

    # SAVING:
    inference_data.to_netcdf(data_dir / "idata_linear.nc")
    print("[INFO] Linear Inference Data saved to disk.")

    # --- STEP 4: Convergence Diagnostics ---
    print("\n" + "="*80)
    print(" BAYESIAN POSTERIOR SUMMARY & DIAGNOSTICS")
    print("="*80)
    
    try:
        response_var = model.response_component.name
    except AttributeError:
        response_var = "IHS_Penalty_BPS" 

    posterior_vars = [v for v in inference_data.posterior.data_vars if v != response_var]
    summary = az.summary(inference_data, var_names=posterior_vars)
    print(summary[['mean', 'sd', 'hdi_3%', 'hdi_97%', 'ess_bulk', 'r_hat']])

    # Trace plots to audit 'fuzzy caterpillar' convergence
    az.plot_trace(inference_data, var_names=['VSTOXX_T_minus_1_std:Is_Big_Surprise'])
    plt.tight_layout()
    plt.show()

    # --- STEP 5: Posterior Distribution Analysis ---
    print("\n[INFO] Plotting Posterior for the Interaction Term...")
    az.plot_posterior(
        inference_data, 
        var_names=['VSTOXX_T_minus_1_std:Is_Big_Surprise'], 
        ref_val=0,
        hdi_prob=0.94
    )
    plt.title("Posterior Probability: Interaction Effect (Surprise * Global Volatility)")
    plt.show()

    # --- STEP 6: Posterior Predictive Check (PPC) ---
    print("\n[INFO] Generating Posterior Predictive Samples...")
    # Generate predictions based on the posterior distribution
    model.predict(inference_data, kind="pps")
    
    print("[INFO] Plotting PPC (Observed vs Predicted Data Structure)...")
    # Plot observed data vs simulated data from the model
    az.plot_ppc(inference_data, num_pp_samples=100)
    plt.title("Posterior Predictive Check: Model Fit vs Real-World Data")
    plt.show()

if __name__ == "__main__":
    main()