import pandas as pd
import numpy as np
import pymc as pm
import pytensor.tensor as pt
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

    df_final = pd.merge(df, vstoxx[['Merge_Date', 'VSTOXX_T_minus_1']], on='Merge_Date', how='inner')
    
    # IHS Transformation
    df_final['IHS_Penalty_BPS'] = np.arcsinh(df_final['Penalty (BPS)'])
    df_final['Is_Thursday'] = (df_final['Date'].dt.dayofweek == 3).astype(int)
    df_final['Is_Big_Surprise'] = (df_final['Policy_Surprise'].abs() >= 0.10).astype(int)

    # STEP 1: Data Refinement (Handling Missing Values & Scaling)
    regression_cols = ['IHS_Penalty_BPS', 'VSTOXX_T_minus_1', 'EURNOK_Penalty_BPS', 'Is_Big_Surprise']
    df_final = df_final.dropna(subset=regression_cols).copy()

    for col in ['VSTOXX_T_minus_1', 'EURNOK_Penalty_BPS']:
        df_final[f'{col}_std'] = (df_final[col] - df_final[col].mean()) / df_final[col].std()

    # --- STEP 2: Bayesian Mixture Model Specification ---
    y_obs = df_final['IHS_Penalty_BPS'].values
    vstoxx_std = df_final['VSTOXX_T_minus_1_std'].values
    eurnok_std = df_final['EURNOK_Penalty_BPS_std'].values
    thursday = df_final['Is_Thursday'].values
    big_surprise = df_final['Is_Big_Surprise'].values
    interaction = vstoxx_std * big_surprise

    with pm.Model() as mixture_model:
        # Priors for the 'Nordic Void' Regime
        intercept = pm.Normal("intercept", mu=1.3, sigma=0.5)
        b_vstoxx = pm.Normal("b_vstoxx", mu=0, sigma=0.5)
        b_eurnok = pm.Normal("b_eurnok", mu=0, sigma=0.5)
        b_thursday = pm.Normal("b_thursday", mu=0, sigma=0.5)
        b_surprise = pm.Normal("b_surprise", mu=0, sigma=1.0)
        b_interaction = pm.Normal("b_interaction", mu=0.8, sigma=0.5)
        
        # Regression Logic for Regime 1 (Collapse Mode)
        mu_vacuum = (intercept + 
                     b_vstoxx * vstoxx_std + 
                     b_eurnok * eurnok_std + 
                     b_thursday * thursday + 
                     b_surprise * big_surprise + 
                     b_interaction * interaction)
        
        # Regime 0 (Normal Market) - Tight prior around zero
        mu_normal_scalar = pm.Normal("mu_normal_base", mu=0, sigma=0.1)
        mu_normal = pt.full(y_obs.shape, mu_normal_scalar)
        
        # --- ИДЕАЛЬНОЕ РАЗДЕЛЕНИЕ ДИСПЕРСИИ (ANTI-LABEL SWITCHING) ---
        # Нормальный режим: жесткий лимит на микро-волатильность
        sigma_normal = pm.Uniform("sigma_normal", lower=0.01, upper=0.3)
        # Вакуум: жесткий лимит на макро-волатильность
        sigma_vacuum = pm.Uniform("sigma_vacuum", lower=0.6, upper=3.0)
        
        sigma_mixed = pt.stack([sigma_normal, sigma_vacuum], axis=-1)
        
        # Weight w: probability of being in 'Vacuum' state
        w = pm.Beta("w", alpha=2, beta=2)
        
        # Correctly aligned Mixture Distribution
        pm.NormalMixture("y_obs", 
                         w=pt.stack([1-w, w], axis=-1), 
                         mu=pt.stack([mu_normal, mu_vacuum], axis=-1), 
                         sigma=sigma_mixed, 
                         observed=y_obs)

        print("\n[INFO] Bayesian Mixture Model Initialized (Normal vs Void regimes).")

        # --- STEP 3: MCMC Sampling (NUTS Algorithm) ---
        print("[INFO] Executing NUTS Sampler (Draws: 2000, Tune: 1000, Chains: 4)...")
        inference_data = pm.sample(draws=2000, tune=1000, chains=4, random_seed=42, target_accept=0.95)

        # SAVING:
        inference_data.to_netcdf(data_dir / "idata_mixture.nc")
        print("[INFO] Mixture Inference Data saved to disk.")
        
        # --- STEP 4: Predictive Sampling ---
        print("[INFO] Generating Posterior Predictive Samples...")
        pm.sample_posterior_predictive(inference_data, extend_inferencedata=True)

    # --- STEP 5: Convergence Diagnostics ---
    print("\n" + "="*80)
    print(" BAYESIAN MIXTURE POSTERIOR SUMMARY & DIAGNOSTICS")
    print("="*80)
    summary = az.summary(inference_data, var_names=["w", "b_interaction", "intercept", "mu_normal_base"])
    print(summary[['mean', 'sd', 'hdi_3%', 'hdi_97%', 'ess_bulk', 'r_hat']])

    # Trace plots to audit 'fuzzy caterpillar' convergence
    az.plot_trace(inference_data, var_names=['b_interaction', 'w'])
    plt.tight_layout()
    plt.show()

    # --- STEP 6: Posterior Distribution Analysis ---
    print("\n[INFO] Plotting Posterior for the Interaction Term...")
    az.plot_posterior(
        inference_data, 
        var_names=['b_interaction'], 
        ref_val=0,
        hdi_prob=0.94
    )
    plt.title("Posterior Probability: Interaction Effect (Corrected for Two Regimes)")
    plt.show()

    # --- STEP 7: Posterior Predictive Check (PPC) ---
    print("\n[INFO] Plotting Mixture PPC (Resolving the Twin Peaks)...")
    az.plot_ppc(inference_data, num_pp_samples=100)
    plt.title("Mixture PPC: Model Fit vs Multi-Modal Market Reality")
    plt.show()

if __name__ == "__main__":
    main()