import pandas as pd
import numpy as np
import pymc as pm
import pytensor.tensor as pt
import arviz as az
from pathlib import Path
import sys
import warnings

# Отключаем предупреждения PyTensor для чистоты вывода
warnings.filterwarnings("ignore")

def clean_numeric(series):
    if series.dtype == 'object':
        return pd.to_numeric(series.str.replace(',', '').str.replace(' ', ''), errors='coerce')
    return series

def main():
    # --- 1. Data Ingestion & Pre-processing (Идентично базовому скрипту) ---
    script_path = Path(__file__).resolve()
    base_path = script_path.parent.parent
    data_dir = base_path / "data"
    
    input_path = data_dir / "final_regression_input.csv"
    vstoxx_path = data_dir / "VSTOXX.csv"

    if not input_path.exists() or not vstoxx_path.exists():
        print(f"[CRITICAL] Data files not found.")
        sys.exit(1)

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
    
    df_final['IHS_Penalty_BPS'] = np.arcsinh(df_final['Penalty (BPS)'])
    df_final['Is_Thursday'] = (df_final['Date'].dt.dayofweek == 3).astype(int)
    df_final['Is_Big_Surprise'] = (df_final['Policy_Surprise'].abs() >= 0.10).astype(int)

    regression_cols = ['IHS_Penalty_BPS', 'VSTOXX_T_minus_1', 'EURNOK_Penalty_BPS', 'Is_Big_Surprise']
    df_final = df_final.dropna(subset=regression_cols).copy()

    for col in ['VSTOXX_T_minus_1', 'EURNOK_Penalty_BPS']:
        df_final[f'{col}_std'] = (df_final[col] - df_final[col].mean()) / df_final[col].std()

    y_obs = df_final['IHS_Penalty_BPS'].values
    vstoxx_std = df_final['VSTOXX_T_minus_1_std'].values
    eurnok_std = df_final['EURNOK_Penalty_BPS_std'].values
    thursday = df_final['Is_Thursday'].values
    big_surprise = df_final['Is_Big_Surprise'].values
    interaction = vstoxx_std * big_surprise

    # --- 2. BASELINE MODEL: Continuous OLS (Байесовский эквивалент) ---
    print("\n[INFO] Compiling Model 1: Continuous Linear OLS...")
    with pm.Model() as model_linear:
        intercept = pm.Normal("intercept", mu=0, sigma=1)
        b_vstoxx = pm.Normal("b_vstoxx", mu=0, sigma=1)
        b_eurnok = pm.Normal("b_eurnok", mu=0, sigma=1)
        b_thursday = pm.Normal("b_thursday", mu=0, sigma=1)
        b_surprise = pm.Normal("b_surprise", mu=0, sigma=1)
        b_interaction = pm.Normal("b_interaction", mu=0, sigma=1)
        
        mu_lin = (intercept + b_vstoxx * vstoxx_std + b_eurnok * eurnok_std + 
                  b_thursday * thursday + b_surprise * big_surprise + b_interaction * interaction)
        sigma_lin = pm.Exponential("sigma", lam=1.0)
        
        pm.Normal("y_obs", mu=mu_lin, sigma=sigma_lin, observed=y_obs)
        
        idata_linear = pm.sample(draws=2000, tune=1000, chains=4, random_seed=42, progressbar=False)
        pm.compute_log_likelihood(idata_linear) # <-- КРИТИЧЕСКИЙ ШАГ ДЛЯ LOO

    # --- 3. ADVANCED MODEL: Bimodal Mixture 
    print("\n[INFO] Compiling Model 2: Bimodal Gaussian Mixture...")
    with pm.Model() as model_mixture:
        intercept = pm.Normal("intercept", mu=1.3, sigma=0.5)
        b_vstoxx = pm.Normal("b_vstoxx", mu=0, sigma=0.5)
        b_eurnok = pm.Normal("b_eurnok", mu=0, sigma=0.5)
        b_thursday = pm.Normal("b_thursday", mu=0, sigma=0.5)
        b_surprise = pm.Normal("b_surprise", mu=0, sigma=1.0)
        b_interaction = pm.Normal("b_interaction", mu=0.8, sigma=0.5)
        
        mu_vacuum = (intercept + b_vstoxx * vstoxx_std + b_eurnok * eurnok_std + 
                     b_thursday * thursday + b_surprise * big_surprise + b_interaction * interaction)
        
        mu_normal_scalar = pm.Normal("mu_normal_base", mu=0, sigma=0.1)
        mu_normal = pt.full(y_obs.shape, mu_normal_scalar)
        
        sigma_normal = pm.Uniform("sigma_normal", lower=0.01, upper=0.3)
        sigma_vacuum = pm.Uniform("sigma_vacuum", lower=0.6, upper=3.0)
        sigma_mixed = pt.stack([sigma_normal, sigma_vacuum], axis=-1)
        
        w = pm.Beta("w", alpha=2, beta=2)
        
        pm.NormalMixture("y_obs", 
                         w=pt.stack([1-w, w], axis=-1), 
                         mu=pt.stack([mu_normal, mu_vacuum], axis=-1), 
                         sigma=sigma_mixed, 
                         observed=y_obs)
        
        idata_mixture = pm.sample(draws=2000, tune=1000, chains=4, random_seed=42, target_accept=0.95, progressbar=False)
        pm.compute_log_likelihood(idata_mixture) # <-- КРИТИЧЕСКИЙ ШАГ ДЛЯ LOO

    # --- 4. PSIS-LOO Cross-Validation ---
    print("\n" + "="*80)
    print(" PSIS-LOO CROSS-VALIDATION RESULTS")
    print("="*80)
    
    comparison_dict = {
        "Bimodal_Mixture": idata_mixture,
        "Continuous_OLS": idata_linear
    }
    
    # Comparing models using PSIS-LOO (Pareto Smoothed Importance Sampling Leave-One-Out)
    model_comparison = az.compare(comparison_dict, ic="loo")
    
    # Table
    print(model_comparison[['rank', 'elpd_loo', 'p_loo', 'weight']])

if __name__ == "__main__":
    main()