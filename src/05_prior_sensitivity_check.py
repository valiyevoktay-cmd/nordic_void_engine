import pymc as pm
import arviz as az
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
import sys

def main():
    # --- 1. Infrastructure & Path Configuration ---
    script_path = Path(__file__).resolve()
    # Assuming script is in /src and data is in /data
    base_path = script_path.parent.parent
    data_dir = base_path / "data"
    input_path = data_dir / "final_regression_input.csv"
    
    if not input_path.exists():
        print(f"[CRITICAL] Registry not found at: {input_path}")
        sys.exit(1)

    # --- 2. Data Preparation ---
    df = pd.read_csv(input_path) 
    df['Date'] = pd.to_datetime(df['Date'])
    
    # Ensure binary 'Detected' column is synchronized
    if 'Detected' not in df.columns:
        df['Detected'] = (df['Penalty (BPS)'] > 0).astype(int)

    # Filtering targeted macroeconomic epochs
    reg_c = df[(df['Date'].dt.year >= 2022) & (df['Date'].dt.year <= 2024)]['Detected']
    reg_d = df[(df['Date'].dt.year >= 2025) & (df['Date'].dt.year <= 2026)]['Detected']

    n_c, k_c = len(reg_c), int(reg_c.sum())
    n_d, k_d = len(reg_d), int(reg_d.sum())

    print("\n" + "="*70)
    print(" BAYESIAN SENSITIVITY STRESS TEST: INITIALIZING ")
    print("="*70)
    print(f"Regime C (Target): N={n_c}, Vacuums={k_c}")
    print(f"Regime D (Normalization): N={n_d}, Vacuums={k_d}")

    # --- 3. Stressed Bayesian Architecture ---
    with pm.Model() as sensitivity_model:
        # Objective Prior for Regime C
        theta_c = pm.Beta("theta_C", alpha=1, beta=1)
        
        # PESSIMISTIC PRIOR for Regime D
        # Anchoring the model to historical ZIRP failure rates (Regime A: 24/36)
        # alpha=24 (expected failures), beta=12 (expected successes)
        theta_d_pessimistic = pm.Beta("theta_D_pessimistic", alpha=24, beta=12)

        # Binomial Likelihoods
        pm.Binomial("obs_C", n=n_c, p=theta_c, observed=k_c)
        pm.Binomial("obs_D", n=n_d, p=theta_d_pessimistic, observed=k_d)

        # Calculating the delta under adversarial conditions
        delta = pm.Deterministic("delta_C_minus_D_stressed", theta_c - theta_d_pessimistic)

        # --- 4. MCMC Sampling (NUTS) ---
        print("\n[INFO] Sampling under pessimistic constraints...")
        idata = pm.sample(draws=2000, tune=1000, chains=4, random_seed=42, progressbar=True)

    # --- 5. Diagnostic Analysis ---
    print("\n" + "="*70)
    print(" POSTERIOR SUMMARY: STRESSED MODEL ")
    print("="*70)
    summary = az.summary(idata, var_names=["theta_C", "theta_D_pessimistic", "delta_C_minus_D_stressed"], hdi_prob=0.94)
    print(summary[['mean', 'sd', 'hdi_3%', 'hdi_97%', 'r_hat']])

    # Visualizing the distribution of the difference
    az.plot_posterior(idata, var_names=["delta_C_minus_D_stressed"], ref_val=0, hdi_prob=0.94)
    plt.title("Bayesian Sensitivity: Regime C vs D (Pessimistic Prior: Beta(24,12))")
    plt.tight_layout()
    plt.show()

if __name__ == "__main__":
    main()