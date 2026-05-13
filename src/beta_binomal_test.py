import pymc as pm
import arviz as az
import pandas as pd
import numpy as np
from pathlib import Path
import sys
import matplotlib.pyplot as plt

def main():
    # --- 1. Path Configuration & Data Ingestion ---
    script_path = Path(__file__).resolve()
    base_path = script_path.parent.parent
    data_dir = base_path / "data"
    input_path = data_dir / "final_regression_input.csv"

    if not input_path.exists():
        print(f"[CRITICAL] Input data not found at {input_path}")
        sys.exit(1)

    df = pd.read_csv(input_path) 
    df['Date'] = pd.to_datetime(df['Date'])
    
    # Ensure binary target exists
    if 'Detected' not in df.columns:
        df['Detected'] = (df['Penalty (BPS)'] > 0).astype(int)

    # --- 2. Isolate Regimes and Aggregate Counts ---
    # Regime A: ZIRP/NIRP (2014-2019)
    reg_a = df[(df['Date'].dt.year >= 2014) & (df['Date'].dt.year <= 2019)]['Detected']
    # Regime C: High Inflation (2022-2024)
    reg_c = df[(df['Date'].dt.year >= 2022) & (df['Date'].dt.year <= 2024)]['Detected']
    # Regime D: Normalization (2025-2026)
    reg_d = df[(df['Date'].dt.year >= 2025) & (df['Date'].dt.year <= 2026)]['Detected']

    # Build discrete contingency dictionary: n = trials, k = successes (vacuums)
    data_matrix = {
        "A": {"n": len(reg_a), "k": int(reg_a.sum())},
        "C": {"n": len(reg_c), "k": int(reg_c.sum())},
        "D": {"n": len(reg_d), "k": int(reg_d.sum())}
    }

    print("\n[INFO] Regime Data Aggregation:")
    for regime, counts in data_matrix.items():
        rate = (counts['k'] / counts['n']) * 100 if counts['n'] > 0 else 0
        print(f"  Regime {regime}: N={counts['n']}, Vacuums={counts['k']} ({rate:.1f}%)")

    # --- 3. Bayesian Model Architecture ---
    print("\n[INFO] Compiling Beta-Binomial Hierarchical Model...")
    with pm.Model() as bb_model:
        # Priors: Uninformative Beta (Uniform distribution bounded 0 to 1)
        theta_a = pm.Beta("theta_A", alpha=1, beta=1)
        theta_c = pm.Beta("theta_C", alpha=1, beta=1)
        theta_d = pm.Beta("theta_D", alpha=1, beta=1)

        # Likelihoods: Binomial representation of the limit order book failures
        pm.Binomial("obs_A", n=data_matrix["A"]["n"], p=theta_a, observed=data_matrix["A"]["k"])
        pm.Binomial("obs_C", n=data_matrix["C"]["n"], p=theta_c, observed=data_matrix["C"]["k"])
        pm.Binomial("obs_D", n=data_matrix["D"]["n"], p=theta_d, observed=data_matrix["D"]["k"])

        # Deterministics: Structural Phase Transitions (Deltas)
        pm.Deterministic("delta_C_minus_A", theta_c - theta_a) # Escalation effect
        pm.Deterministic("delta_C_minus_D", theta_c - theta_d) # Normalization recovery
        pm.Deterministic("delta_A_minus_D", theta_a - theta_d) # Long-term stabilization vs baseline

        # --- 4. MCMC Sampling ---
        print("[INFO] Executing NUTS Sampler (2000 draws, 1000 tune)...")
        idata = pm.sample(draws=2000, tune=1000, chains=4, random_seed=42, target_accept=0.95, progressbar=True)

    # --- 5. Diagnostic Output & Visualization ---
    print("\n" + "="*80)
    print(" BAYESIAN POSTERIOR SUMMARY (BETA-BINOMIAL)")
    print("="*80)
    
    # Target specific variables for the summary table
    target_vars = ["theta_A", "theta_C", "theta_D", "delta_C_minus_A", "delta_C_minus_D", "delta_A_minus_D"]
    summary = az.summary(idata, var_names=target_vars, hdi_prob=0.94, kind="stats")
    print(summary)

    # Generate High-Resolution Posterior Plots
    print("\n[INFO] Generating Posterior Difference Plots...")
    fig, axes = plt.subplots(1, 3, figsize=(18, 5))
    
    az.plot_posterior(idata, var_names=["delta_C_minus_A"], ref_val=0, hdi_prob=0.94, ax=axes[0])
    axes[0].set_title("Escalation: Regime C vs A")
    
    az.plot_posterior(idata, var_names=["delta_C_minus_D"], ref_val=0, hdi_prob=0.94, ax=axes[1])
    axes[1].set_title("Normalization: Regime C vs D")
    
    az.plot_posterior(idata, var_names=["delta_A_minus_D"], ref_val=0, hdi_prob=0.94, ax=axes[2])
    axes[2].set_title("Stabilization: Regime A vs D")

    plt.tight_layout()
    plt.show()

if __name__ == "__main__":
    main()