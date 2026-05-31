# Nordic Void: Event-Driven Infrastructure for High-Frequency Microstructure Research

Nordic Void is a specialized analytical engine designed for the millisecond-level quantification of liquidity vacuum events in foreign exchange markets. The architecture executes vectorized processing of high-fidelity Top-of-Book tick data to isolate microstructural decay surrounding scheduled monetary policy shocks.

## Core Research Objective

The engine identifies preemptive algorithmic liquidity withdrawal preceding Swedish Riksbank interest rate announcements. By evaluating the EUR/SEK currency pair across a longitudinal sample (2014–2026), the framework maps the deterministic phase transition from baseline market resiliency to structural failure. 

## Technical Architecture

The pipeline avoids iterative loops to mitigate computational latency. It relies on C-optimized backends for localized window calculations.

* **Data Ingestion:** Vectorized ingestion of compressed binary tick files (.bi5) with millisecond precision.
* **Anomaly Detection:** An empirically calibrated 5.3-sigma threshold ($Z > 5.3$) isolates endogenous signals from Eurozone noise. 
* **Transaction Cost Analysis (TCA):** Calculation of the Top-of-Book Execution Penalty as a quoted shadow cost. 
* **Variance Stabilization:** Inverse Hyperbolic Sine (IHS) transformation handles heavy-tailed distributions and zero-value instances.

## Mathematical Specification

The engine quantifies liquidity impairment through the following slippage metric:

$$Slippage_{bps} = \left( \frac{Ask_{shock} - Mid_{baseline}}{Mid_{baseline}} \right) \times 10000$$

$Mid_{baseline}$ is strictly defined as the median bid-ask midpoint across a 60-second rolling window preceding the algorithmic trigger. 

## Inferential Framework

The analytical core integrates Bayesian Markov Chain Monte Carlo (MCMC) methods to bypass the limitations of standard frequentist asymptotics in constrained samples. 

* **Regime Evaluation:** A Bayesian Beta-Binomial framework models the latent probability of liquidity failure.
* **Phase Transition Mapping:** A Bayesian Gaussian Mixture Model via the No-U-Turn Sampler (NUTS) confirms the bimodal nature of market collapse.
* **Model Selection:** Pareto Smoothed Importance Sampling Leave-One-Out (PSIS-LOO) cross-validation mathematically rejects linear scaling models.

## Project Structure

* `data/`: Raw and synchronized millisecond tick datasets.
* `src/`: Core Python 3.10+ analytical scripts and vectorized modules.
* `notebooks/`: Bayesian MCMC sampling routines and Posterior Predictive Checks.
* `interface/`: WebGL-powered Streamlit environment for microstructural auditing.

## Academic Citation

If utilizing this infrastructure for research, please cite the primary study:

> [Valiyev, O. (2026). *The Riksbank Liquidity Vacuum: High-Frequency Evidence of Monetary Policy Transmission Friction*.](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=6771542)

## License

This project is open-source and intended for academic replication purposes. Source code and environment configurations are available for peer review.
