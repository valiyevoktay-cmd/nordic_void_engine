# Nordic Void: Event-Driven Infrastructure for High-Frequency Microstructure Research

<p align="left">
  <img src="https://img.shields.io/badge/Python-3.10+-3776AB?style=for-the-badge&logo=python&logoColor=white" />
  <img src="https://img.shields.io/badge/PyMC-MCMC-orange?style=for-the-badge" />
  <img src="https://img.shields.io/badge/Streamlit-FF4B4B?style=for-the-badge&logo=streamlit&logoColor=white" />
  <img src="https://img.shields.io/badge/License-MIT-green?style=for-the-badge" />
</p>

> A specialized analytical engine for **millisecond-level quantification of liquidity vacuum events** in foreign exchange markets. Processes vectorized Top-of-Book tick data to isolate microstructural decay surrounding Swedish Riksbank and Norges Bank interest rate announcements. Backed by a published SSRN research paper.

📄 **Paper:** [*The Riksbank Liquidity Vacuum: High-Frequency Evidence of Monetary Policy Transmission Friction*](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=6771542) — Valiyev, O. (2026)

---

##  Core Research Objective

The engine identifies **preemptive algorithmic liquidity withdrawal** preceding Riksbank and Norges Bank interest rate announcements. By evaluating EUR/SEK and EUR/NOK across a longitudinal sample (2014–2026, ~12 years of millisecond tick data), it maps the deterministic phase transition from baseline market resiliency to structural failure.

**Key empirical result:** Market makers withdraw a statistically significant portion of Top-of-Book depth in the 30–120 seconds preceding scheduled policy announcements, quantified via a calibrated 5.3-sigma anomaly threshold.

---

##  Mathematical Specification

### Liquidity Vacuum Detection

$$Slippage_{bps} = \left( \frac{Ask_{shock} - Mid_{baseline}}{Mid_{baseline}} \right) \times 10{,}000$$

Where $Mid_{baseline}$ is the median bid-ask midpoint over a 60-second rolling window preceding the algorithmic trigger.

**Z-score anomaly threshold:**

$$Z = \frac{QuoteDensity_t - \mu_{rolling}}{\sigma_{rolling}} \quad \Rightarrow \quad \text{Vacuum triggered if } Z > 5.3$$

The 5.3-sigma threshold is empirically calibrated to filter Eurozone noise and isolate endogenous Riksbank signals.

### Variance Stabilization

Inverse Hyperbolic Sine (IHS) transformation handles heavy-tailed spread distributions and zero-value instances:

$$IHS(x) = \ln\left(x + \sqrt{x^2 + 1}\right)$$

---

##  Inferential Framework

Standard frequentist asymptotics are insufficient for constrained intraday samples near policy events. The engine uses Bayesian MCMC methods throughout:

| Method | Purpose |
| :--- | :--- |
| **Beta-Binomial** (PyMC) | Models latent probability of liquidity failure across announcement windows |
| **Gaussian Mixture + NUTS** | Confirms bimodal nature of market collapse (pre-event vs. post-event regimes) |
| **PSIS-LOO** (ArviZ) | Pareto Smoothed Importance Sampling LOO cross-validation rejects linear scaling models |

---

## ⚙️ Architecture

```
nordic_void_engine/
├── src/
│   ├── engine.py                  # Vectorized Z-score vacuum detector, slippage TCA
│   ├── ingester.py                # Binary .bi5 tick file parser (Dukascopy format)
│   ├── pipeline.py                # Master execution pipeline
│   ├── downloader.py              # Dukascopy tick data downloader
│   ├── 01_eurnok_preprocessing.py # FX tick cleaning & resampling
│   ├── 02_multiple_regression.py  # Statistical regression pipeline
│   ├── 03_add_policy_surprise.py  # Macro shock data integration
│   ├── 04_bayesic_basic.py        # PyMC Bayesian model specifications
│   ├── 05_model_comparison.py     # PSIS-LOO cross-validation
│   └── statistical_tests.py       # Hypothesis testing routines
├── dashboard/
│   └── streamlit_app.py           # WebGL Streamlit dashboard for microstructure auditing
├── notebooks/                     # Bayesian MCMC sampling & Posterior Predictive Checks
├── data/                          # Compressed millisecond tick datasets (.bi5)
├── config.yaml                    # Configuration settings
└── requirements.txt
```

---

##  Quick Start

```bash
# 1. Clone the repository
git clone https://github.com/valiyevoktay-cmd/nordic_void_engine.git
cd nordic_void_engine

# 2. Install dependencies
pip install -r requirements.txt

# 3. Download tick data (Dukascopy EUR/SEK, specify date range in config.yaml)
python download_data.py

# 4. Run the full analytical pipeline
python src/pipeline.py

# 5. Launch the interactive dashboard
streamlit run dashboard/streamlit_app.py
```

---

##  Tech Stack

| Layer | Technology |
| :--- | :--- |
| Language | Python 3.10+ |
| Data Processing | Pandas, NumPy (vectorized, C-optimized backends) |
| Bayesian Inference | PyMC, ArviZ, NUTS sampler |
| Visualization | Streamlit, Plotly (WebGL) |
| Data Source | Dukascopy millisecond tick data (.bi5 binary format) |
| Statistical Tests | `scipy.stats`, custom hypothesis testing |

---

## 📜 Academic Citation

If using this infrastructure for research, please cite:

> Valiyev, O. (2026). *The Riksbank Liquidity Vacuum: High-Frequency Evidence of Monetary Policy Transmission Friction*. SSRN Working Paper. https://papers.ssrn.com/sol3/papers.cfm?abstract_id=6771542

---

## 📜 License

MIT License — open-source for academic replication and peer review. See [LICENSE](LICENSE) for details.
