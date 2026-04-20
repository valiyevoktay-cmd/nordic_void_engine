# NORDIC VOID | Quantitative Research Environment

Nordic Void is a high-frequency econometrics infrastructure designed to detect, quantify, and visualize systemic liquidity vacuums in the EUR/SEK microstructure during Riksbank monetary policy announcements.

### 1. Executive Summary
The environment isolates endogenous liquidity withdrawal from exogenous macroeconomic noise using an empirically calibrated 5.3-sigma anomaly detection protocol. By analyzing the complete 2014–2026 Riksbank policy cycle ($N=69$), the engine identifies the precise temporal window where market makers retreat from the limit order book (LOB) to mitigate inventory risk.

### 2. Core Methodology
Standard volatility models often fail to distinguish between fundamental price discovery and microstructural decay. Nordic Void addresses this through:

* **Adaptive Thresholding:** Implementation of an instantaneous Z-score filter ($Z>5.3$) relative to a 60-second rolling baseline window.
* **Vectorized Pipeline:** High-throughput processing of Level-2 tick data (.bi5) to calculate slippage taxes and execution penalties.
* **Signal Purification:** Sensitivity analysis proves that a 5.3-sigma threshold suppresses the False Positive Rate (FPR) to 5.80%, effectively filtering out Eurozone spillover effects and early-session institutional order flow.
* **Macroeconomic Regime Filtering:** Segmentation of results based on policy environments (NIRP, High-Inflation, Normalization) to evaluate the causal link between uncertainty and liquidity decay.

### 3. Empirical Key Performance Indicators (KPIs)
Based on the complete longitudinal study of 69 events:

* **Systemic Hit Rate:** 63.8% global vacuum hit rate.
* **Mean Execution Penalty:** 3.85 Basis Points (BPS).
* **Mean Temporal Offset ($\Delta t$):** 91.70 seconds preceding official announcements.
* **Maximum Recorded Friction:** 15.61 BPS (Nov 25, 2021).
* **High-Inflation Peak:** The hit rate scales non-linearly with macroeconomic uncertainty, reaching an 88.2% frequency during high-inflation shocks.

### 4. Technical Architecture
The system is built on a modular Python-based stack for reproducibility:

* **Engine Core:** NumPy/Pandas for vectorized array processing.
* **Visualization:** WebGL-accelerated plotting for millisecond-resolution tick analysis.
* **Frontend:** Streamlit-based dashboard for interactive microstructural auditing.
* **Data Layer:** Automated ingestion and temporal synchronization of high-frequency pricing series.

### 5. Directory Structure
```text
riksbank-liquidity-vacuum/
├── dashboard/          # Streamlit UI, WebGL visualization, and Regime Filtering modules
├── data/               # Processed results, event registries, and N=69 baseline
├── src/                # Core calculation engine and TCA logic
├── scripts/            # Data ingestion and normalization utilities
├── notebooks/          # Exploratory Data Analysis (EDA) and Z-score calibration
└── README.md           # Project documentation
```

### 6. Installation and Usage

Clone the environment:
```bash
git clone [https://github.com/valiyevoktay-cmd/riksbank-liquidity-vacuum.git](https://github.com/valiyevoktay-cmd/riksbank-liquidity-vacuum.git)
cd riksbank-liquidity-vacuum
```

Configure environment:
```bash
pip install -r requirements.txt
```

Launch the research engine:
```bash
streamlit run dashboard/app.py
```

### 7. Author
**Oktay Valiyev** | Quantitative Research & Data Engineering

*Disclaimer: This tool is designed for academic research and microstructural analysis. Historical execution penalties are not predictive of future market liquidity.*
