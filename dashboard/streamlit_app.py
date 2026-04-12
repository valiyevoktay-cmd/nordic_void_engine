import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from pathlib import Path
import sys
from datetime import datetime

# Inject src directory into system path so Streamlit can find our modules
project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))

from src.pipeline import Bi5Parser
from src.engine import MicrostructureEngine

# --- 1. Institutional UI Configuration & CSS Injection ---
st.set_page_config(
    page_title="Liquidity Vacuum Analysis", 
    layout="wide", 
    initial_sidebar_state="expanded"
)

def apply_institutional_theme():
    """Injects custom CSS to override default Streamlit 'toy' aesthetics."""
    st.markdown("""
    <style>
        /* Deep Dark Backgrounds */
        .stApp {
            background-color: #0b0e14;
            color: #d1d4dc;
        }
        
        /* Stealth Mode: Hide Streamlit UI elements but keep sidebar toggle */
        #MainMenu {visibility: hidden;}
        footer {visibility: hidden;}
        header {background-color: transparent !important;}
        
        /* Layout Optimization */
        .block-container {
            padding-top: 2rem !important;
            padding-bottom: 1rem !important;
            max-width: 95% !important;
        }
        
        /* Typography: Clean Headers */
        h1, h2, h3 {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;
            font-weight: 400;
            color: #ffffff;
            letter-spacing: -0.5px;
        }
        
        /* Typography: Subtitles and Academic Metadata */
        .academic-meta {
            font-family: 'SFMono-Regular', Consolas, 'Liberation Mono', Menlo, monospace;
            font-size: 0.85rem;
            color: #8b93a6;
            margin-bottom: 2rem;
            border-bottom: 1px solid #2a2e39;
            padding-bottom: 10px;
        }
        
        /* Metric Cards: Bloomberg Terminal Aesthetic */
        div[data-testid="metric-container"] {
            background-color: #131722;
            border: 1px solid #2a2e39;
            padding: 15px 20px;
            border-radius: 4px;
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.2);
            border-left: 3px solid #3b82f6; /* Subtle accent */
        }
        div[data-testid="metric-container"] label {
            color: #8b93a6;
            font-size: 0.75rem !important;
            text-transform: uppercase;
            letter-spacing: 1px;
        }
        div[data-testid="metric-container"] div[data-testid="stMetricValue"] {
            font-family: 'SFMono-Regular', Consolas, 'Liberation Mono', Menlo, monospace;
            color: #e0e3eb;
            font-size: 1.8rem;
            font-weight: 500;
        }
        
        /* Delta Value styling */
        div[data-testid="stMetricDelta"] svg {
            display: none; /* Hide the generic arrow */
        }
    </style>
    """, unsafe_allow_html=True)

apply_institutional_theme()

# --- 2. Interactive Sidebar ---
st.sidebar.markdown("### ⚙️ Engine Parameters")
z_thresh = st.sidebar.slider("Z-Score Anomaly Threshold", min_value=1.5, max_value=5.0, value=3.0, step=0.1)
window_sec = st.sidebar.slider("Rolling Baseline Window (sec)", min_value=10, max_value=120, value=60, step=10)
st.sidebar.markdown("---")
st.sidebar.caption("Adjusting parameters recalculates the HFT withdrawal timestamp and Implicit Slippage Tax in real-time.")

# --- 3. Dynamic Data Processing Pipeline ---
@st.cache_data
def load_and_process_data(z_val, win_val):
    """
    Cached execution of the Microstructure Engine.
    Computes fair-value baseline and Transaction Cost Analysis (TCA).
    """
    file_path = project_root / "data" / "raw" / "EURSEK_20230921_0700.bi5"
    target_hour_utc = datetime(2023, 9, 21, 7, 0, 0)
    
    # Graceful degradation if the primary file is missing
    if not file_path.exists():
        return None, None, None, 0.0, 0.0

    df_ticks = Bi5Parser.parse_file(file_path, target_hour_utc)
    
    # Inject dynamic window
    df_stats = MicrostructureEngine.compute_spread_dynamics(df_ticks, window=f'{win_val}s')
    
    event_time = pd.Timestamp("2023-09-21 07:30:00+00:00").tz_localize(None)
    focus_window = df_stats.loc[event_time - pd.Timedelta(seconds=300) : event_time + pd.Timedelta(seconds=60)]
    
    # Inject dynamic Z-score threshold
    vacuum_start = MicrostructureEngine.detect_vacuum(
        focus_window.loc[:event_time], 
        z_threshold=z_val
    )
    
    # --- TCA Integration & Baseline Calculation ---
    slippage_bps, slippage_sek = 0.0, 0.0
    if vacuum_start:
        # Define the baseline window: 30 seconds of stable data ending right before the vacuum trigger
        baseline_start = vacuum_start - pd.Timedelta(seconds=30)
        baseline_slice = df_stats.loc[baseline_start : vacuum_start]
        
        if not baseline_slice.empty:
            # Baseline MidPrice = Average(Bid + Ask) / 2
            baseline_mid = (baseline_slice['ask'].mean() + baseline_slice['bid'].mean()) / 2
            
            # Run TCA Engine
            slippage_bps, slippage_sek = MicrostructureEngine.calculate_implicit_slippage(
                df_stats, vacuum_start, baseline_mid
            )
    
    return focus_window, event_time, vacuum_start, slippage_bps, slippage_sek

@st.cache_data
def run_aggregate_study(z_val, win_val):
    """Module 5: Event Study Aggregator."""
    events = [
        pd.Timestamp("2023-09-21 07:30:00"),
        pd.Timestamp("2023-11-23 08:30:00"),
        pd.Timestamp("2024-02-01 08:30:00"),
        pd.Timestamp("2024-05-08 07:30:00"),
        pd.Timestamp("2024-06-27 07:30:00")
    ]
    
    results = []
    missing_files = []
    
    for event in events:
        target_hour = event.replace(minute=0, second=0)
        file_name = f"EURSEK_{target_hour.strftime('%Y%m%d_%H00')}.bi5"
        file_path = project_root / "data" / "raw" / file_name
        
        if not file_path.exists():
            missing_files.append(file_name)
            continue
            
        try:
            df_ticks = Bi5Parser.parse_file(file_path, target_hour)
            df_stats = MicrostructureEngine.compute_spread_dynamics(df_ticks, window=f'{win_val}s')
            focus_window = df_stats.loc[event - pd.Timedelta(seconds=300) : event]
            
            vacuum_start = MicrostructureEngine.detect_vacuum(focus_window, z_threshold=z_val)
            
            if vacuum_start:
                baseline_slice = df_stats.loc[vacuum_start - pd.Timedelta(seconds=30) : vacuum_start]
                baseline_mid = (baseline_slice['ask'].mean() + baseline_slice['bid'].mean()) / 2 if not baseline_slice.empty else 0
                
                bps, sek = MicrostructureEngine.calculate_implicit_slippage(df_stats, vacuum_start, baseline_mid)
                delta_t = (event - vacuum_start).total_seconds()
                
                results.append({
                    "Date": event.strftime("%Y-%m-%d"),
                    "Shock (UTC)": event.strftime("%H:%M:%S"),
                    "Vacuum Trigger": vacuum_start.strftime("%H:%M:%S.%f")[:-3],
                    "Δt (sec)": round(delta_t, 3),
                    "Penalty (BPS)": round(bps, 2),
                    "Loss (SEK)": round(sek, 2),
                    "Detected": True
                })
        except Exception:
            continue
            
    return pd.DataFrame(results), len(events), missing_files

# --- 4. Academic Header ---
st.markdown("<h1>The Riksbank Liquidity Vacuum</h1>", unsafe_allow_html=True)
st.markdown(
    '<div class="academic-meta">High-Frequency Econometrics & Market Microstructure | JEL Codes: E52, G14 | Asset: EUR/SEK</div>', 
    unsafe_allow_html=True
)

tab1, tab2 = st.tabs(["📉 Single Event Analysis", "📊 Multi-Event Aggregator (N=5)"])

with tab1:
    with st.spinner("Decoding Level-2 Order Book..."):
        # Returns 5 values to populate the TCA metrics
        df, event_time, vacuum_start, slippage_bps, slippage_sek = load_and_process_data(z_thresh, window_sec)

    if df is not None:
        # --- 5. Quant Metrics (TCA Row) ---
        if vacuum_start:
            delta_t = (event_time - vacuum_start).total_seconds()
            col1, col2, col3, col4, col5 = st.columns(5)
            
            col1.metric("Shock Time (UTC)", event_time.strftime("%H:%M:%S"))
            col2.metric("Vacuum Delta (Δt)", f"{delta_t:.2f} s")
            
            # Implicit Costs (TCA)
            col3.metric("Execution Penalty", f"{slippage_bps:.2f} BPS", delta="Adverse Selection", delta_color="inverse")
            col4.metric("Loss / 1M SEK", f"{int(slippage_sek):,} SEK")
            col5.metric("Tick Density", f"{len(df):,} updates")
        else:
            st.warning(f"No statistical anomaly detected with Z > {z_thresh} in the current window.")

        st.markdown("<br>", unsafe_allow_html=True)

        # --- 6. High-Resolution WebGL Charting ---
        fig = go.Figure()

        # Minimalist traces (WebGL for performance)
        fig.add_trace(go.Scattergl(
            x=df.index, y=df['ask'], 
            mode='lines', name='Ask Price', 
            line=dict(color='#ff3366', width=1.2),
            opacity=0.9
        ))

        fig.add_trace(go.Scattergl(
            x=df.index, y=df['bid'], 
            mode='lines', name='Bid Price', 
            line=dict(color='#00d2ff', width=1.2),
            opacity=0.9
        ))

        # 1. Macro Event Line & Annotation (Decoupled Fix for Pandas Timestamps)
        fig.add_vline(x=event_time, line_width=1.5, line_dash="solid", line_color="#ffffff")
        fig.add_annotation(
            x=event_time, y=1.02, yref="paper", 
            text="Riksbank Rate Announcement",
            showarrow=False,
            font=dict(color="#ffffff", size=11, family="sans-serif"),
            xanchor="left", yanchor="bottom"
        )

        # 2. Vacuum Trigger Line & Annotation (Decoupled Fix for Pandas Timestamps)
        if vacuum_start:
            fig.add_vline(x=vacuum_start, line_width=1.5, line_dash="dot", line_color="#f5a623")
            fig.add_annotation(
                x=vacuum_start, y=0.02, yref="paper", 
                text="Algorithm Withdrawal",
                showarrow=False,
                font=dict(color="#f5a623", size=11, family="sans-serif"),
                xanchor="right", yanchor="bottom"
            )

        # Strict Chart Layout
        fig.update_layout(
            height=650,
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            margin=dict(l=10, r=10, t=10, b=10),
            legend=dict(
                orientation="h",
                yanchor="bottom", y=1.02,
                xanchor="right", x=1,
                font=dict(color="#8b93a6", size=12)
            ),
            hovermode="x unified",
            hoverlabel=dict(
                bgcolor="#131722",
                font_size=13,
                font_family="monospace"
            )
        )

        fig.update_xaxes(
            showgrid=True, gridwidth=1, gridcolor='#1f2937',
            showline=True, linewidth=1, linecolor='#2a2e39',
            tickfont=dict(color="#8b93a6", family="monospace"),
        )

        fig.update_yaxes(
            title_text="EUR/SEK Exchange Rate",
            title_font=dict(color="#8b93a6", size=12),
            showgrid=True, gridwidth=1, gridcolor='#1f2937',
            showline=True, linewidth=1, linecolor='#2a2e39',
            tickfont=dict(color="#8b93a6", family="monospace"),
            tickformat=".5f"
        )

        st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})
    else:
         st.error("Missing initial data file: EURSEK_20230921_0700.bi5")

with tab2:
    st.markdown("### Multi-Event Statistical Proof")
    st.markdown("Aggregating microstructure dynamics across multiple historical Riksbank rate announcements to prove systemic algorithmic withdrawal.")
    
    with st.spinner("Processing historical events..."):
        results_df, total_events, missing = run_aggregate_study(z_thresh, window_sec)
        
    if not results_df.empty:
        detected_df = results_df[results_df["Detected"] == True]
        hit_rate = (len(detected_df) / total_events) * 100
        
        # Aggregate Metrics
        st.markdown("<br>", unsafe_allow_html=True)
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Processed Events", f"{len(results_df)} / {total_events}")
        c2.metric("Vacuum Hit Rate", f"{hit_rate:.1f}%")
        c3.metric("Mean Withdrawal Time (Δt)", f"{detected_df['Δt (sec)'].mean():.2f} s")
        c4.metric("Mean Execution Penalty", f"{detected_df['Penalty (BPS)'].mean():.2f} BPS", delta="Avg Tax", delta_color="inverse")
        
        st.markdown("<br>", unsafe_allow_html=True)
        st.dataframe(results_df, use_container_width=True, hide_index=True)
        
    if missing:
        st.info(f"**Research Note:** To complete the N={total_events} study, please download the following Dukascopy `.bi5` files into your `data/raw/` directory:\n" + 
                "\n".join([f"- `{m}`" for m in missing]))