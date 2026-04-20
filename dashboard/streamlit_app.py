import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from pathlib import Path
import sys
from datetime import datetime

# Inject src directory into system path
project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))

from src.pipeline import Bi5Parser
from src.engine import MicrostructureEngine

# --- 0. Global Event Registry (Expanded N=69) ---
RIKSBANK_EVENTS = [
    # --- 2014 ---
    pd.Timestamp("2014-02-13 08:30:00"), pd.Timestamp("2014-04-09 07:30:00"),
    pd.Timestamp("2014-07-03 07:30:00"), pd.Timestamp("2014-09-04 07:30:00"),
    pd.Timestamp("2014-10-28 08:30:00"), pd.Timestamp("2014-12-16 08:30:00"),
    # --- 2015 ---
    pd.Timestamp("2015-02-12 08:30:00"), pd.Timestamp("2015-04-29 07:30:00"),
    pd.Timestamp("2015-07-02 07:30:00"), pd.Timestamp("2015-09-03 07:30:00"),
    pd.Timestamp("2015-10-28 08:30:00"), pd.Timestamp("2015-12-15 08:30:00"),
    # --- 2016 ---
    pd.Timestamp("2016-02-11 08:30:00"), pd.Timestamp("2016-04-21 07:30:00"),
    pd.Timestamp("2016-07-06 07:30:00"), pd.Timestamp("2016-09-07 07:30:00"),
    pd.Timestamp("2016-10-27 08:30:00"), pd.Timestamp("2016-12-21 08:30:00"),
    # --- 2017 ---
    pd.Timestamp("2017-02-15 08:30:00"), pd.Timestamp("2017-04-27 07:30:00"),
    pd.Timestamp("2017-07-04 07:30:00"), pd.Timestamp("2017-09-07 07:30:00"),
    pd.Timestamp("2017-10-26 08:30:00"), pd.Timestamp("2017-12-20 08:30:00"),
    # --- 2018 ---
    pd.Timestamp("2018-02-14 08:30:00"), pd.Timestamp("2018-04-26 07:30:00"),
    pd.Timestamp("2018-07-03 07:30:00"), pd.Timestamp("2018-09-06 07:30:00"),
    pd.Timestamp("2018-10-24 08:30:00"), pd.Timestamp("2018-12-20 08:30:00"),
    # --- 2019 ---
    pd.Timestamp("2019-02-13 08:30:00"), pd.Timestamp("2019-04-25 07:30:00"),
    pd.Timestamp("2019-07-03 07:30:00"), pd.Timestamp("2019-09-05 07:30:00"),
    pd.Timestamp("2019-10-24 08:30:00"), pd.Timestamp("2019-12-19 08:30:00"),
    # --- Original 2020-2026 ---
    pd.Timestamp("2020-11-26 08:30:00"), pd.Timestamp("2021-02-10 08:30:00"),
    pd.Timestamp("2021-04-27 07:30:00"), pd.Timestamp("2021-07-01 07:30:00"),
    pd.Timestamp("2021-09-21 07:30:00"), pd.Timestamp("2021-11-25 08:30:00"),
    pd.Timestamp("2022-02-10 08:30:00"), pd.Timestamp("2022-04-28 07:30:00"),
    pd.Timestamp("2022-06-30 07:30:00"), pd.Timestamp("2022-09-20 07:30:00"),
    pd.Timestamp("2022-11-24 08:30:00"), pd.Timestamp("2023-02-09 08:30:00"),
    pd.Timestamp("2023-04-26 07:30:00"), pd.Timestamp("2023-06-29 07:30:00"),
    pd.Timestamp("2023-09-21 07:30:00"), pd.Timestamp("2023-11-23 08:30:00"),
    pd.Timestamp("2024-02-01 08:30:00"), pd.Timestamp("2024-05-08 07:30:00"),
    pd.Timestamp("2024-06-27 07:30:00"), pd.Timestamp("2024-08-20 07:30:00"),
    pd.Timestamp("2024-09-25 07:30:00"), pd.Timestamp("2024-11-07 08:30:00"),
    pd.Timestamp("2024-12-19 08:30:00"), pd.Timestamp("2025-02-06 08:30:00"),
    pd.Timestamp("2025-03-27 07:30:00"), pd.Timestamp("2025-05-14 07:30:00"),
    pd.Timestamp("2025-06-24 07:30:00"), pd.Timestamp("2025-08-20 07:30:00"),
    pd.Timestamp("2025-09-25 07:30:00"), pd.Timestamp("2025-11-06 08:30:00"),
    pd.Timestamp("2025-12-18 08:30:00"), pd.Timestamp("2026-02-05 08:30:00"),
    pd.Timestamp("2026-03-26 07:30:00"),
]

# --- 1. Institutional UI Configuration & CSS Injection ---
st.set_page_config(
    page_title="Liquidity Vacuum Analysis", 
    layout="wide", 
    initial_sidebar_state="expanded"
)

def apply_institutional_theme():
    st.markdown("""
    <style>
        .stApp { background-color: #0b0e14; color: #d1d4dc; }
        #MainMenu {visibility: hidden;}
        footer {visibility: hidden;}
        header {background-color: transparent !important;}
        .block-container { padding-top: 2rem !important; max-width: 95% !important; }
        h1, h2, h3 { font-family: sans-serif; font-weight: 400; color: #ffffff; }
        .academic-meta { font-family: monospace; font-size: 0.85rem; color: #8b93a6; margin-bottom: 2rem; border-bottom: 1px solid #2a2e39; padding-bottom: 10px; }
        div[data-testid="metric-container"] { background-color: #131722; border: 1px solid #2a2e39; padding: 15px 20px; border-radius: 4px; border-left: 3px solid #3b82f6; }
        div[data-testid="metric-container"] label { color: #8b93a6; font-size: 0.75rem !important; text-transform: uppercase; }
        div[data-testid="metric-container"] div[data-testid="stMetricValue"] { font-family: monospace; color: #e0e3eb; font-size: 1.8rem; }
    </style>
    """, unsafe_allow_html=True)

apply_institutional_theme()

# --- 2. Interactive Sidebar ---
st.sidebar.markdown("### ⚙️ Engine Parameters")

# NEW: Dynamic Event Selector
selected_event = st.sidebar.selectbox(
    "Select Riksbank Event", 
    options=RIKSBANK_EVENTS,
    format_func=lambda x: x.strftime('%Y-%m-%d %H:%M UTC')
)

z_thresh = st.sidebar.slider("Z-Score Anomaly Threshold", 1.5, 10.0, 6.0, 0.1)
window_sec = st.sidebar.slider("Rolling Baseline Window (sec)", 10, 120, 60, 10)
st.sidebar.markdown("---")
st.sidebar.caption("Adjusting parameters recalculates the HFT withdrawal timestamp and Implicit Slippage Tax in real-time.")

# --- 3. Dynamic Data Processing Pipeline ---
@st.cache_data
def load_and_process_data(event_ts, z_val, win_val):
    # Dynamic path construction based on selected timestamp
    target_hour = event_ts.replace(minute=0, second=0)
    file_name = f"EURSEK_{target_hour.strftime('%Y%m%d_%H00')}.bi5"
    file_path = project_root / "data" / "raw" / file_name
    
    if not file_path.exists():
        return None, event_ts, None, 0.0, 0.0

    df_ticks = Bi5Parser.parse_file(file_path, target_hour)
    df_stats = MicrostructureEngine.compute_spread_dynamics(df_ticks, window=f'{win_val}s')
    
    # Slice window for visualization
    focus_window = df_stats.loc[event_ts - pd.Timedelta(seconds=300) : event_ts + pd.Timedelta(seconds=60)]
    
    # Detect vacuum
    vacuum_start = MicrostructureEngine.detect_vacuum(focus_window.loc[:event_ts], z_threshold=z_val)
    
    slippage_bps, slippage_sek = 0.0, 0.0
    if vacuum_start:
        baseline_start = vacuum_start - pd.Timedelta(seconds=30)
        baseline_slice = df_stats.loc[baseline_start : vacuum_start]
        if not baseline_slice.empty:
            baseline_mid = (baseline_slice['ask'].mean() + baseline_slice['bid'].mean()) / 2
            slippage_bps, slippage_sek = MicrostructureEngine.calculate_implicit_slippage(df_stats, vacuum_start, baseline_mid)
    
    return focus_window, event_ts, vacuum_start, slippage_bps, slippage_sek

@st.cache_data
def run_aggregate_study(z_val, win_val):
    results = []
    missing_files = []
    
    for event in RIKSBANK_EVENTS:
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
                results.append({"Date": event.strftime("%Y-%m-%d"), "Shock (UTC)": event.strftime("%H:%M:%S"), "Vacuum Trigger": vacuum_start.strftime("%H:%M:%S.%f")[:-3], "Δt (sec)": round(delta_t, 3), "Penalty (BPS)": round(bps, 2), "Loss (SEK)": round(sek, 2), "Detected": True})
            else:
                results.append({"Date": event.strftime("%Y-%m-%d"), "Shock (UTC)": event.strftime("%H:%M:%S"), "Vacuum Trigger": "No Trigger", "Δt (sec)": 0.0, "Penalty (BPS)": 0.0, "Loss (SEK)": 0.0, "Detected": False})
        except Exception: continue
            
    return pd.DataFrame(results), len(RIKSBANK_EVENTS), missing_files

# --- 4. Academic Header ---
st.markdown("<h1>NORDIC VOID | Quantitative Research Environment</h1>", unsafe_allow_html=True)
st.markdown('<div class="academic-meta">High-Frequency Econometrics & Market Microstructure | JEL Codes: E52, G14 | Asset: EUR/SEK</div>', unsafe_allow_html=True)

# Tabs are now dynamically scaling their title based on the length of RIKSBANK_EVENTS
tab1, tab2 = st.tabs(["📉 Single Event Analysis", f"📊 Multi-Event Aggregator (N={len(RIKSBANK_EVENTS)})"])

with tab1:
    with st.spinner("Decoding Level-2 Order Book..."):
        df, event_time, vacuum_start, slippage_bps, slippage_sek = load_and_process_data(selected_event, z_thresh, window_sec)

    if df is not None:
        if vacuum_start:
            delta_t = (event_time - vacuum_start).total_seconds()
            c1, c2, c3, c4, c5 = st.columns(5)
            c1.metric("Shock Time (UTC)", event_time.strftime("%H:%M:%S"))
            c2.metric("Vacuum Delta (Δt)", f"{delta_t:.2f} s")
            c3.metric("Execution Penalty", f"{slippage_bps:.2f} BPS", delta="Adverse Selection", delta_color="inverse")
            c4.metric("Loss / 1M SEK", f"{int(slippage_sek):,} SEK")
            c5.metric("Tick Density", f"{len(df):,} updates")
        else:
            st.warning(f"No statistical anomaly detected with Z > {z_thresh} for this event.")

        # Plotly WebGL Chart
        fig = go.Figure()
        fig.add_trace(go.Scattergl(x=df.index, y=df['ask'], mode='lines', name='Ask Price', line=dict(color='#ff3366', width=1.2)))
        fig.add_trace(go.Scattergl(x=df.index, y=df['bid'], mode='lines', name='Bid Price', line=dict(color='#00d2ff', width=1.2)))
        
        fig.add_vline(x=event_time, line_width=1.5, line_color="#ffffff")
        fig.add_annotation(x=event_time, y=1.02, yref="paper", text="Riksbank Announcement", showarrow=False, font=dict(color="#ffffff"))

        if vacuum_start:
            fig.add_vline(x=vacuum_start, line_width=1.5, line_dash="dot", line_color="#f5a623")
            fig.add_annotation(x=vacuum_start, y=0.02, yref="paper", text="Algorithm Withdrawal", showarrow=False, font=dict(color="#f5a623"))

        fig.update_layout(height=650, paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', margin=dict(l=10, r=10, t=10, b=10), hovermode="x unified")
        fig.update_xaxes(showgrid=True, gridcolor='#1f2937', tickfont=dict(color="#8b93a6"))
        fig.update_yaxes(showgrid=True, gridcolor='#1f2937', tickfont=dict(color="#8b93a6"), tickformat=".5f")
        st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})
    else:
        st.error(f"Missing data file for the selected date: {selected_event.strftime('%Y-%m-%d')}")

with tab2:
    st.markdown("### Multi-Event Statistical Proof")
    
    # --- NEW: Dynamic Regime Filter ---
    regime_filter = st.radio(
        "Filter by Macroeconomic Regime:",
        options=[
            "All History (2014-2026)", 
            "Regime A: NIRP / ZIRP Era (2014-2019)", 
            "Regime B: Pandemic & Early Shock (2020-2021)",
            "Regime C: High-Inflation (2022-2024)",
            "Regime D: Normalization (2025-2026)"
        ],
        horizontal=True
    )

    with st.spinner(f"Processing N={len(RIKSBANK_EVENTS)} study..."):
        results_df, total_events, missing = run_aggregate_study(z_thresh, window_sec)
        
    if not results_df.empty:
        # --- NEW: Apply Filter Logic ---
        temp_df = results_df.copy()
        temp_df['Year'] = pd.to_datetime(temp_df['Date']).dt.year
        
        if "2014-2019" in regime_filter:
            filtered_df = temp_df[temp_df['Year'] <= 2019].drop(columns=['Year'])
        elif "2020-2021" in regime_filter:
            filtered_df = temp_df[(temp_df['Year'] >= 2020) & (temp_df['Year'] <= 2021)].drop(columns=['Year'])
        elif "2022-2024" in regime_filter:
            filtered_df = temp_df[(temp_df['Year'] >= 2022) & (temp_df['Year'] <= 2024)].drop(columns=['Year'])
        elif "2025-2026" in regime_filter:
            filtered_df = temp_df[temp_df['Year'] >= 2025].drop(columns=['Year'])
        else:
            filtered_df = temp_df.drop(columns=['Year'])
            
        current_events_count = len(filtered_df)

        if current_events_count > 0:
            detected_df = filtered_df[filtered_df["Detected"] == True]
            hit_rate = (len(detected_df) / current_events_count) * 100
            
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Processed Events", f"{current_events_count} / {total_events}")
            c2.metric("Vacuum Hit Rate", f"{hit_rate:.1f}%")
            c3.metric("Mean Withdrawal (Δt)", f"{detected_df['Δt (sec)'].mean():.2f} s" if not detected_df.empty else "N/A")
            c4.metric("Mean Penalty", f"{detected_df['Penalty (BPS)'].mean():.2f} BPS" if not detected_df.empty else "N/A", delta="Avg Tax", delta_color="inverse")
            
            st.dataframe(filtered_df, use_container_width=True, hide_index=True)
        else:
            st.warning("No data available for the selected regime.")