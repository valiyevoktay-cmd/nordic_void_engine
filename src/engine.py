import pandas as pd
import numpy as np
from typing import Optional, Tuple

class MicrostructureEngine:
    """
    Quantifies high-frequency liquidity dynamics and detects order book anomalies.
    
    Academic Rationale:
    Integrates Z-score anomaly detection with Transaction Cost Analysis (TCA) 
    to prove the economic friction caused by liquidity evaporation. This engine 
    identifies when market makers withdraw from the book, leaving investors 
    vulnerable to adverse selection.
    """

    @staticmethod
    def compute_quote_density(tick_df: pd.DataFrame, bin_size: str = '100ms') -> pd.DataFrame:
        """
        Calculates the number of Level-2 updates per time bin.
        
        A decay in quote density is a secondary signal of market maker withdrawal.
        """
        density = tick_df.resample(bin_size).size().to_frame(name='quote_count')
        return density

    @staticmethod
    def compute_spread_dynamics(tick_df: pd.DataFrame, window: str = '1s') -> pd.DataFrame:
        """
        Calculates rolling statistics for the Bid-Ask spread.
        
        CRITICAL: Preserves all columns (ask, bid) by using .copy() to ensure 
        downstream visualization modules have access to raw quote data.
        """
        stats_df = tick_df.copy()
        
        # Calculate rolling statistics on the spread column
        rolling_window = stats_df['spread'].rolling(window=window)
        stats_df['spread_mean'] = rolling_window.mean()
        stats_df['spread_std'] = rolling_window.std()
        
        return stats_df

    @staticmethod
    def detect_vacuum(stats_df: pd.DataFrame, z_threshold: float = 3.0) -> Optional[pd.Timestamp]:
        """
        Identifies the timestamp where the spread breaches the Z-score threshold.
        
        Standardizes spread expansion relative to its rolling volatility. 
        $$Z = \frac{Spread_t - \mu_{rolling}}{\sigma_{rolling}}$$
        """
        # Replace zero std with NaN to avoid ZeroDivisionError during stale markets
        std_safe = stats_df['spread_std'].replace(0, np.nan)
        z_scores = (stats_df['spread'] - stats_df['spread_mean']) / std_safe
        
        # Detect anomaly trigger
        anomalies = stats_df[z_scores > z_threshold]
        
        if not anomalies.empty:
            return anomalies.index[0]
        return None

    @staticmethod
    def calculate_implicit_slippage(
        tick_df: pd.DataFrame, 
        vacuum_timestamp: pd.Timestamp, 
        baseline_mid: float
    ) -> Tuple[float, float]:
        """
        Quantifies the execution tax (slippage) during the liquidity vacuum.
        
        Rationale:
        Calculates the cost penalty for an urgent buy order at the moment of 
        liquidity evaporation relative to the fair-value baseline.
        
        Formula:
        $$Slippage_{bps} = \frac{Ask_{trigger} - Mid_{baseline}}{Mid_{baseline}} \times 10,000$$
        """
        try:
            # Locate the first available ask price at or after the vacuum trigger
            # We use .iloc[0] to capture the immediate liquidity impact
            ask_at_trigger = tick_df.loc[vacuum_timestamp:].iloc[0]['ask']
            
            if baseline_mid <= 0:
                return 0.0, 0.0

            # Slippage in Basis Points (bps)
            slippage_bps = ((ask_at_trigger - baseline_mid) / baseline_mid) * 10000
            
            # Nominal Loss on a standard 1,000,000 SEK execution size
            nominal_loss = (slippage_bps / 10000) * 1_000_000
            
            return float(slippage_bps), float(nominal_loss)
            
        except (IndexError, KeyError, ZeroDivisionError):
            # Graceful failure for edge cases in HFT data streams
            return 0.0, 0.0

if __name__ == "__main__":
    print("Module 3 (Microstructure Engine) - Global Analytics Core Ready.")