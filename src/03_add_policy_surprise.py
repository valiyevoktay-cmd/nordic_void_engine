import pandas as pd
from pathlib import Path
import numpy as np

def get_riksbank_rate(date, rate_history):
    """Returns the active interest rate for a given date."""
    current_rate = 0.75 # Base rate at the start of 2014
    for change_date, rate in rate_history.items():
        if date >= pd.to_datetime(change_date):
            current_rate = rate
    return current_rate

def main():
    base_path = Path(__file__).resolve().parent.parent
    input_path = base_path / "data" / "final_regression_input.csv"
    
    if not input_path.exists():
        print(f"[ERROR] File not found: {input_path}")
        return

    df = pd.read_csv(input_path)
    df['Date'] = pd.to_datetime(df['Date'])
    
    # Comprehensive Riksbank policy rate history (2014 - 2026)
    rate_changes = {
        '2014-07-03': 0.25,
        '2014-10-28': 0.00,
        '2015-02-12': -0.10,
        '2015-03-18': -0.25,
        '2015-07-02': -0.35,
        '2016-02-11': -0.50,
        '2018-12-20': -0.25,
        '2019-12-19': 0.00,
        '2022-04-28': 0.25,
        '2022-06-30': 0.75,
        '2022-09-20': 1.75,
        '2022-11-24': 2.50,
        '2023-02-09': 3.00,
        '2023-04-26': 3.50,
        '2023-06-29': 3.75,
        '2023-09-21': 4.00,
        '2024-05-08': 3.75,
        '2024-08-20': 3.50,
        '2024-09-25': 3.25,
        '2024-11-07': 2.75,
        '2024-12-19': 2.50,
        # Projections for 2025-2026 easing cycle
        '2025-03-27': 2.25,
        '2025-06-24': 2.00,
        '2025-09-25': 1.75,
        '2026-02-05': 1.50
    }
    
    # Sort history chronologically
    rate_history = dict(sorted(rate_changes.items()))
    
    actual_rates = []
    previous_rates = []
    surprises = []
    
    print("--- INJECTING MACROECONOMIC DATA ---")
    for event_date in df['Date']:
        # Rate BEFORE the meeting (calculated 1 day prior)
        prev_date = event_date - pd.Timedelta(days=1)
        prev_rate = get_riksbank_rate(prev_date, rate_history)
        
        # Rate AFTER the meeting
        actual_rate = get_riksbank_rate(event_date, rate_history)
        
        # Calculate the absolute surprise magnitude
        surprise = abs(actual_rate - prev_rate)
        
        actual_rates.append(actual_rate)
        previous_rates.append(prev_rate)
        surprises.append(surprise)

    # Append new macroeconomic vectors to the dataset
    df['Actual_Rate'] = actual_rates
    df['Previous_Rate'] = previous_rates
    df['Policy_Surprise'] = surprises
    
    # Save the enriched dataset
    df.to_csv(input_path, index=False)
    
    print(f"[SUCCESS] Macro vectors added: Actual_Rate, Previous_Rate, Policy_Surprise.")
    print(f"Total events processed: {len(df)}")
    print(f"Number of rate shocks (absolute change > 0): {sum(1 for s in surprises if s > 0)}")

if __name__ == "__main__":
    main()