import struct
import lzma
import pandas as pd
from pathlib import Path
import numpy as np
import os
import requests
import time

def download_bi5(url, save_path):
    """Downloads .bi5 file from Dukascopy."""
    try:
        response = requests.get(url, timeout=15)
        if response.status_code == 200:
            with open(save_path, 'wb') as f:
                f.write(response.content)
            return True
    except:
        pass
    return False

def process_bi5(file_path):
    with open(file_path, 'rb') as f:
        data = lzma.decompress(f.read())
    ticks = []
    for i in range(0, len(data), 20):
        t, ask, bid, _, _ = struct.unpack('>IIIII', data[i:i+20])
        ticks.append([t, ask / 100000, bid / 100000])
    return pd.DataFrame(ticks, columns=['ms', 'Ask', 'Bid'])

def main():
    base_path = Path(__file__).resolve().parent.parent
    registry_path = base_path / "data" / "2026-05-07T11-14_export.csv"
    raw_dir = base_path / "data" / "raw_nok"
    output_path = base_path / "data" / "final_regression_input.csv"
    
    raw_dir.mkdir(parents=True, exist_ok=True)
    df_events = pd.read_csv(registry_path)
    df_events['Date'] = pd.to_datetime(df_events['Date'])

    nok_penalties = []
    success_count = 0
    
    print(f"--- RECOVERY & DOWNLOAD MODE ---")

    for idx, row in df_events.iterrows():
        event_date = row['Date']
        # Риксбанк вещает в 09:30 CET/CEST. Проверяем 07:00 и 08:00 UTC.
        possible_hours = ["07", "08"]
        file_found = False
        
        for hh in possible_hours:
            filename = f"EURNOK_{event_date.strftime('%Y%m%d')}_{hh}00.bi5"
            file_path = raw_dir / filename
            
            # Если утреннего файла нет - пробуем скачать
            if not file_path.exists():
                url = f"https://datafeed.dukascopy.com/datafeed/EURNOK/{event_date.year}/{event_date.month-1:02d}/{event_date.day:02d}/{hh}h_ticks.bi5"
                if download_bi5(url, file_path):
                    print(f"[DOWNLOADED] {filename}")
                    time.sleep(0.5) # Вежливость к серверу

            if file_path.exists():
                try:
                    ticks = process_bi5(file_path)
                    target_ms = 30 * 60 * 1000 # 30-я минута (09:30 местного времени)
                    
                    baseline = ticks[(ticks['ms'] >= target_ms - 60000) & (ticks['ms'] < target_ms)]
                    if baseline.empty: continue
                    
                    mid_baseline = ((baseline['Ask'] + baseline['Bid']) / 2).mean()
                    after_event = ticks[ticks['ms'] >= target_ms]
                    if after_event.empty: continue
                    
                    shock_tick = after_event.iloc[0]
                    bps = ((shock_tick['Ask'] - mid_baseline) / mid_baseline) * 10000
                    nok_penalties.append(bps)
                    
                    # Фиксим время в реестре для корректных дамми-переменных
                    df_events.at[idx, 'Date'] = event_date.replace(hour=int(hh)+1, minute=30)
                    success_count += 1
                    file_found = True
                    break
                except:
                    continue
        
        if not file_found:
            nok_penalties.append(np.nan)

    df_events['EURNOK_Penalty_BPS'] = nok_penalties
    df_events.to_csv(output_path, index=False)
    
    print(f"\n[FINAL REPORT]")
    print(f"Successfully processed: {success_count} / {len(df_events)}")
    print(f"New data saved to: {output_path}")

if __name__ == "__main__":
    main()