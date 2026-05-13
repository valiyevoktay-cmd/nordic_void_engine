import pandas as pd
import requests
from pathlib import Path
import time

def download_dukascopy_bi5(symbol, target_date, target_dir):
    """
    Скачивает .bi5 файл напрямую из Dukascopy CDN.
    target_date: объект datetime (UTC)
    """
    year = target_date.strftime('%Y')
    # ВАЖНО: Месяцы в Dukascopy 0-индексированы (00 = Январь)
    dukascopy_month = str(target_date.month - 1).zfill(2)
    day = target_date.strftime('%d')
    hour = target_date.strftime('%H')
    
    filename = f"{symbol}_{target_date.strftime('%Y%m%d_%H')}00.bi5"
    
    # Ссылка на CDN
    url = f"https://datafeed.dukascopy.com/datafeed/{symbol}/{year}/{dukascopy_month}/{day}/{hour}h_ticks.bi5"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)'
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=15)
        if response.status_code == 200:
            file_path = target_dir / filename
            with open(file_path, 'wb') as f:
                f.write(response.content)
            print(f"[SUCCESS] Downloaded: {filename}")
            return True
        else:
            print(f"[ERROR] {filename} | HTTP {response.status_code}")
            return False
    except Exception as e:
        print(f"[ERROR] {filename} | {e}")
        return False

import os

if __name__ == "__main__":
    # Имя, которое мы ищем
    target_name = "2026-05-07T11-14_export.csv"
    registry_path = None

    print("--- Запуск диагностического поиска ---")
    # Прочесываем все папки в проекте, чтобы найти этот файл
    for root, dirs, files in os.walk("."):
        if target_name in files:
            registry_path = Path(root) / target_name
            break

    if registry_path:
        print(f"[OK] Файл найден по адресу: {registry_path}")
    else:
        print(f"[ERROR] Файл '{target_name}' не найден ни в одной папке проекта!")
        print("Список файлов, которые я вижу в текущей папке:")
        print(os.listdir("."))
        exit()

    # Дальше твой старый код...
    df = pd.read_csv(registry_path)
    df['Date'] = pd.to_datetime(df['Date'])
    
    target_directory = Path("data/raw_nok")
    target_directory.mkdir(parents=True, exist_ok=True)
    
    print(f"Starting EURNOK data acquisition for {len(df)} events...")
    
    success_count = 0
    for event_date in df['Date']:
        # Качаем файл для часа события
        if download_dukascopy_bi5("EURNOK", event_date, target_directory):
            success_count += 1
        
        # Небольшая пауза, чтобы сервер не забанил
        time.sleep(0.5)
        
    print(f"\nCompleted! Successfully downloaded {success_count} out of {len(df)} files.")
    print(f"Files are located in: {target_directory}")