import requests
from pathlib import Path

# Прямая ссылка на файл, которого не хватает в твоем списке
url = "https://datafeed.dukascopy.com/datafeed/EURSEK/2024/08/25/07h_ticks.bi5"
filename = "EURSEK_20240925_0700.bi5"
target_dir = Path("data/raw")

headers = {'User-Agent': 'Mozilla/5.0'}

print(f"Targeting: {filename}")
response = requests.get(url, headers=headers)

if response.status_code == 200:
    target_dir.mkdir(parents=True, exist_ok=True)
    with open(target_dir / filename, 'wb') as f:
        f.write(response.content)
    print("SUCCESS: File downloaded correctly.")
else:
    print(f"FAILED: Status {response.status_code}. Dukascopy CDN might be down or file moved.")