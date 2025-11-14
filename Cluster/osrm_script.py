import numpy as np
import pandas as pd
import requests
from tqdm import tqdm
import time
import os

# ==============================
# CONFIG
# ==============================

# Work from current folder
BASE_DIR = os.getcwd()

df = pd.read_csv(f"{BASE_DIR}/df_15c.csv")
puertos = pd.read_csv(f"{BASE_DIR}/puertos.csv")

# ===========================================
# OSRM Travel Time Calculation to ALL Ports
# ===========================================

def osrm_route_time(lat1, lon1, lat2, lon2, retries=3):
    """Query OSRM public API for driving time between two coordinates (in minutes)."""
    url = f"https://router.project-osrm.org/route/v1/driving/{lon1},{lat1};{lon2},{lat2}?overview=false"
    for _ in range(retries):
        try:
            r = requests.get(url, timeout=15)
            if r.status_code == 200:
                data = r.json()
                return data['routes'][0]['duration'] / 60  # seconds → minutes
        except Exception:
            pass
        time.sleep(1)
    return None  # failed after retries

# ===========================================
# Compute travel times for each port
# ===========================================
for _, port in puertos.iterrows():
    col_name = f"Tiempo_Prt_{port['portName']}"
    print(f"\n🛳️ Calculating {col_name} ...")
    times = []
    for _, row in tqdm(df.iterrows(), total=len(df)):
        t = osrm_route_time(row['Latitud'], row['Longitud'], port['latitude'], port['longitude'])
        times.append(t)
        time.sleep(0.1)  # rate limiting to avoid API bans
    df[col_name] = times

# ===========================================
# Save final dataset
# ===========================================
output_path = f"{BASE_DIR}/minas_con_tiempos_puertos.csv"
df.to_csv(output_path, index=False)
print(f"\n✅ Travel times calculated and saved to {output_path}")
