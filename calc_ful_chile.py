import pandas as pd
import numpy as np
import requests
import os
import time
from tqdm import tqdm

# ==========================================
# 1. CONFIGURATION
# ==========================================
OSRM_TABLE_URL = "http://127.0.0.1:5000/table/v1/driving/"
FILE_PATH = 'minas_ll.csv'
OUTPUT_FILE = 'matrices_chile_complete.npz'

# ==========================================
# 2. OSRM MATRIX FUNCTION
# ==========================================
def get_region_matrix(coords):
    """
    Fetch N x N travel time matrix (minutes) from local OSRM.
    """
    # Format: "lon,lat;lon,lat"
    coord_str = ";".join([f"{lon},{lat}" for lon, lat in coords])
    url = f"{OSRM_TABLE_URL}{coord_str}?annotations=duration"
    
    try:
        # 10 minute timeout for large regions (like Atacama)
        r = requests.get(url, timeout=600)
        
        if r.status_code == 200:
            data = r.json()
            
            # Check if 'durations' exists in response
            if 'durations' not in data:
                return None
                
            # Convert list of lists to numpy array (Float32 to save space)
            matrix = np.array(data['durations'], dtype=np.float32)
            
            # OSRM returns None/null for unreachable points (islands, disconnected roads).
            # We replace these with a huge number (6000 mins = 100 hours) so they don't cluster.
            matrix = np.nan_to_num(matrix, nan=360000.0)
            
            # Convert seconds to minutes
            return matrix / 60.0
            
        elif r.status_code == 414:
            print("   ❌ Error: Region too big (URI Too Long).")
            return None
        else:
            print(f"   ❌ Error: OSRM Status {r.status_code}")
            return None
            
    except Exception as e:
        print(f"   ❌ Connection Error: {e}")
        return None

# ==========================================
# 3. MAIN EXECUTION
# ==========================================
if __name__ == "__main__":
    print(f"--- 🇨🇱 Starting Full Chile Scan ---")
    
    # 1. Load Data
    if not os.path.exists(FILE_PATH):
        print(f"❌ Error: '{FILE_PATH}' not found.")
        exit()
        
    df = pd.read_csv(FILE_PATH)
    # Clean invalid rows
    df = df.dropna(subset=['Latitud', 'Longitud', 'RegionFaena'])
    
    # Get list of all regions sorted alphabetically
    regions = sorted(df['RegionFaena'].unique().astype(str))
    
    print(f"📂 Loaded {len(df)} mines across {len(regions)} regions.")
    print(f"📋 Regions found: {regions}")
    
    # Dictionary to store all results
    arrays_to_save = {}
    
    # 2. Iterate through every region
    for region in tqdm(regions, desc="Processing Regions"):
        # Filter data for current region
        df_reg = df[df['RegionFaena'] == region]
        n_mines = len(df_reg)
        
        # Skip invalid regions
        if n_mines < 2:
            continue
            
        # Prepare Coordinates
        coords = df_reg[['Longitud', 'Latitud']].values.tolist()
        
        # Calculate Matrix
        matrix = get_region_matrix(coords)
        
        if matrix is not None:
            # Save Matrix using the Region Name as key
            arrays_to_save[f"{region}_matrix"] = matrix
            
            # Save ID mapping (Use IdFaena if exists, else use index)
            if 'IdFaena' in df_reg.columns:
                arrays_to_save[f"{region}_ids"] = df_reg['IdFaena'].values
            else:
                arrays_to_save[f"{region}_ids"] = df_reg.index.to_numpy()
                
            # Optional: Print status for big regions
            if n_mines > 1000:
                tqdm.write(f"   ✅ {region}: Processed {n_mines} mines.")

    # 3. Save Final File
    print(f"\n💾 Compressing and saving to '{OUTPUT_FILE}'...")
    np.savez_compressed(OUTPUT_FILE, **arrays_to_save)
    
    file_size = os.path.getsize(OUTPUT_FILE) / (1024 * 1024)
    print(f"✅ Done! Final file size: {file_size:.2f} MB")
    print("You can now download this file and use it in your notebook.")