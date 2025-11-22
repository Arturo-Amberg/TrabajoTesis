import pandas as pd
import numpy as np
import requests
import os
import time

# ==========================================
# CONFIGURATION
# ==========================================
OSRM_URL = "http://127.0.0.1:5000/table/v1/driving/"
FILE_PATH = 'minas_ll.csv'
TARGET_REGION = "III" # Atacama (The Stress Test)

def get_matrix(coords):
    """Fetches the N x N travel time matrix."""
    # 1. Format Coordinates
    # OSRM expects "lon,lat;lon,lat..."
    coord_str = ";".join([f"{lon},{lat}" for lon, lat in coords])
    
    # 2. Build URL
    url = f"{OSRM_URL}{coord_str}?annotations=duration"
    
    print(f"   -> Requesting Matrix for {len(coords)} points...")
    print(f"   -> Payload size: ~{len(coord_str)/1024:.2f} KB")
    
    start_time = time.time()
    
    try:
        # 3. Send Request (Long timeout for big calculation)
        r = requests.get(url, timeout=300)
        
        if r.status_code == 200:
            data = r.json()
            duration = time.time() - start_time
            
            # 4. Parse Result
            matrix = np.array(data['durations'], dtype=np.float32)
            # Clean unreachables
            matrix = np.nan_to_num(matrix, nan=360000.0) 
            # Convert to Minutes
            matrix = matrix / 60.0
            
            print(f"   ✅ Success! Calculated in {duration:.2f} seconds.")
            return matrix
        
        elif r.status_code == 400:
            print(f"   ❌ Error 400: Too many points? Did you set --max-table-size?")
            print(f"   Message: {r.text}")
        elif r.status_code == 414:
            print(f"   ❌ Error 414: URI Too Long. The list of coordinates is too big for HTTP.")
        else:
            print(f"   ❌ Error {r.status_code}: {r.text}")
            
    except Exception as e:
        print(f"   ❌ Connection Failed: {e}")
    
    return None

if __name__ == "__main__":
    print(f"--- 🚗 Starting Test Drive: Region {TARGET_REGION} ---")
    
    # 1. Load Data
    if not os.path.exists(FILE_PATH):
        print("File not found.")
        exit()
        
    df = pd.read_csv(FILE_PATH)
    
    # 2. Filter Region
    df_reg = df[df['RegionFaena'] == TARGET_REGION]
    print(f"Found {len(df_reg)} mines in Region {TARGET_REGION}.")
    
    # 3. Prepare Coordinates
    coords = df_reg[['Longitud', 'Latitud']].values.tolist()
    
    # 4. Run Test
    matrix = get_matrix(coords)
    
    # 5. Validate
    if matrix is not None:
        print("\n--- 📊 Matrix Statistics ---")
        print(f"Shape: {matrix.shape}")
        print(f"Min Time: {np.min(matrix):.1f} min")
        print(f"Max Time: {np.max(matrix):.1f} min")
        print(f"Mean Time: {np.mean(matrix):.1f} min")
        
        # Save it to verify size
        outfile = f"matrix_{TARGET_REGION}.npz"
        np.savez_compressed(outfile, matrix=matrix)
        size_mb = os.path.getsize(outfile) / (1024 * 1024)
        print(f"\n💾 Saved to {outfile} ({size_mb:.2f} MB)")
        print("Test Drive Passed. System is ready for full run.")
    else:
        print("\nTest Drive Failed.")