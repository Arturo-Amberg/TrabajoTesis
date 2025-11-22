import pandas as pd
import numpy as np
import requests
import os
from tqdm import tqdm

# ==========================================
# 1. CONFIGURATION
# ==========================================
OSRM_TABLE_URL = "http://127.0.0.1:5000/table/v1/driving/"
FILE_PATH = 'minas_ll.csv'
OUTPUT_FILE = 'matrix_chile_mega.npz'
BATCH_SIZE = 300 # Safe size for URL length limits

# ==========================================
# 2. MATRIX CHUNKING FUNCTION
# ==========================================
def get_chunk_matrix(src_coords, dst_coords):
    """
    Calculates a rectangular matrix between Source batch and Dest batch.
    """
    # Combine all coords into one string: sources then destinations
    # OSRM expects: lon,lat;lon,lat...
    all_coords = src_coords + dst_coords
    coord_str = ";".join([f"{lon},{lat}" for lon, lat in all_coords])
    
    # Define which indices are sources and which are destinations
    n_src = len(src_coords)
    n_dst = len(dst_coords)
    
    # OSRM uses 0-based indexing for the supplied coordinates
    src_indices = ";".join(map(str, range(0, n_src)))
    dst_indices = ";".join(map(str, range(n_src, n_src + n_dst)))
    
    url = f"{OSRM_TABLE_URL}{coord_str}?sources={src_indices}&destinations={dst_indices}&annotations=duration"
    
    try:
        r = requests.get(url, timeout=60)
        if r.status_code == 200:
            data = r.json()
            matrix = np.array(data['durations'], dtype=np.float32)
            # Replace unreachable (None) with infinity
            matrix = np.nan_to_num(matrix, nan=360000.0)
            return matrix / 60.0 # Minutes
    except Exception as e:
        print(f"Error in chunk: {e}")
    
    return np.full((n_src, n_dst), 999999.0) # Return high value on fail

# ==========================================
# 3. MAIN EXECUTION
# ==========================================
if __name__ == "__main__":
    print("--- 🇨🇱 Calculating The Mega Matrix (All Chile) ---")
    
    if not os.path.exists(FILE_PATH):
        print("❌ Error: Data file not found.")
        exit()
        
    # 1. Prepare Data
    df = pd.read_csv(FILE_PATH)
    df = df.dropna(subset=['Latitud', 'Longitud'])
    
    # Store IDs to map back later
    if 'IdFaena' in df.columns:
        ids = df['IdFaena'].astype(str).values
    else:
        ids = df.index.astype(str).values
        
    coords = df[['Longitud', 'Latitud']].values.tolist()
    N = len(coords)
    
    print(f"📊 Total Mines: {N}")
    print(f"📦 Matrix Size: {N}x{N} ({N**2/1e6:.1f} Million cells)")
    print(f"🚀 Batches: {(N//BATCH_SIZE)+1} x {(N//BATCH_SIZE)+1}")
    
    # 2. Initialize Giant Matrix (Float32 to save RAM, approx 250MB)
    mega_matrix = np.zeros((N, N), dtype=np.float32)
    
    # 3. Loop via Chunks
    # We iterate through the list in blocks of BATCH_SIZE
    for i in tqdm(range(0, N, BATCH_SIZE), desc="Rows"):
        for j in range(0, N, BATCH_SIZE):
            # Define batches
            src_batch = coords[i : i + BATCH_SIZE]
            dst_batch = coords[j : j + BATCH_SIZE]
            
            if not src_batch or not dst_batch: continue
            
            # Fetch sub-matrix
            sub_matrix = get_chunk_matrix(src_batch, dst_batch)
            
            # Slot it into the giant matrix
            # i:i+len covers the row range, j:j+len covers the column range
            mega_matrix[i : i + len(src_batch), j : j + len(dst_batch)] = sub_matrix

    # 4. Save
    print(f"\n💾 Saving to {OUTPUT_FILE}...")
    np.savez_compressed(OUTPUT_FILE, matrix=mega_matrix, ids=ids)
    print("✅ Done! You have the full connectivity of Chile.")