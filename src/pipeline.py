import lzma
import numpy as np
import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import Union

class Bi5Parser:
    """
    High-performance, vectorized parser for Dukascopy .bi5 binary tick files.
    """
    
    # Point factor for EUR/SEK (5 decimal places)
    PRICE_FACTOR = 100_000.0 
    
    # Dukascopy struct format: >3I2f (Big-endian: 3 Unsigned Ints, 2 Floats)
    DTYPE = np.dtype([
        ('time_ms', '>u4'),
        ('ask', '>u4'),
        ('bid', '>u4'),
        ('ask_vol', '>f4'),
        ('bid_vol', '>f4')
    ])

    @classmethod
    def parse_file(cls, file_path: Union[str, Path], base_hour: datetime) -> pd.DataFrame:
        """
        Decompresses and parses a single .bi5 file into a Pandas DataFrame.
        """
        file_path = Path(file_path)
        if not file_path.exists():
            raise FileNotFoundError(f"Missing tick data file: {file_path}")

        # Decompress the LZMA file into raw bytes
        with lzma.open(file_path, 'rb') as f:
            raw_bytes = f.read()

        if not raw_bytes:
            return pd.DataFrame()

        # Map bytes directly to a NumPy array (Zero-copy)
        data = np.frombuffer(raw_bytes, dtype=cls.DTYPE)
        
        # BULLETPROOF ENDIANNESS FIX:
        # Instead of manual byte-swapping, we extract each column and cast it 
        # to a native Little-Endian type using .astype(). NumPy automatically 
        # translates Big-Endian memory to native memory during this cast.
        
        parsed_data = {
            'timestamp': base_hour + pd.to_timedelta(data['time_ms'].astype(np.uint32), unit='ms'),
            'ask': data['ask'].astype(np.uint32) / cls.PRICE_FACTOR,
            'bid': data['bid'].astype(np.uint32) / cls.PRICE_FACTOR,
            'ask_vol': data['ask_vol'].astype(np.float32),
            'bid_vol': data['bid_vol'].astype(np.float32)
        }
        
        df = pd.DataFrame(parsed_data)
        
        df['spread'] = df['ask'] - df['bid']
        df.set_index('timestamp', inplace=True)
        
        return df

if __name__ == "__main__":
    print("Module 2 (Bi5Parser) ready.")