from pathlib import Path

# init logging
import logging
logging.basicConfig(level=logging.DEBUG)
logging.getLogger().handlers[0].setLevel(logging.DEBUG)

"""
"""

filename: str = 'next_day_gas_v1_2025_dec_16'
azure_blob_folder: str = 'ice_python'
azure_blob_name: str = f'{azure_blob_folder}/{filename}'


