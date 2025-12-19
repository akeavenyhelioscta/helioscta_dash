from pathlib import Path

import pandas as pd

from helioscta_python.helioscta_python.utils import (
    azure_postgresql,
)

from helioscta_python.helioscta_python.chunk_storage.v1_2025_dec_19 import (
    configs,
    pandas_azure_writer,
)

# init logging
import logging
logging.basicConfig(level=logging.DEBUG)
logging.getLogger().handlers[0].setLevel(logging.DEBUG)

"""
"""

def _pull_from_sql(sql_filename: str = configs.filename) -> pd.DataFrame:
    """"""
    
    sql_file_path = Path(__file__).parent.parent / "sql" / f'{sql_filename}.sql'

    with open(sql_file_path, 'r') as f:
        query = f.read()
    
    df = azure_postgresql.pull_from_db(query=query)
    logging.info(f"Pulled {len(df)} rows from {sql_filename} ...")
    
    return df


def _basic_write(
        writer, 
        df: pd.DataFrame,
        blob_name = configs.azure_blob_name,
    ):
    
    azure_blob_url = writer.write_parquet(
        df=df,
        blob_name=f'{blob_name}.parquet',
        engine="pyarrow",
        compression="snappy",
    )
    logging.info(f"Wrote {len(df)} to {azure_blob_url} ...")


def _basic_read(
        writer,
        blob_name = configs.azure_blob_name,
    ):
    
    # Read specific partition
    try:
        df = writer.read_parquet(
            blob_name=f'{blob_name}.parquet',
        )
        return df
    except ValueError as e:
        logging.error(f"Error reading {blob_name}: {e}")


def pull():
    # df = _pull_from_sql()
    
    # Initialize writer
    writer = pandas_azure_writer.PandasAzureWriter()

    # # test basic write
    # _basic_write(
    #     writer=writer,
    #     df=df,
    # )    

    # test basic write
    df = _basic_read(
        writer=writer,
    )

    return df 
    

if __name__ == "__main__":
    df = pull()



