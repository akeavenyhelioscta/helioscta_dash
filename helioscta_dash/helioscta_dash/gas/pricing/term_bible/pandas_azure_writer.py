"""
PandasAzureWriter - Module for writing Pandas DataFrames to Azure Blob Storage

This module provides a class-based interface for writing Pandas DataFrames
to Azure Blob Storage as Parquet files, integrating with AzureChunkStorageClient.
"""

import os
import tempfile
from pathlib import Path
from typing import Optional, List, Dict, Union
import pandas as pd
from dotenv import load_dotenv

from helioscta_python.helioscta_python.chunk_storage.v1_2025_dec_19 import (
    azure_chunk_storage_utils,
)

# Load environment variables
load_dotenv()

# Azure Configuration from environment
AZURE_CONNECTION_STRING = os.getenv("AZURE_CHUNK_STORAGE_CONNECTION_STRING")
AZURE_STORAGE_ACCOUNT = os.getenv("AZURE_CHUNK_STORAGE_ACCOUNT_NAME")
AZURE_CONTAINER = os.getenv("AZURE_CHUNK_STORAGE_CONTAINER_NAME")

"""
"""

class PandasAzureWriter:
    """
    Pandas writer for Azure Blob Storage Parquet files.
    
    This class provides methods to write Pandas DataFrames to Azure Blob Storage
    as Parquet files with various configurations (partitioning, compression, chunking).
    
    Attributes:
        azure_client (AzureChunkStorageClient): Azure storage client instance
        storage_account (str): Azure storage account name
        container (str): Azure container name
    """
    
    def __init__(
        self,
        storage_account: Optional[str] = None,
        container: Optional[str] = None,
        connection_string: Optional[str] = None
    ):
        """
        Initialize PandasAzureWriter.
        
        Args:
            storage_account: Azure storage account name (defaults to env var)
            container: Azure container name (defaults to env var)
            connection_string: Azure connection string (defaults to env var)
        """
        self.azure_client = azure_chunk_storage_utils.AzureChunkStorageClient(
            connection_string=connection_string or AZURE_CONNECTION_STRING,
            storage_account_name=storage_account or AZURE_STORAGE_ACCOUNT,
            container_name=container or AZURE_CONTAINER
        )
        self.storage_account = storage_account or AZURE_STORAGE_ACCOUNT
        self.container = container or AZURE_CONTAINER
    
    def write_parquet(
        self,
        df: pd.DataFrame,
        blob_name: str,
        compression: str = "snappy",
        overwrite: bool = True,
        include_timestamp: bool = False,
        engine: str = "pyarrow",
        **kwargs
    ) -> str:
        """
        Write DataFrame as Parquet to Azure Blob Storage.
        
        Args:
            df: Pandas DataFrame to write
            blob_name: Name/path of blob in Azure (e.g., "output/data.parquet")
            compression: Compression codec (snappy, gzip, brotli, lz4, zstd, none)
            overwrite: Whether to overwrite existing blob
            include_timestamp: Add timestamp to filename
            engine: Parquet engine (pyarrow or fastparquet)
            **kwargs: Additional arguments passed to df.to_parquet()
        
        Returns:
            URL of the written blob
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            # Ensure .parquet extension
            if not blob_name.endswith('.parquet'):
                blob_name = f"{blob_name}.parquet"
            
            # Add timestamp if requested
            if include_timestamp:
                from datetime import datetime
                name, ext = blob_name.rsplit('.', 1)
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                blob_name = f"{name}_{timestamp}.{ext}"
            
            # Write to temp file
            temp_file = Path(temp_dir) / "temp.parquet"
            df.to_parquet(
                temp_file,
                engine=engine,
                compression=compression,
                index=False,
                **kwargs
            )
            
            # Upload to Azure
            url = self.azure_client.upload_file(
                file_path=temp_file,
                blob_name=blob_name,
                content_type='application/octet-stream',
                overwrite=overwrite
            )
            
            return url
    
    def write_parquet_partitioned(
        self,
        df: pd.DataFrame,
        base_blob_path: str,
        partition_cols: List[str],
        compression: str = "snappy",
        overwrite: bool = True,
        engine: str = "pyarrow"
    ) -> List[str]:
        """
        Write DataFrame as partitioned Parquet files to Azure.
        
        Creates a folder structure based on partition columns.
        Example: department=Engineering/country=USA/data.parquet
        
        Args:
            df: Pandas DataFrame to write
            base_blob_path: Base path for partitioned data (e.g., "output/data")
            partition_cols: List of columns to partition by
            compression: Compression codec
            overwrite: Whether to overwrite existing blobs
            engine: Parquet engine
        
        Returns:
            List of URLs for written blobs
        """
        # Ensure partition columns exist
        for col in partition_cols:
            if col not in df.columns:
                raise ValueError(f"Partition column '{col}' not found in DataFrame")
        
        # Group by partition columns
        grouped = df.groupby(partition_cols)
        
        urls = []
        for partition_values, group_df in grouped:
            # Build partition path
            if isinstance(partition_values, tuple):
                partition_path_parts = [
                    f"{col}={val}" 
                    for col, val in zip(partition_cols, partition_values)
                ]
            else:
                partition_path_parts = [f"{partition_cols[0]}={partition_values}"]
            
            partition_path = "/".join(partition_path_parts)
            blob_name = f"{base_blob_path}/{partition_path}/data.parquet"
            
            # Remove partition columns from data (they're in the path)
            data_to_write = group_df.drop(columns=partition_cols)
            
            # Write partition
            url = self.write_parquet(
                df=data_to_write,
                blob_name=blob_name,
                compression=compression,
                overwrite=overwrite,
                engine=engine
            )
            urls.append(url)
        
        return urls
    
    def write_parquet_chunked(
        self,
        df: pd.DataFrame,
        base_blob_path: str,
        chunk_size: int = 100000,
        compression: str = "snappy",
        overwrite: bool = True,
        engine: str = "pyarrow"
    ) -> List[str]:
        """
        Write DataFrame as multiple Parquet files (chunks) to Azure.
        
        Splits large DataFrame into smaller files for better manageability.
        
        Args:
            df: Pandas DataFrame to write
            base_blob_path: Base path for chunks (e.g., "output/data")
            chunk_size: Number of rows per chunk
            compression: Compression codec
            overwrite: Whether to overwrite existing blobs
            engine: Parquet engine
        
        Returns:
            List of URLs for written blobs
        """
        urls = []
        num_chunks = (len(df) + chunk_size - 1) // chunk_size  # Ceiling division
        
        for i in range(num_chunks):
            start_idx = i * chunk_size
            end_idx = min((i + 1) * chunk_size, len(df))
            chunk_df = df.iloc[start_idx:end_idx]
            
            # Create chunk filename
            blob_name = f"{base_blob_path}/chunk_{i:05d}.parquet"
            
            url = self.write_parquet(
                df=chunk_df,
                blob_name=blob_name,
                compression=compression,
                overwrite=overwrite,
                engine=engine
            )
            urls.append(url)
        
        return urls
    
    def append_parquet(
        self,
        df: pd.DataFrame,
        blob_name: str,
        compression: str = "snappy",
        engine: str = "pyarrow"
    ) -> str:
        """
        Append DataFrame to existing Parquet file in Azure.
        
        Reads existing file, concatenates new data, and writes back.
        Note: This reads the entire existing file into memory.
        
        Args:
            df: Pandas DataFrame to append
            blob_name: Name of existing parquet blob
            compression: Compression codec
            engine: Parquet engine
        
        Returns:
            URL of the updated blob
        """
        # Check if blob exists
        if self.azure_client.blob_exists(blob_name):
            # Read existing data
            existing_df = self.read_parquet(blob_name)
            
            # Combine with new data
            combined_df = pd.concat([existing_df, df], ignore_index=True)
        else:
            combined_df = df
        
        # Write combined data
        return self.write_parquet(
            df=combined_df,
            blob_name=blob_name,
            compression=compression,
            overwrite=True,
            engine=engine
        )
    
    def read_parquet(
        self,
        blob_name: str,
        columns: Optional[List[str]] = None,
        filters: Optional[List] = None,
        engine: str = "pyarrow"
    ) -> pd.DataFrame:
        """
        Read Parquet file from Azure Blob Storage.
        
        Args:
            blob_name: Name of the parquet blob
            columns: Specific columns to read (None = all columns)
            filters: Row filters (PyArrow format)
            engine: Parquet engine
        
        Returns:
            Pandas DataFrame
        """
        import io
        
        blob_data = self.azure_client.download_blob(blob_name)
        
        df = pd.read_parquet(
            io.BytesIO(blob_data),
            engine=engine,
            columns=columns,
            filters=filters
        )
        
        return df
    
    def read_parquet_partitioned(
        self,
        base_blob_path: str,
        partition_filter: Optional[Dict[str, str]] = None
    ) -> pd.DataFrame:
        """
        Read partitioned Parquet files from Azure.
        
        Args:
            base_blob_path: Base path to partitioned data
            partition_filter: Filter specific partitions (e.g., {'year': '2024'})
        
        Returns:
            Combined Pandas DataFrame
        """
        # Build search prefix
        search_prefix = base_blob_path.rstrip('/')
        if partition_filter:
            for key, value in partition_filter.items():
                search_prefix += f"/{key}={value}"
        
        # Find all parquet files
        all_blobs = self.azure_client.list_blobs(name_starts_with=search_prefix)
        parquet_blobs = [b for b in all_blobs if b.endswith('.parquet')]
        
        if not parquet_blobs:
            raise ValueError(f"No parquet files found at: {search_prefix}")
        
        # Read all files
        dfs = []
        for blob in parquet_blobs:
            df = self.read_parquet(blob)
            
            # Extract and add partition columns from path
            path_parts = blob.split('/')
            for part in path_parts:
                if '=' in part:
                    key, value = part.split('=', 1)
                    df[key] = value
            
            dfs.append(df)
        
        # Combine
        combined_df = pd.concat(dfs, ignore_index=True)
        return combined_df
    
    def read_parquet_chunked(
        self,
        base_blob_path: str,
        process_func=None
    ) -> pd.DataFrame:
        """
        Read multiple Parquet chunk files from Azure.
        
        Args:
            base_blob_path: Base path containing chunk files
            process_func: Optional function to process each chunk before combining
        
        Returns:
            Combined Pandas DataFrame
        """
        # Find all chunk files
        all_blobs = self.azure_client.list_blobs(name_starts_with=base_blob_path)
        chunk_blobs = sorted([b for b in all_blobs if b.endswith('.parquet')])
        
        if not chunk_blobs:
            raise ValueError(f"No parquet files found at: {base_blob_path}")
        
        # Read and optionally process each chunk
        dfs = []
        for blob in chunk_blobs:
            df = self.read_parquet(blob)
            
            if process_func:
                df = process_func(df)
            
            dfs.append(df)
        
        # Combine
        combined_df = pd.concat(dfs, ignore_index=True)
        return combined_df
    
    def list_blobs(self, prefix: Optional[str] = None) -> List[str]:
        """
        List blobs in the container.
        
        Args:
            prefix: Optional prefix to filter blobs
        
        Returns:
            List of blob names
        """
        return self.azure_client.list_blobs(name_starts_with=prefix)
    
    def blob_exists(self, blob_name: str) -> bool:
        """
        Check if a blob exists.
        
        Args:
            blob_name: Name of the blob
        
        Returns:
            True if blob exists, False otherwise
        """
        return self.azure_client.blob_exists(blob_name)
    
    def get_blob_url(self, blob_name: str) -> str:
        """
        Get URL for a blob.
        
        Args:
            blob_name: Name of the blob
        
        Returns:
            Full URL to the blob
        """
        return self.azure_client.get_blob_url(blob_name)
    
    def delete_blob(self, blob_name: str) -> bool:
        """
        Delete a blob.
        
        Args:
            blob_name: Name of the blob
        
        Returns:
            True if successful
        """
        return self.azure_client.delete_blob(blob_name)
    
    def get_parquet_info(self, blob_name: str) -> Dict:
        """
        Get information about a Parquet file without reading all data.
        
        Args:
            blob_name: Name of the parquet blob
        
        Returns:
            Dictionary with file info (rows, columns, size, etc.)
        """
        import io
        import pyarrow.parquet as pq
        
        blob_data = self.azure_client.download_blob(blob_name)
        parquet_file = pq.ParquetFile(io.BytesIO(blob_data))
        
        info = {
            'num_rows': parquet_file.metadata.num_rows,
            'num_columns': parquet_file.metadata.num_columns,
            'num_row_groups': parquet_file.metadata.num_row_groups,
            'columns': [col.name for col in parquet_file.schema],
            'size_bytes': len(blob_data),
            'compression': parquet_file.metadata.row_group(0).column(0).compression
        }
        
        return info


"""
"""

if __name__ == '__main__':

    
        
    # Initialize writer
    writer: PandasAzureWriter = PandasAzureWriter()
    
    # List blob
    blobs: List[str] = writer.list_blobs()
    print(blobs)