from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import SecretStr
import duckdb 
import pyarrow as pa
import pyarrow.parquet as pq


def connect(
    memory_limit: str = "8GB", threads: int = 8,
) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.sql("INSTALL spatial; LOAD spatial;")
    con.sql("INSTALL httpfs; LOAD httpfs;")
    con.sql("INSTALL h3 FROM community; LOAD h3")
    con.sql("INSTALL a5 FROM community; LOAD a5")
    con.sql("SET preserve_insertion_order = false")
    con.sql("SET TimeZone = 'UTC'")  # Delta stores timestamps as UTC
    con.sql("SET max_temp_directory_size = '10GB'") #Added as a solution to avoid OOM.
    return con

class Settings(BaseSettings):
    FIRMS_API_KEY: SecretStr

    model_config = SettingsConfigDict(
        env_file = '.env',
        env_file_encoding = 'utf-8',
        case_sensitive = False
    
    )

def write_parquet(df, path):
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(
        table,
        path,
        compression="snappy",
    )