import os
import aiohttp
from dotenv import load_dotenv
import pandas as pd
import sqlalchemy as sql
from datetime import datetime as dt

BASE_API = "http://127.0.0.1:8000"
HOST = "localhost"
BASE_WS = f"ws://{HOST}:"
load_dotenv()


async def async_get(session: aiohttp.ClientSession, url: str, pair: str = None) -> list:
    async with session.get(url) as response:
        data = await response.json()
        if pair:
            return [pair, data]
        return data


def get_db_connection() -> sql.Engine:
    user = os.getenv("DB_USER")
    pwd = os.getenv("POSTGRES_PASSWORD")
    db_name = os.getenv("DB_NAME")
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    dsn = f"postgresql://{user}:{pwd}@{host}:{port}/{db_name}"
    return sql.create_engine(dsn)


def datetime_unix_conversion(
    df: pd.DataFrame, convert_to: str, cols: list = None
) -> pd.DataFrame:
    cols = cols if cols else df.columns
    for col in cols:
        if col.endswith("tmstmp"):
            if convert_to == "unix":
                df[col] = pd.to_datetime(df[col], utc=True).astype("int64") // 10**9
            else:
                df[col] = df[col].apply(lambda x: dt.utcfromtimestamp(x))
    return df
