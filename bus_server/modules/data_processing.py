### Data processing functions


import logging
import urllib.parse
from datetime import date, datetime
from io import BytesIO
from pathlib import Path
from typing import Any

import aiohttp
import pandas as pd
from isodate import parse_duration  # type: ignore[import-untyped]
from modules.config import CONFIG
from mypy_boto3_s3 import S3Client


def iso_duration_to_seconds(duration_str):
    """
    Convert ISO 8601 duration string to seconds.
    If the input is not a string or is an empty string, return the input as is.
    """
    if not isinstance(duration_str, str) or duration_str == "":
        return duration_str
    try:
        return int(parse_duration(duration_str).total_seconds())
    except Exception:
        return duration_str  # Return original value if parsing fails


async def gather_city_data(
    requested_date: date, city_id: int, city: str
) -> pd.DataFrame:
    """
    Fetches data from the reference server and returns it as a DataFrame.
    """

    logging.info(f"Processing data for {city}:{city_id}:{requested_date}")

    async with aiohttp.ClientSession() as session:
        dt_date = datetime.combine(requested_date, datetime.min.time())
        async with session.get(
            urllib.parse.urljoin(
                str(CONFIG.REF_SERVER_URL),
                f"cities/{city_id}/stats?date={dt_date}",
            )
        ) as response:
            data = await response.json()

    city_df = pd.DataFrame(data)
    if city_df.empty:
        logging.warning(f"No data found for {city}:{requested_date}")
        return city_df

    city_df["delay"] = city_df["delay"].apply(iso_duration_to_seconds)

    return city_df


async def process_data_task_local(
    requested_date: date, city_id: int, city: str, country: str, local_path: Path
) -> None:
    city_df = await gather_city_data(requested_date, city_id, city)

    buffer = BytesIO()
    city_df.to_parquet(buffer, index=False)
    buffer.seek(0)

    file = f"{country}/{requested_date}/{city}/data.parquet"

    path = local_path / file
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "wb") as f:
        f.write(buffer.getvalue())

    logging.info(f"Processed and saved data for {city}:{requested_date}")


async def process_data_task_s3(
    requested_date: date, city_id: int, city: str, country: str, client: S3Client
) -> None:
    if not client:
        raise ValueError("S3 client is not initialized")

    city_df = await gather_city_data(requested_date, city_id, city)

    buffer = BytesIO()
    city_df.to_parquet(buffer, index=False)
    buffer.seek(0)

    file = f"{country}/{requested_date}/{city}/data.parquet"

    client.put_object(Bucket=CONFIG.S3_BUCKET, Key=file, Body=buffer.getvalue())

    logging.info(f"Processed and saved data for {city}:{requested_date}")


def get_country_stats_local(
    country: str, requested_date: date, local_path: Path
) -> dict[str, Any]:
    """
    List the data stored in local directory for given date create statistics for given
    country and date
    """

    base_path = local_path / country / str(requested_date)
    files = list(base_path.rglob("*/*.parquet"))

    if not files:
        return {}

    data = []

    for city_file in files:
        df = pd.read_parquet(city_file)
        data.append(df)

    country_wide_data = pd.concat(data)

    logging.info(f"Processing data for {country}:{requested_date}")

    return {
        "number_of_buses": int(country_wide_data["bus-type"].nunique()),
        "total_passengers": int(country_wide_data["passengers"].sum()),
        "total_accidents": int(country_wide_data["accident"].sum()),
        "average_delay": float(country_wide_data["delay"].mean()),
    }


def get_country_stats_s3(
    country: str, requested_date: date, client: S3Client
) -> dict[str, Any]:
    """
    List the data stored in S3 for given date create statistics for given country
    and date
    """
    if not client:
        raise ValueError("S3 client is not initialized")

    contents = client.list_objects_v2(
        Bucket=CONFIG.S3_BUCKET, Prefix=f"{country}/{requested_date}"
    ).get("Contents", [])
    files = [obj["Key"] for obj in contents]

    if not files:
        return {}

    data = []

    for city_file in files:
        response = client.get_object(Bucket=CONFIG.S3_BUCKET, Key=city_file)

        buffer = BytesIO(response["Body"].read())
        df = pd.read_parquet(buffer)
        data.append(df)

    country_wide_data = pd.concat(data)

    logging.info(f"Processing data for {country}:{requested_date}")

    return {
        "number_of_buses": int(country_wide_data["bus-type"].nunique()),
        "total_passengers": int(country_wide_data["passengers"].sum()),
        "total_accidents": int(country_wide_data["accident"].sum()),
        "average_delay": float(country_wide_data["delay"].mean()),
    }
