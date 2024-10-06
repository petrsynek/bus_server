import logging
import urllib.parse
from datetime import date, datetime, timedelta
from io import BytesIO
from pathlib import Path
from typing import Any

import aiohttp
import boto3
import pandas as pd
import pydantic
from isodate import parse_duration  # type: ignore[import-untyped]
from litestar import Litestar, Response, get, post
from litestar.background_tasks import BackgroundTask, BackgroundTasks
from litestar.params import Parameter
from pydantic_settings import BaseSettings


class AppConfig(BaseSettings):
    S3_BUCKET: str = "your-s3-bucket-name"
    REF_SERVER_URL: pydantic.HttpUrl = pydantic.HttpUrl("http://localhost:8080")
    DEBUG: bool = True
    DEBUG_S3_LOCAL_PATH: Path = Path.cwd() / "local"

    class Config:
        env_prefix = "BUS_APP_"
        env_file = ".env"


CONFIG = AppConfig()

# Initialize S3 client
s3_client = boto3.client("s3", region_name="us-east-1")

### Data processing functions


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


async def process_data_task(
    requested_date: date, city_id: int, city: str, country: str
) -> None:
    """
    Fetches data from the reference server and saves it to S3
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

    df = pd.DataFrame(data)
    if df.empty:
        logging.warning(f"No data found for {city}:{requested_date}")
        return

    df["delay"] = df["delay"].apply(iso_duration_to_seconds)

    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)

    file = f"{country}/{requested_date}/{city}/data.parquet"

    if CONFIG.DEBUG:
        path = CONFIG.DEBUG_S3_LOCAL_PATH / file
        path.parent.mkdir(parents=True, exist_ok=True)
        logging.info(f"Saving data to {path}")
        with open(path, "wb") as f:
            f.write(buffer.getvalue())
    else:
        app.state.s3_client.put_object(
            Bucket=CONFIG.S3_BUCKET, Key=file, Body=buffer.getvalue()
        )

    logging.info(f"Processed and saved data for {city}:{requested_date}")


def get_country_stats(country: str, requested_date: date) -> dict[str, Any]:
    """
    List the data stored in S3 for given date create statistics for given country
    and date
    """

    if CONFIG.DEBUG:
        base_path = CONFIG.DEBUG_S3_LOCAL_PATH / country / str(requested_date)
        files = list(base_path.rglob("*/*.parquet"))
    else:
        contents = app.state.s3_client.list_objects_v2(
            Bucket=CONFIG.S3_BUCKET, Prefix=f"{country}/{date}"
        ).response.get("Contents", [])
        files = [obj["Key"] for obj in contents]

    if not files:
        return {}

    data = []

    for city_file in files:
        if CONFIG.DEBUG:
            df = pd.read_parquet(city_file)
        else:
            response = app.state.s3_client.get_object(
                Bucket=CONFIG.S3_BUCKET, Key=city_file
            )
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


### API endpoints


@post("/process-request")
async def process_request(
    requested_date: date = Parameter(query="date"),
) -> Response[dict[str, date]]:
    """
    Fetches the list of cities from the reference server and registers tasks to process
    data for each city
    """

    async with aiohttp.ClientSession() as session:
        async with session.get(
            urllib.parse.urljoin(str(CONFIG.REF_SERVER_URL), "cities")
        ) as response:
            cities = await response.json()

    return Response(
        {"Processing": requested_date},
        background=BackgroundTasks(
            [
                BackgroundTask(
                    process_data_task,
                    requested_date,
                    city["id"],
                    city["name"],
                    city["country"],
                )
                for city in cities
            ]
        ),
    )


class TrafficStats(pydantic.BaseModel):
    number_of_buses: int = pydantic.Field(
        ..., description="Number of buses in operation"
    )
    total_passengers: int = pydantic.Field(
        ..., description="Total number of passengers"
    )
    total_accidents: int = pydantic.Field(..., description="Total number of accidents")
    average_delay: float = pydantic.Field(..., description="Average delay in seconds")


class DateStats(pydantic.RootModel):
    root: dict[str, TrafficStats] = pydantic.Field(
        ..., description="Date to stats mapping"
    )


class CountryStats(pydantic.RootModel):
    root: dict[str, DateStats] = pydantic.Field(
        ..., description="Country to date stats mapping"
    )


@get("/country-stats", response_model=CountryStats)
async def country_stats(
    from_date: date = Parameter(query="from"),
    to_date: date = Parameter(query="to"),
) -> dict[str, dict[str, Any]]:
    # FIXME: This should be `CountryStats` instead of `dict[str, dict[str, Any]]`
    # but swagger raises an error
    """
    List the data stored in S3 for given date create statistics for each country
    """

    if CONFIG.DEBUG:
        countries = [d.name for d in CONFIG.DEBUG_S3_LOCAL_PATH.iterdir() if d.is_dir()]
    else:
        response = app.state.s3_client.list_objects_v2(
            Bucket=CONFIG.S3_BUCKET, Delimiter="/"
        )
        countries = [
            prefix["Prefix"].strip("/") for prefix in response.get("CommonPrefixes", [])
        ]

    response = {}

    for requested_date in (
        from_date + timedelta(days=n) for n in range((to_date - from_date).days + 1)
    ):
        for country in countries:
            country_stats = get_country_stats(country, requested_date)
            try:
                response[country][str(requested_date)] = country_stats
            except KeyError:
                response[country] = {}
                response[country][str(requested_date)] = country_stats

    return response


app = Litestar([process_request, country_stats])
app.state.s3_client = s3_client

if __name__ == "__main__":
    import uvicorn

    logging.info(f"Starting the reference server {CONFIG}")
    uvicorn.run(app, host="0.0.0.0", port=8000)
