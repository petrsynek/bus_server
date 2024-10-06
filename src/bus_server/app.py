import logging
import urllib.parse
from datetime import date, timedelta
from functools import partial

import aiohttp
import boto3
import pydantic
from litestar import Litestar, Response, get, post
from litestar.background_tasks import BackgroundTask, BackgroundTasks
from litestar.params import Parameter
from mypy_boto3_s3 import S3Client

from bus_server.config import CONFIG
from bus_server.data_processing import (get_country_stats_local,
                                        get_country_stats_s3,
                                        process_data_task_local,
                                        process_data_task_s3)

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

    processing_function = (
        partial(process_data_task_local, local_path=CONFIG.LOCAL_STORAGE_PATH)
        if CONFIG.RUN_LOCALLY else 
        partial(process_data_task_s3, client=app.state.s3_client)
    )

    return Response(
        {"Processing": requested_date},
        background=BackgroundTasks(
            [
                BackgroundTask(
                    processing_function,
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
    # ) -> dict[str, dict[str, Any]]:
) -> CountryStats:
    # FIXME: This should be `CountryStats` instead of `dict[str, dict[str, Any]]`
    # but swagger raises an error
    """
    List the data stored in S3 for given date create statistics for each country
    """

    if CONFIG.RUN_LOCALLY:
        processing_function = partial(
            get_country_stats_local, local_path=CONFIG.LOCAL_STORAGE_PATH
        )
        countries = [d.name for d in CONFIG.LOCAL_STORAGE_PATH.iterdir() if d.is_dir()]
    else:
        processing_function = partial(get_country_stats_s3, client=app.state.s3_client)
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
            country_stats = processing_function(country, requested_date)

            try:
                response[country][str(requested_date)] = country_stats
            except KeyError:
                response[country] = {}
                response[country][str(requested_date)] = country_stats

    return response


app = Litestar([process_request, country_stats])

if __name__ == "__main__":


    import uvicorn

    logging.info(f"Starting the reference server {CONFIG}")

    if not CONFIG.RUN_LOCALLY:
        # Initialize S3 client
        s3_client: S3Client = boto3.client("s3", region_name="us-east-1")
        app.state.s3_client = s3_client

    uvicorn.run(app, host="0.0.0.0", port=8000)
