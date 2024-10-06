import urllib.parse
from datetime import date, timedelta
from functools import partial
from typing import Dict

import aiohttp
import pydantic
from litestar import Response, get, post
from litestar.background_tasks import BackgroundTask, BackgroundTasks
from litestar.params import Parameter
from modules.config import CONFIG, S3_CLIENT
from modules.data_processing import (
    get_country_stats_local,
    get_country_stats_s3,
    process_data_task_local,
    process_data_task_s3,
)


@post(
    "/process-request",
    description="""
    Fetches the list of cities from the reference server and registers tasks to process
    data for each city

    Args:
        requested_date (date): The date for which data should be processed

    Returns:
        Response[dict[str, date]]: A response containing the requested date
    """,
)
async def process_request(
    requested_date: date = Parameter(query="date", description="The date to process"),
) -> Response[dict[str, date]]:
    """
    Fetches the list of cities from the reference server and registers tasks to process
    data for each city

    Args:
        requested_date (date): The date for which data should be processed

    Returns:
        Response[dict[str, date]]: A response containing the requested date
    """

    async with aiohttp.ClientSession() as session:
        async with session.get(
            urllib.parse.urljoin(str(CONFIG.REF_SERVER_URL), "cities")
        ) as response:
            cities = await response.json()

    processing_function = (
        partial(process_data_task_local, local_path=CONFIG.LOCAL_STORAGE_PATH)
        if CONFIG.RUN_LOCALLY
        else partial(process_data_task_s3, client=S3_CLIENT)
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


DateStats = pydantic.RootModel[Dict[date, TrafficStats]]

CountryStats = pydantic.RootModel[Dict[str, DateStats]]  # type: ignore[valid-type]


@get(
    "/country-stats",
    response_model=CountryStats,
    description="""
    List the data stored in S3 for given dates create statistics for each country

    Args:
        from_date (date): The start date
        to_date (date): The end date

    Returns:
        CountryStats: A dictionary containing the statistics for each country for each date
    """,
)  # type: ignore[valid-type]
async def country_stats(
    from_date: date = Parameter(query="from", description="The start date"),
    to_date: date = Parameter(query="to", description="The end date"),
) -> CountryStats:  # type: ignore[valid-type]
    """
    List the data stored in S3 for given dates create statistics for each country

    Args:
        from_date (date): The start date
        to_date (date): The end date

    Returns:
        CountryStats: A dictionary containing the statistics for each country for each date
    """

    if CONFIG.RUN_LOCALLY:
        processing_function = partial(
            get_country_stats_local, local_path=CONFIG.LOCAL_STORAGE_PATH
        )
        countries = [d.name for d in CONFIG.LOCAL_STORAGE_PATH.iterdir() if d.is_dir()]
    else:
        processing_function = partial(get_country_stats_s3, client=S3_CLIENT)
        response = S3_CLIENT.list_objects_v2(Bucket=CONFIG.S3_BUCKET, Delimiter="/")
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
