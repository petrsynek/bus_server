from datetime import date
from io import BytesIO
from unittest.mock import AsyncMock, patch

import boto3
import pandas as pd
import pytest
from moto import mock_aws
from mypy_boto3_s3 import S3Client

from bus_server.modules.config import CONFIG
from bus_server.modules.data_processing import (
    gather_city_data,
    get_country_stats_local,
    get_country_stats_s3,
    iso_duration_to_seconds,
    process_data_task_local,
)


def test_iso_duration_to_seconds_valid():
    assert iso_duration_to_seconds("PT10M") == 600  # 10 minutes in seconds
    assert iso_duration_to_seconds("PT1H") == 3600  # 1 hour in seconds
    assert iso_duration_to_seconds("PT1H30M") == 5400  # 1 hour 30 minutes in seconds
    assert iso_duration_to_seconds("P1DT1H") == 90000  # 1 day 1 hour in seconds


def test_iso_duration_to_seconds_invalid():
    assert iso_duration_to_seconds("invalid") == "invalid"
    assert iso_duration_to_seconds("") == ""
    assert iso_duration_to_seconds(None) is None
    assert iso_duration_to_seconds(123) == 123


def test_iso_duration_to_seconds_edge_cases():
    assert iso_duration_to_seconds("PT0S") == 0  # 0 seconds
    assert iso_duration_to_seconds("PT0.5S") == 0  # 0.5 seconds rounded down to 0


@pytest.mark.asyncio
@patch("aiohttp.ClientSession.get")
async def test_gather_city_data_success(mock_get) -> None:
    requested_date = date(2023, 10, 1)
    city_id = 1
    city = "TestCity"
    mock_data = [
        {"bus-type": "A", "passengers": 10, "accident": 0, "delay": "PT10M"},
        {"bus-type": "B", "passengers": 20, "accident": 1, "delay": "PT20M"},
    ]

    mock_get.return_value.__aenter__.return_value.json = AsyncMock(
        side_effect=[mock_data]
    )

    result = await gather_city_data(requested_date, city_id, city)

    assert isinstance(result, pd.DataFrame)
    assert not result.empty
    assert result["delay"].iloc[0] == 600  # 10 minutes in seconds
    assert result["delay"].iloc[1] == 1200  # 20 minutes in seconds


@pytest.mark.asyncio
@patch("aiohttp.ClientSession.get")
async def test_gather_city_data_no_data(mock_get) -> None:
    requested_date = date(2023, 10, 1)
    city_id = 1
    city = "TestCity"
    mock_data: list[str] = []

    mock_get.return_value.__aenter__.return_value.json = AsyncMock(
        side_effect=[mock_data]
    )

    result = await gather_city_data(requested_date, city_id, city)

    assert isinstance(result, pd.DataFrame)
    assert result.empty


@pytest.mark.asyncio
@patch("aiohttp.ClientSession.get")
async def test_gather_city_data_invalid_delay(mock_get) -> None:
    requested_date = date(2023, 10, 1)
    city_id = 1
    city = "TestCity"
    mock_data = [
        {"bus-type": "A", "passengers": 10, "accident": 0, "delay": "invalid"},
    ]

    mock_get.return_value.__aenter__.return_value.json = AsyncMock(
        side_effect=[mock_data]
    )

    result = await gather_city_data(requested_date, city_id, city)

    assert isinstance(result, pd.DataFrame)
    assert not result.empty
    assert result["delay"].iloc[0] == "invalid"


@pytest.mark.asyncio
@patch("aiohttp.ClientSession.get")
async def test_process_data_task_local_success(mock_get, tmp_path) -> None:
    requested_date = date(2023, 10, 1)
    city_id = 1
    city = "TestCity"
    country = "TestCountry"
    mock_data = [
        {"bus-type": "A", "passengers": 10, "accident": 0, "delay": "PT10M"},
        {"bus-type": "B", "passengers": 20, "accident": 1, "delay": "PT20M"},
    ]

    mock_get.return_value.__aenter__.return_value.json = AsyncMock(
        side_effect=[mock_data]
    )

    local_path = tmp_path / "data"
    await process_data_task_local(requested_date, city_id, city, country, local_path)

    file_path = local_path / f"{country}/{requested_date}/{city}/data.parquet"
    assert file_path.exists()

    df = pd.read_parquet(file_path)
    assert not df.empty
    assert df["delay"].iloc[0] == 600  # 10 minutes in seconds
    assert df["delay"].iloc[1] == 1200  # 20 minutes in seconds


@pytest.mark.asyncio
@patch("aiohttp.ClientSession.get")
async def test_process_data_task_local_no_data(mock_get, tmp_path) -> None:
    requested_date = date(2023, 10, 1)
    city_id = 1
    city = "TestCity"
    country = "TestCountry"
    mock_data: list[str] = []

    mock_get.return_value.__aenter__.return_value.json = AsyncMock(
        side_effect=[mock_data]
    )

    local_path = tmp_path / "data"
    await process_data_task_local(requested_date, city_id, city, country, local_path)

    file_path = local_path / f"{country}/{requested_date}/{city}/data.parquet"
    assert file_path.exists()

    df = pd.read_parquet(file_path)
    assert df.empty


@pytest.mark.asyncio
@patch("aiohttp.ClientSession.get")
async def test_process_data_task_local_invalid_delay(mock_get, tmp_path) -> None:
    requested_date = date(2023, 10, 1)
    city_id = 1
    city = "TestCity"
    country = "TestCountry"
    mock_data = [
        {"bus-type": "A", "passengers": 10, "accident": 0, "delay": "invalid"},
    ]

    mock_get.return_value.__aenter__.return_value.json = AsyncMock(
        side_effect=[mock_data]
    )

    local_path = tmp_path / "data"
    await process_data_task_local(requested_date, city_id, city, country, local_path)

    file_path = local_path / f"{country}/{requested_date}/{city}/data.parquet"
    assert file_path.exists()

    df = pd.read_parquet(file_path)
    assert not df.empty
    assert df["delay"].iloc[0] == "invalid"


def test_get_country_stats_local_success(tmp_path) -> None:
    country = "TestCountry"
    requested_date = date(2023, 10, 1)
    local_path = tmp_path / "data"
    city = "TestCity"
    city_data = [
        {"bus-type": "A", "passengers": 10, "accident": 0, "delay": 600},
        {"bus-type": "B", "passengers": 20, "accident": 1, "delay": 1200},
    ]

    city_path = local_path / f"{country}/{requested_date}/{city}"
    city_path.mkdir(parents=True, exist_ok=True)
    city_file = city_path / "data.parquet"
    pd.DataFrame(city_data).to_parquet(city_file)

    stats = get_country_stats_local(country, requested_date, local_path)

    assert stats["number_of_buses"] == 2
    assert stats["total_passengers"] == 30
    assert stats["total_accidents"] == 1
    assert stats["average_delay"] == 900.0  # (600 + 1200) / 2


def test_get_country_stats_local_no_data(tmp_path) -> None:
    country = "TestCountry"
    requested_date = date(2023, 10, 1)
    local_path = tmp_path / "data"

    stats = get_country_stats_local(country, requested_date, local_path)

    assert stats == {}


def test_get_country_stats_local_multiple_cities(tmp_path) -> None:
    country = "TestCountry"
    requested_date = date(2023, 10, 1)
    local_path = tmp_path / "data"
    city1 = "TestCity1"
    city2 = "TestCity2"
    city1_data = [
        {"bus-type": "A", "passengers": 10, "accident": 0, "delay": 600},
    ]
    city2_data = [
        {"bus-type": "B", "passengers": 20, "accident": 1, "delay": 1200},
    ]

    city1_path = local_path / f"{country}/{requested_date}/{city1}"
    city1_path.mkdir(parents=True, exist_ok=True)
    city1_file = city1_path / "data.parquet"
    pd.DataFrame(city1_data).to_parquet(city1_file)

    city2_path = local_path / f"{country}/{requested_date}/{city2}"
    city2_path.mkdir(parents=True, exist_ok=True)
    city2_file = city2_path / "data.parquet"
    pd.DataFrame(city2_data).to_parquet(city2_file)

    stats = get_country_stats_local(country, requested_date, local_path)

    assert stats["number_of_buses"] == 2
    assert stats["total_passengers"] == 30
    assert stats["total_accidents"] == 1
    assert stats["average_delay"] == 900.0  # (600 + 1200) / 2


def test_get_country_stats_s3_success() -> None:
    with mock_aws():
        country = "TestCountry"
        requested_date = date(2023, 10, 1)
        s3_client: S3Client = boto3.client("s3", region_name="us-east-1")
        s3_client.create_bucket(Bucket=CONFIG.S3_BUCKET)

        city = "TestCity"
        city_data = [
            {"bus-type": "A", "passengers": 10, "accident": 0, "delay": 600},
            {"bus-type": "B", "passengers": 20, "accident": 1, "delay": 1200},
        ]

        buffer = BytesIO()
        pd.DataFrame(city_data).to_parquet(buffer, index=False)
        buffer.seek(0)

        s3_client.put_object(
            Bucket=CONFIG.S3_BUCKET,
            Key=f"{country}/{requested_date}/{city}/data.parquet",
            Body=buffer.getvalue(),
        )

        stats = get_country_stats_s3(country, requested_date, s3_client)

        assert stats["number_of_buses"] == 2
        assert stats["total_passengers"] == 30
        assert stats["total_accidents"] == 1
        assert stats["average_delay"] == 900.0  # (600 + 1200) / 2


def test_get_country_stats_s3_no_data() -> None:
    with mock_aws():
        country = "TestCountry"
        requested_date = date(2023, 10, 1)
        s3_client: S3Client = boto3.client("s3", region_name="us-east-1")
        s3_client.create_bucket(Bucket=CONFIG.S3_BUCKET)

        stats = get_country_stats_s3(country, requested_date, s3_client)

        assert stats == {}


def test_get_country_stats_s3_multiple_cities() -> None:
    with mock_aws():
        country = "TestCountry"
        requested_date = date(2023, 10, 1)
        s3_client: S3Client = boto3.client("s3", region_name="us-east-1")
        s3_client.create_bucket(Bucket=CONFIG.S3_BUCKET)

        city1 = "TestCity1"
        city2 = "TestCity2"
        city1_data = [
            {"bus-type": "A", "passengers": 10, "accident": 0, "delay": 600},
        ]
        city2_data = [
            {"bus-type": "B", "passengers": 20, "accident": 1, "delay": 1200},
        ]

        buffer1 = BytesIO()
        pd.DataFrame(city1_data).to_parquet(buffer1, index=False)
        buffer1.seek(0)
        s3_client.put_object(
            Bucket=CONFIG.S3_BUCKET,
            Key=f"{country}/{requested_date}/{city1}/data.parquet",
            Body=buffer1.getvalue(),
        )

        buffer2 = BytesIO()
        pd.DataFrame(city2_data).to_parquet(buffer2, index=False)
        buffer2.seek(0)
        s3_client.put_object(
            Bucket=CONFIG.S3_BUCKET,
            Key=f"{country}/{requested_date}/{city2}/data.parquet",
            Body=buffer2.getvalue(),
        )

        stats = get_country_stats_s3(country, requested_date, s3_client)

        assert stats["number_of_buses"] == 2
        assert stats["total_passengers"] == 30
        assert stats["total_accidents"] == 1
        assert stats["average_delay"] == 900.0  # (600 + 1200) / 2
