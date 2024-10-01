# test_app.py
import pytest
from moto import mock_aws
import boto3
from litestar.testing import TestClient
from app import app, S3_BUCKET, s3_client
import json
from datetime import datetime, timedelta

@pytest.fixture(scope='function')
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    import os
    os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
    os.environ['AWS_SECURITY_TOKEN'] = 'testing'
    os.environ['AWS_SESSION_TOKEN'] = 'testing'
    os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'

@pytest.fixture(scope='function')
def s3(aws_credentials):
    with mock_aws():
        yield boto3.client('s3', region_name='us-east-1')

@pytest.fixture(scope='function')
def test_client(s3):
    # Override the s3_client in the app with our mocked version
    app.state.s3_client = s3
    return TestClient(app=app)

def test_process_request(test_client):
    response = test_client.post("/process-request?date=2023-01-01T00:00:00")
    assert response.status_code == 200
    assert "task_id" in response.json()

def test_city_stats(test_client, s3):
    # Create a test bucket
    s3.create_bucket(Bucket=S3_BUCKET)

    # Upload some test data
    test_data = {
        "date": "2023-01-01",
        "countries": {
            "USA": {"buses_started": 100, "total_passengers": 5000, "accident": False, "avg_delay": 5},
            "Canada": {"buses_started": 80, "total_passengers": 4000, "accident": True, "avg_delay": 10}
        }
    }
    s3.put_object(
        Bucket=S3_BUCKET,
        Key="processed_data_2023-01-01T00:00:00.json",
        Body=json.dumps(test_data)
    )

    # Test the city_stats endpoint
    response = test_client.get("/city-stats?from=2023-01-01T00:00:00&to=2023-01-02T00:00:00")
    assert response.status_code == 200
    assert "2023-01-01T00:00:00" in response.json()
    assert response.json()["2023-01-01T00:00:00"] == test_data["countries"]

if __name__ == "__main__":
    pytest.main()