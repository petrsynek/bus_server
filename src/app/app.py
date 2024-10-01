# app.py
from litestar import Litestar, get, post
from litestar.params import Parameter
from datetime import datetime
from celery import Celery
import boto3
import json
import os

# Initialize Celery
celery_app = Celery('tasks', broker='redis://localhost:6379/0')

# Initialize S3 client
s3_client = boto3.client('s3', region_name='us-east-1')

S3_BUCKET = os.environ.get('S3_BUCKET', 'your-s3-bucket-name')

@celery_app.task
def process_data_task(date):
    print(f"Processing data for date: {date}")
    dummy_data = {
        "date": date,
        "countries": {
            "USA": {"buses_started": 100, "total_passengers": 5000, "accident": False, "avg_delay": 5},
            "Canada": {"buses_started": 80, "total_passengers": 4000, "accident": True, "avg_delay": 10}
        }
    }
    app.state.s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=f"processed_data_{date}.json",
        Body=json.dumps(dummy_data)
    )

@post("/process-request")
async def process_request(date: datetime = Parameter(query="date")):
    task = process_data_task.delay(date.isoformat())
    return {"message": "Task registered", "task_id": task.id}

@get("/city-stats")
async def city_stats(from_date: datetime = Parameter(query="from"), to_date: datetime = Parameter(query="to")):
    current_date = from_date
    stats = {}
    
    while current_date <= to_date:
        try:
            response = app.state.s3_client.get_object(
                Bucket=S3_BUCKET,
                Key=f"processed_data_{current_date.isoformat()}.json"
            )
            data = json.dumps(response['Body'].read().decode('utf-8'))
            stats[current_date.isoformat()] = data['countries']
        except app.state.s3_client.exceptions.NoSuchKey:
            # No data for this date
            pass
        current_date = current_date.replace(day=current_date.day + 1)
    
    return stats

app = Litestar([process_request, city_stats])
app.state.s3_client = s3_client

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)