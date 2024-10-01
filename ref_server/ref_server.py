"""Reference server.

This server is used to test the client implementation.

```shell
pip install litestar==2.12.1 Faker==29.0.0
uvicorn ref_server:app
```

Schema is available at http://127.0.0.1:8080/schema (or http://127.0.0.1:8080/schema/swager).
"""

import asyncio
from datetime import datetime, timedelta
from random import choice, randint, random
from typing import Any

from faker import Faker
from litestar import Litestar, get
import uvicorn

faker = Faker()


COUTRIES = [faker.country() for _ in range(5)]
CITIES = {i: {"name": faker.city(), "country": choice(COUTRIES)} for i in range(30)}


@get("/cities")
async def get_cities() -> list[dict[str, str | int]]:
    return [{"id": city_id, **city_info} for city_id, city_info in CITIES.items()]


@get("/cities/{city_id:int}/stats", cache=True)
async def get_city_stats(city_id: int, date: datetime) -> list[dict[str, Any]]:
    await asyncio.sleep(random() * 4.2)
    return [
        {
            "departure-time": date + timedelta(minutes=randint(0, 720)),
            "bus-type": f"BUS-{randint(100, 113)}",
            "passengers": randint(5, 100),
            "delay": timedelta(minutes=randint(0, 90)),
            "accident": random() > 0.9,
        }
        for _ in range(randint(1000, 3000))
    ]


app = Litestar([get_cities, get_city_stats])

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8080)
