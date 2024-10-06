import logging

from litestar import Litestar
from modules.config import CONFIG
from modules.routes import country_stats, process_request

app = Litestar([process_request, country_stats])

if __name__ == "__main__":
    import uvicorn

    logging.info(f"Starting the reference server {CONFIG}")

    uvicorn.run(app, host="0.0.0.0", port=8000)
