import asyncio
import datetime
import logging
import contextlib
import os
import time

from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync


# REQUIRED CONFIGURATION:
# Replace all placeholder values before running the controllers.
database = {
    "url": "REPLACE_WITH_INFLUXDB_URL",
    "token": "REPLACE_WITH_INFLUXDB_TOKEN",
    "org": "REPLACE_WITH_INFLUXDB_ORG",
    "bucket": "REPLACE_WITH_INFLUXDB_BUCKET",
}


_PLACEHOLDER_PREFIX = "REPLACE_WITH_"


def _assert_database_configured() -> None:
    missing = [
        key
        for key, value in database.items()
        if not isinstance(value, str) or not value or value.startswith(_PLACEHOLDER_PREFIX)
    ]
    if missing:
        raise RuntimeError(
            "commons.py contains placeholder InfluxDB values for: "
            + ", ".join(missing)
            + ". Edit the 'database' configuration before running."
        )


def get_logger(name: str):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    
    if len(logger.handlers) == 0:
        os.makedirs("logs", exist_ok=True)
        fh = logging.FileHandler(filename=f'logs/{datetime.datetime.now()}_{name}.log', mode='a')
        fh.setLevel(logging.DEBUG)

        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)

        formatter = logging.Formatter('%(asctime)s %(name)s [%(levelname)s] %(message)s')
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)

        logger.addHandler(fh)
        logger.addHandler(ch)

    return logger

async def iterate(interval: int, duration: int):
    iteration = 0
    delay_ns = interval * 10**9
    timer = time.perf_counter_ns()

    while True:
        iteration += 1
        yield iteration
        timer += delay_ns
        if iteration * interval >= duration:
            break
        await asyncio.sleep((timer - time.perf_counter_ns()) / 10**9)

async def write_to_database(measurement: str, tags: dict, fields: dict):
    _assert_database_configured()
    logger = logging.getLogger()
    data = {
        "measurement": measurement,
        "tags": tags,
        "fields": fields
    }
    async with contextlib.AsyncExitStack() as stack:
        influx = await stack.enter_async_context(InfluxDBClientAsync(**database))
        try:
            await influx.write_api().write(bucket=database.get("bucket"), record=data)
            logger.info("Wrote %d records", len(data))
        except Exception as error:
            logger.error(error)
