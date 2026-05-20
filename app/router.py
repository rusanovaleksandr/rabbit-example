from datetime import datetime
import asyncio
import json
import os
import random

import aio_pika
from fastapi import APIRouter
from schema import RaceEvent

route = APIRouter()

RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")
RACE_EXCHANGE = os.getenv("RACE_EXCHANGE", "race_events")
PUBLISH_INTERVAL_SECONDS = float(os.getenv("PUBLISH_INTERVAL_SECONDS", "2"))

races_data = {
    "F1": {
        "routing_key": "f1",
        "drivers": ["Hamilton", "Verstappen", "Sainz", "Leclerc"],
        "max_laps": 70,
    },
    "NASCAR": {
        "routing_key": "nascar",
        "drivers": ["Johnson", "Elliott", "Logano", "Larson"],
        "max_laps": 400,
    },
    "LeMans": {
        "routing_key": "lemans",
        "drivers": ["Porsche Team", "Ferrari Team", "Toyota Team", "BMW Team"],
        "max_laps": 350,
    },
}

race_state = {
    "F1": {"current_lap": 0, "positions": [1, 2, 3, 4]},
    "NASCAR": {"current_lap": 0, "positions": [1, 2, 3, 4]},
    "LeMans": {"current_lap": 0, "positions": [1, 2, 3, 4]},
}

_publish_task = None
_task_lock = asyncio.Lock()


async def _race_publish_loop():
    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    channel = await connection.channel()
    exchange = await channel.declare_exchange(RACE_EXCHANGE, aio_pika.ExchangeType.DIRECT, durable=True)

    iteration = 0
    try:
        while True:
            iteration += 1

            for race_name, race_info in races_data.items():
                state = race_state[race_name]
                state["current_lap"] = (state["current_lap"] + 1) % race_info["max_laps"]

                for idx, driver in enumerate(race_info["drivers"]):
                    position = state["positions"][idx]
                    event = RaceEvent(
                        race=race_name,
                        driver=driver,
                        position=position,
                        lap=state["current_lap"],
                        timestamp=datetime.utcnow().isoformat(),
                    )
                    payload = json.dumps(event.model_dump()).encode("utf-8")
                    await exchange.publish(
                        aio_pika.Message(
                            body=payload,
                            content_type="application/json",
                            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                        ),
                        routing_key=race_info["routing_key"],
                    )
                    print(f"[PRODUCER] {race_name}: {driver} at P{position} on lap {state['current_lap']}")

            if iteration % 5 == 0:
                for race_name in race_state:
                    positions = race_state[race_name]["positions"]
                    i, j = random.sample(range(len(positions)), 2)
                    positions[i], positions[j] = positions[j], positions[i]

            await asyncio.sleep(PUBLISH_INTERVAL_SECONDS)
    except asyncio.CancelledError:
        print("[PRODUCER] Publish loop cancelled")
        raise
    finally:
        await channel.close()
        await connection.close()


@route.post("/start_race_events")
async def start_race_events():
    global _publish_task

    async with _task_lock:
        if _publish_task and not _publish_task.done():
            return {"status": "Race events are already running"}

        _publish_task = asyncio.create_task(_race_publish_loop())
        return {"status": "Race events started"}


@route.post("/stop_race_events")
async def stop_race_events():
    global _publish_task

    async with _task_lock:
        if not _publish_task or _publish_task.done():
            return {"status": "Race events are not running"}

        _publish_task.cancel()
        try:
            await _publish_task
        except asyncio.CancelledError:
            pass
        return {"status": "Race events stopped"}


@route.get("/race_status")
async def get_race_status():
    return race_state
