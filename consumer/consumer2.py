import asyncio
from collections import defaultdict
import json
import os

import aio_pika

RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")
RACE_EXCHANGE = os.getenv("RACE_EXCHANGE", "race_events")
QUEUE_NAME = os.getenv("RABBITMQ_QUEUE_CONSUMER2", "race_events_consumer2")
ROUTING_KEYS = ["f1", "nascar", "lemans"]

leaders = defaultdict(lambda: {"driver": "Unknown", "position": 999, "lap": 0})


async def consume_consumer2():
    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    channel = await connection.channel()
    await channel.set_qos(prefetch_count=50)

    exchange = await channel.declare_exchange(RACE_EXCHANGE, aio_pika.ExchangeType.DIRECT, durable=True)
    queue = await channel.declare_queue(QUEUE_NAME, durable=True)

    for routing_key in ROUTING_KEYS:
        await queue.bind(exchange, routing_key=routing_key)

    message_count = defaultdict(int)
    print("Starting Consumer 2 (Leaders Tracker)...")

    async with queue.iterator() as iterator:
        async for message in iterator:
            async with message.process():
                event = json.loads(message.body.decode("utf-8"))
                race = event["race"]
                message_count[race] += 1

                if event["position"] == 1:
                    leaders[race] = {
                        "driver": event["driver"],
                        "position": event["position"],
                        "lap": event["lap"],
                    }

                if sum(message_count.values()) % 12 == 0:
                    print("\n" + "=" * 60)
                    print("[CONSUMER2] CURRENT LEADERS:")
                    print("=" * 60)
                    for race_name in ["F1", "NASCAR", "LeMans"]:
                        leader_info = leaders[race_name]
                        print(
                            f"  {race_name:8} -> {leader_info['driver']:15} | "
                            f"Lap {leader_info.get('lap', 0):3} | Messages: {message_count[race_name]}"
                        )
                    print("=" * 60 + "\n")


if __name__ == "__main__":
    asyncio.run(consume_consumer2())
