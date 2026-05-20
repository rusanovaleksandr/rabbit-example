import asyncio
import json
import os

import aio_pika

RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")
RACE_EXCHANGE = os.getenv("RACE_EXCHANGE", "race_events")
QUEUE_NAME = os.getenv("RABBITMQ_QUEUE_CONSUMER1", "race_events_consumer1")
ROUTING_KEYS = ["f1", "nascar", "lemans"]


async def consume_consumer1():
    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    channel = await connection.channel()
    await channel.set_qos(prefetch_count=50)

    exchange = await channel.declare_exchange(RACE_EXCHANGE, aio_pika.ExchangeType.DIRECT, durable=True)
    queue = await channel.declare_queue(QUEUE_NAME, durable=True)

    for routing_key in ROUTING_KEYS:
        await queue.bind(exchange, routing_key=routing_key)

    print("Starting Consumer 1...")

    async with queue.iterator() as iterator:
        async for message in iterator:
            async with message.process():
                event = json.loads(message.body.decode("utf-8"))

                # Эмуляция продолжительной обработки
                await asyncio.sleep(0.5)
                
                emoji_map = {"F1": "RACE", "NASCAR": "USA", "LeMans": "FR"}
                emoji = emoji_map.get(event["race"], "INFO")
                print(
                    f"[CONSUMER1] {emoji} {event['race']} UPDATE: {event['driver']} "
                    f"at P{event['position']} on lap {event['lap']} @ {event['timestamp']}"
                )


if __name__ == "__main__":
    asyncio.run(consume_consumer1())
