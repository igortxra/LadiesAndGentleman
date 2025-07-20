import asyncio
import redis
import httpx
import json
import os

REDIS_URL = os.getenv("REDIS_URL")
QUEUE_NAME = "payments"
BATCH_SIZE = 500
ENDPOINT_URL = "http://payment-processor-default:8080"  # altere para seu destino

async def worker():
    redis_client = redis.from_url(REDIS_URL, decode_responses=True)
    async with httpx.AsyncClient() as client:
        while True:
            batch = redis_client.lpop(QUEUE_NAME, count=BATCH_SIZE)

            if not batch:
                continue

            # Converte cada item do batch (assumindo que são JSON)
            payloads = [json.loads(item) for item in batch]

            async def enviar(item):
                try:
                    r = await client.post(f"{ENDPOINT_URL}/payments", json=item)
                except Exception as e:
                    print("Falha ao enviar item:", e)

            # Envia em paralelo
            await asyncio.gather(*[enviar(p) for p in payloads])

# async def worker():
#     redis_client = redis.from_url(REDIS_URL, decode_responses=True)
#     async with httpx.AsyncClient() as client:
#         while True:
#             batch = redis_client.lpop(QUEUE_NAME, count=BATCH_SIZE)
#
#             if not batch:
#                 await asyncio.sleep(1)  # Evita busy-wait
#                 continue
#
#             # tasks = [process_item(client, redis_client, item) for item in batch]
#             # await asyncio.gather(*tasks)
#             # Aqui você pode processar o batch
if __name__ == "__main__":
    asyncio.run(worker())
