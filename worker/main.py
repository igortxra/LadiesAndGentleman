from datetime import datetime, timezone
from typing import Tuple
import os

from redis.asyncio import Redis
import httpx
import asyncio


STREAM_NAME = "payments"

PROCESSOR_URL = {
    "default": os.getenv(
        "PAYMENT_PROCESSOR_URL_DEFAULT", "http://payment-processor-default:8080"
    ),
    "fallback": os.getenv(
        "PAYMENT_PROCESSOR_URL_FALLBACK", "http://payment-processor-fallback:8080"
    ),
}

HEALTH_STATUS = {
    "default": {"failing": False, "minResponseTime": 0},
    "fallback": {"failing": False, "minResponseTime": 0},
}

DEFAULT_WORKERS = 25
FALLBACK_WORKERS = 20

BATCH = {
    "default": 1,
    "fallback": 1,
}

CONTINGENCY = {
    "default": False,
    "fallback": False,
}

RECOVERY_MODE = {"default": False, "fallback": False}

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379")

REDIS = Redis.from_url(REDIS_URL, decode_responses=True)


async def process_payment(
    msg_id, payload, client: httpx.AsyncClient, processor_name: str
) -> Tuple[bool, float]:
    requested_at = datetime.now(timezone.utc)
    payload["requestedAt"] = requested_at.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    score = requested_at.timestamp()
    response = await client.post(
        f"{PROCESSOR_URL[processor_name]}/payments", json=payload, timeout=10
    )
    if response.status_code == 200:
        await REDIS.zadd(f"{STREAM_NAME}:{processor_name}", {msg_id: score})
        await REDIS.xack(STREAM_NAME, "payment-workers", msg_id)
        return True, response.elapsed.total_seconds() * 1000

    RECOVERY_MODE[processor_name] = True
    HEALTH_STATUS[processor_name]["failing"] = True
    return False, 0


async def contingency_worker(processor_name: str):
    CONTINGENCY[processor_name] = True
    async with httpx.AsyncClient() as client:
        while HEALTH_STATUS[processor_name]["failing"]:
            msgs = await REDIS.xreadgroup(
                groupname="payment-workers",
                consumername=f"{processor_name}-contingency",
                streams={STREAM_NAME: ">"},
                count=1,
                block=5000,  # espera até 100ms por nova mensagem
            )

            if not msgs:
                RECOVERY_MODE[processor_name] = False
                CONTINGENCY[processor_name] = False
                return

            # Extrai a primeira mensagem
            for _, entries in msgs:
                for msg_id, payload in entries:
                    message_processed = False
                    while not message_processed:
                        response = await process_payment(
                            msg_id, payload, client, processor_name
                        )
                        success, minResponseTime = response

                        # Verifica se a falha foi resolvida
                        if success:
                            message_processed = True
                            if minResponseTime <= 100:
                                RECOVERY_MODE[processor_name] = False
                                HEALTH_STATUS[processor_name]["failing"] = False
                                HEALTH_STATUS[processor_name]["minResponseTime"] = (
                                    minResponseTime
                                )
                                return

                        await asyncio.sleep(0.005)

        RECOVERY_MODE[processor_name] = False
        CONTINGENCY[processor_name] = False
        return


async def worker(processor_name: str, consumer_name: str):
    limits = httpx.Limits(max_connections=1000, max_keepalive_connections=32)
    async with httpx.AsyncClient(limits=limits) as client:
        while True:
            batch = BATCH[processor_name]
            if RECOVERY_MODE[processor_name]:
                if not CONTINGENCY[processor_name]:
                    await asyncio.create_task(contingency_worker(processor_name))
                else:
                    await asyncio.sleep(0.005)
                    continue

            else:
                if HEALTH_STATUS[processor_name]["failing"]:
                    await asyncio.sleep(0.005)
                    continue

                if HEALTH_STATUS[processor_name]["minResponseTime"] > 100:
                    await asyncio.sleep(0.005)
                    continue

            stream_batch = await REDIS.xreadgroup(
                groupname="payment-workers",
                consumername=consumer_name,
                streams={STREAM_NAME: ">"},
                count=batch,
                block=0,
            )

            tasks = []
            for _, entries in stream_batch:
                for msg_id, payload in entries:
                    tasks.append(
                        process_payment(msg_id, payload, client, processor_name)
                    )

            if not tasks:
                await asyncio.sleep(0.005)
                continue

            await asyncio.gather(*tasks)


async def health_check(processor_name: str, delay: float = 0, interval=5):
    await asyncio.sleep(delay)
    async with httpx.AsyncClient() as client:
        while True:
            response = await client.get(
                PROCESSOR_URL[processor_name] + "/payments/service-health"
            )
            if response.status_code == 200:
                new_status = response.json()
                HEALTH_STATUS[processor_name] = new_status
                if new_status["failing"]:
                    RECOVERY_MODE[processor_name] = True
            else:
                HEALTH_STATUS[processor_name] = {
                    "failing": True,
                    "minResponseTime": HEALTH_STATUS[processor_name]["minResponseTime"],
                }

            await asyncio.sleep(interval)


async def main():
    try:
        await REDIS.xgroup_create(
            name=STREAM_NAME, groupname="payment-workers", id="0", mkstream=True
        )
    except Exception:
        # Group already created
        pass

    default_workers = [worker("default", f"d{i}") for i in range(DEFAULT_WORKERS)]
    fallback_workers = [worker("fallback", f"f{i}") for i in range(FALLBACK_WORKERS)]

    workers = [
        *default_workers,
        *fallback_workers,
        health_check("default", 0),
        health_check("fallback", 2.5),
    ]

    await asyncio.gather(*workers)


if __name__ == "__main__":
    asyncio.run(main())
