import asyncio
import os
from redis.asyncio import Redis
from datetime import datetime, timezone
import httpx
import asyncio
from dateutil.parser import isoparse




# ENVIO EM LOTES QUE VARIAM CONFORME DISPOBIBILIDADE

# DAR PREFERENCIA A DEFAULT PARA MAIS LUCRO

# SABER O MAIS RAPIDO POSSIVEL QUANDO U SERVIÇO FICA DOWN
# SABER O MAIS RAPIDO POSSIVEL QUANDO UM SERViÇO SE RECUPEROU
# SABER O MAIS RAPIDO POSSIVEL QUANDO UM SERVIÇO ESTÁ LENTO

# ENVIAR PARALELAMENTE AOS DOIS A CADA 100ms QUANDO OS DOIS CAIREM ATÉ QUE TENDO SUCESSO
# ENVIAR ALTO VOLUME (500) PARA DEFAULT QUANDO O TEMPO DE RESPOSTA FOR BAIXO < 1000ms
# ENVIAR ALTO VOLUME (500) 'PARA FALLBACK QUANDO DEFAULT ESTIVER DOWN ou O TEMPO DE RESPOSTA FOR >= 1000ms

# REDUZIR VOLUME QUANDO TEMPO DE RESPOSTA FOR > 500ms e < 1000ms
# Considerar DOWN se TMEPO DE RESPOSTA (do health check) >= 1000ms

# PROCESSOR_URL = {
#     "default": os.getenv("PAYMENT_PROCESSOR_URL_DEFAULT", "http://payment-processor-default:8080"),
#     "fallback": os.getenv("PAYMENT_PROCESSOR_URL_FALLBACK", "http://payment-processor-fallback:8080")
# }

STREAM_NAME = "payments"

PROCESSOR_URL = {
    "default": os.getenv("PAYMENT_PROCESSOR_URL_DEFAULT", "http://payment-processor-default:8080"),
    "fallback": os.getenv("PAYMENT_PROCESSOR_URL_FALLBACK", "http://payment-processor-fallback:8080")
}

HEALTH_STATUS = {
    "default": {"failing": False, "minResponseTime": 0, "last_failing": False},
    "fallback": {"failing": False, "minResponseTime": 0, "last_failing": False}
}

DELAY_SECONDS = {
    "default": 0,
    "fallback": 0,
}

DEFAULT_WORKERS = 2
FALLBACK_WORKERS = 2


REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379")
r = Redis.from_url(REDIS_URL, decode_responses=True)


async def process_payment_msg(msg_id, payload, url, client: httpx.AsyncClient, processor_name: str):
    try:
        # req_at = datetime.now(timezone.utc)
        req_at = isoparse(payload["requestedAt"])

        response = await client.post(f"{url}/payments", json=payload, timeout=10)
        if response.status_code == 200:
            await r.xack(STREAM_NAME, "payment-workers", msg_id)
            # STORE
            score = req_at.timestamp()
            # if response.elapsed.total_seconds() >= 1:
            #     await asyncio.sleep(0.1)
            await r.zadd(f"{STREAM_NAME}:{processor_name}", {msg_id: score})
            return


    except (httpx.ConnectError, httpx.ConnectTimeout):
        HEALTH_STATUS[processor_name]["failing"] = True

async def worker_loop(processor_name: str, consumer_name: str):
    async with httpx.AsyncClient() as client:
        while True:
            if HEALTH_STATUS[processor_name]["failing"]:
                await asyncio.sleep(0.01)
                continue

            if HEALTH_STATUS[processor_name]["minResponseTime"] > 1000:
                await asyncio.sleep(0.01)
                continue
    
            if HEALTH_STATUS[processor_name]["last_failing"]:
                pending = await r.xpending_range(STREAM_NAME, "payment-workers", "-", "+", 100)
                to_claim = [p.message_id for p in pending if p.idle >= 60000]
                await r.xclaim(
                    STREAM_NAME,
                    "payment-workers",
                    consumername=consumer_name,
                    min_idle_time=60000,
                    message_ids=to_claim)
            
            msgs = await r.xreadgroup(groupname='payment-workers', consumername=consumer_name, streams={STREAM_NAME: '>'}, count=200, block=50)
            tasks = []
            for _, entries in msgs:
                for msg_id, payload in entries:
                    parsed = {k: v for k, v in payload.items()}
                    tasks.append(process_payment_msg(msg_id, parsed, PROCESSOR_URL[processor_name], client, processor_name))

            await asyncio.gather(*tasks)
            
            await asyncio.sleep(DELAY_SECONDS[processor_name])


async def health_check(processor_name: str, delay: float = 0, interval = 5):
    await asyncio.sleep(delay)
    while True:
        response = httpx.get(PROCESSOR_URL[processor_name] + "/payments/service-health")
        if response.status_code == 200:
            last_failing = HEALTH_STATUS[processor_name]["last_failing"]
            HEALTH_STATUS[processor_name] = response.json()
            HEALTH_STATUS[processor_name]["last_failing"] = last_failing
        else:
            HEALTH_STATUS[processor_name] = {"failing": True, "minResponseTime": 9999, "last_failing": HEALTH_STATUS[processor_name]["last_failing"]}

        await asyncio.sleep(interval)


async def main():

    try:
        await r.xgroup_create(name=STREAM_NAME, groupname="payment-workers", id="0", mkstream=True)
    except Exception:
        pass

    default_workers = [worker_loop("default", f"d{i}") for i in range(DEFAULT_WORKERS)]
    fallback_workers = [worker_loop("fallback", f"f{i}") for i in range(FALLBACK_WORKERS)]
    
    workers = [*default_workers, *fallback_workers, health_check("default"), health_check("fallback", 2.5)]

    await asyncio.gather(*workers)

if __name__ == "__main__":
    asyncio.run(main())
