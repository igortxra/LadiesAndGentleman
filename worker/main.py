import asyncio
from datetime import datetime, timezone
import os
from typing import Tuple
from redis.asyncio import Redis
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
    "default": {"failing": False, "minResponseTime": 0},
    "fallback": {"failing": False, "minResponseTime": 0}
}

# RECOVERY_ONLY = {
#     "default": False,
#     "fallback": False
# }

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

RECOVERY_MODE = {
    "default": False,
    "fallback": False
}

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379")
r = Redis.from_url(REDIS_URL, decode_responses=True)


async def process_payment_msg(msg_id, payload, url, client: httpx.AsyncClient, processor_name: str, other_name: str) -> Tuple[bool, float]:
    try:
        req_at = datetime.now(timezone.utc)
        payload['requestedAt'] =  req_at.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        
        # MAIN PROCESSOR
        response = await client.post(f"{url}/payments", json=payload, timeout=10)
        if response.status_code == 200:
            await r.xack(STREAM_NAME, "payment-workers", msg_id)
            score = req_at.timestamp()
            await r.zadd(f"{STREAM_NAME}:{processor_name}", {msg_id: score})
            return True, response.elapsed.total_seconds() * 1000
    except (httpx.ConnectError, httpx.ConnectTimeout):
        pass

    RECOVERY_MODE[processor_name] = True
    HEALTH_STATUS[processor_name]["failing"] = True
    # BATCH[other_name] = 2
    return False, 0
    


async def contingency_worker(processor_name: str, other_name: str):
    CONTINGENCY[processor_name] = True
    async with httpx.AsyncClient() as client:
        while HEALTH_STATUS[processor_name]["failing"]:
            msgs = await r.xreadgroup(
                groupname='payment-workers',
                consumername=f"{processor_name}-contingency",
                streams={STREAM_NAME: '>'},
                count=1,
                block=5000  # espera até 100ms por nova mensagem
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
                        try:
                            res = await process_payment_msg(msg_id, payload, PROCESSOR_URL[processor_name], client, processor_name, other_name)
                            success, minResponseTime = res

                            # Verifica se a falha foi resolvida
                            if success:
                                message_processed = True
                                if minResponseTime <= 100:
                                    RECOVERY_MODE[processor_name] = False
                                    HEALTH_STATUS[processor_name]["failing"] = False
                                    HEALTH_STATUS[processor_name]["minResponseTime"] = minResponseTime
                                    continue

                        except Exception:
                            pass

                        await asyncio.sleep(0.005)

                    await asyncio.sleep(0.005)

        RECOVERY_MODE[processor_name] = False
        CONTINGENCY[processor_name] = False
        return


async def worker_loop(processor_name: str, consumer_name: str, other_name: str):
    limits = httpx.Limits(max_connections=1000, max_keepalive_connections=32)
    async with httpx.AsyncClient(limits=limits) as client:
        while True:
            batch = BATCH[processor_name]
            if RECOVERY_MODE[processor_name]:
                if not CONTINGENCY[processor_name]:
                    await asyncio.create_task(contingency_worker(processor_name, other_name))
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
            
            # BUSCA SEMPRE AS MAIS RECENTES PARA EVITAR QUE REQUISIÇÕES ATRASADAS, ESSE LOTE TEM QUE SER PROCESSADO EM ATÉ 100ms
            msgs = await r.xreadgroup(groupname='payment-workers', consumername=consumer_name, streams={STREAM_NAME: '>'}, count=batch, block=0)
            tasks = []
            len_tasks = 0
            for _, entries in msgs:
                for msg_id, payload in entries:
                    len_tasks += 1
                    tasks.append(process_payment_msg(msg_id, payload, PROCESSOR_URL[processor_name], client, processor_name, other_name))

            if tasks:
                await asyncio.gather(*tasks)
            else:
                await asyncio.sleep(0.005)

            # await asyncio.sleep(DELAY_SECONDS[processor_name])


async def health_check(processor_name: str, other_name: str, delay: float = 0, interval = 5):
    await asyncio.sleep(delay)
    async with httpx.AsyncClient() as client:
        while True:
            response = await client.get(PROCESSOR_URL[processor_name] + "/payments/service-health")
            if response.status_code == 200:
                new_status = response.json()
                HEALTH_STATUS[processor_name] = new_status
                if new_status["failing"]:
                    RECOVERY_MODE[processor_name] = True
            else:
                HEALTH_STATUS[processor_name] = {"failing": True, "minResponseTime": HEALTH_STATUS[processor_name]["minResponseTime"]}

            await asyncio.sleep(interval)


async def main():

    try:
        await r.xgroup_create(name=STREAM_NAME, groupname="payment-workers", id="0", mkstream=True)
    except Exception:
        pass

    default_workers = [worker_loop("default", f"d{i}", "fallback") for i in range(DEFAULT_WORKERS)]
    fallback_workers = [worker_loop("fallback", f"f{i}", "default") for i in range(FALLBACK_WORKERS)]
    
    workers = [*default_workers, *fallback_workers, health_check("default", "fallback", 0), health_check("fallback", "default", 2.5)]

    await asyncio.gather(*workers)

if __name__ == "__main__":
    asyncio.run(main())
