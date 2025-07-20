import asyncio
import os
from redis.asyncio import Redis
from datetime import datetime, timezone
import httpx


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


STREAM = "payments"
GROUP = "payment-workers"
CONSUMER_NAME = "consumer-1"
BATCH_SIZE = {
    "default": 100,
    "fallback": 100
}
BLOCK_TIME_MS = 500  # Espera até 500ms por mensagens
PROCESSOR_URL = {
    "default": os.getenv("PAYMENT_PROCESSOR_URL_DEFAULT", "http://payment-processor-default:8080"),
    "fallback": os.getenv("PAYMENT_PROCESSOR_URL_FALLBACK", "http://payment-processor-default:8080")
}
CURRENT_PROCESSOR = "default"
OTHER_PROCESSOR = "fallback"
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379")

r = Redis.from_url(REDIS_URL, decode_responses=True)

async def create_group():
    await r.xgroup_create(name=STREAM, groupname=GROUP, id="$", mkstream=True)

async def process_batch(messages, client: httpx.AsyncClient):
    for _, msgs in messages:
        async def send_and_ack(msg_id, payload):
            global BATCH_SIZE 
            global PROCESSOR_URL 
            global CURRENT_PROCESSOR 
            global OTHER_PROCESSOR 
            payload["requested_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
            resp = await client.post(f"{PROCESSOR_URL[CURRENT_PROCESSOR]}/payments", json=payload)
            if resp.status_code == 200:
                print(f"SUCESSO {CURRENT_PROCESSOR}")
                elapsed = resp.elapsed.total_seconds()
                if  elapsed >= 2:
                    BATCH_SIZE[CURRENT_PROCESSOR] = 1
                    _ = CURRENT_PROCESSOR 
                    CURRENT_PROCESSOR = OTHER_PROCESSOR
                    OTHER_PROCESSOR = _
                elif  elapsed >= 1:
                    BATCH_SIZE[CURRENT_PROCESSOR] = 1
                    if CURRENT_PROCESSOR == "fallback":
                        CURRENT_PROCESSOR = "default"
                        BATCH_SIZE["default"] = 1
                elif elapsed <= 0.3:
                    BATCH_SIZE[CURRENT_PROCESSOR] = 200
                elif elapsed <= 0.5:
                    if CURRENT_PROCESSOR == "fallback":
                        CURRENT_PROCESSOR = "default"
                        BATCH_SIZE["default"] = 1
                    else:
                        BATCH_SIZE[CURRENT_PROCESSOR] = 100
                elif elapsed > 0.5:
                    BATCH_SIZE[CURRENT_PROCESSOR] = 100
                    if CURRENT_PROCESSOR == "fallback":
                        CURRENT_PROCESSOR = "default"
                        BATCH_SIZE["default"] = 1
                return msg_id
            else:
                BATCH_SIZE[CURRENT_PROCESSOR] = 1
                _ = CURRENT_PROCESSOR 
                CURRENT_PROCESSOR = OTHER_PROCESSOR
                OTHER_PROCESSOR = _
                print(f"⚠️ Falha HTTP {resp.status_code} para msg {msg_id}: {payload}")

        tasks = [send_and_ack(msg_id, payload) for msg_id, payload in msgs]
        results = await asyncio.gather(*tasks)

        ids_to_ack = [msg_id for msg_id in results if msg_id]
        if ids_to_ack:
            await r.xack(STREAM, GROUP, *ids_to_ack)

async def worker_loop():
    await create_group()
    async with httpx.AsyncClient() as client:
        while True:
            messages = await r.xreadgroup(
                groupname=GROUP,
                consumername=CONSUMER_NAME,
                streams={STREAM: '>'},
                count=BATCH_SIZE[CURRENT_PROCESSOR],
                block=BLOCK_TIME_MS
            )
            if messages:
                print(f"LOTE SENDO ENVIADO!!!! {BATCH_SIZE}")
                await process_batch(messages, client)

if __name__ == "__main__":
    asyncio.run(worker_loop())

