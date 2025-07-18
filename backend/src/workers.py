import asyncio
import random
import logging
import requests
from datetime import datetime, timezone
import os
import redis.asyncio as aioredis
import json

from .config import PROCESSOR_URLS
from .models import PaymentRequest

logger = logging.getLogger(__name__)

best_processor_to_use = "default"

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379")
PAYMENT_QUEUE_KEY = "payments:queue"
PAYMENTS_SET_KEY_PREFIX = "payments:processed"

async def get_redis_client():
    return aioredis.from_url(REDIS_URL, decode_responses=True)

async def store_successful_payment(redis_client: aioredis.Redis, processor_used: str, payment: PaymentRequest):
    """Armazena um pagamento bem-sucedido no Redis."""
    try:
        key = f"{PAYMENTS_SET_KEY_PREFIX}:{processor_used}"
        
        # Converte a data para timestamp para usar como score
        dt_object = datetime.fromisoformat(payment.requestedAt.replace('Z', '+00:00'))
        score = int(dt_object.timestamp() * 1000)

        # O membro do set será o próprio objeto de pagamento em JSON
        member = json.dumps(payment.__dict__)
        
        await redis_client.zadd(key, {member: score})
        logger.info(f"Stored payment {payment.correlationId} for {processor_used} with score {score}")

    except Exception as e:
        logger.error(f"Error storing successful payment {payment.correlationId} to Redis: {e}")

async def update_best_processor():
    global best_processor_to_use
    redis_client = await get_redis_client()
    while True:
        try:
            value = await redis_client.get("processor:best")
            if value in ("default", "fallback", "none"):
                best_processor_to_use = value
                logger.info(f"[Redis] Updated best_processor_to_use: {value}")
            else:
                best_processor_to_use = "none"
        except Exception as e:
            logger.warning(f"[Redis] Error fetching best processor: {e}")
            best_processor_to_use = "none"
        await asyncio.sleep(5)

def send_payment_to_processor(url: str, payment: PaymentRequest) -> bool:
    try:
        response = requests.post(
            url + "/payments",
            json=payment.__dict__
        )
        return response.status_code == 200
    except Exception as e:
        return False

async def payment_worker():
    redis_client = await get_redis_client()
    while True:
        result = await redis_client.blpop(PAYMENT_QUEUE_KEY, timeout=5)
        if not result:
            continue
        _, payment_json = result
        payment_data = json.loads(payment_json)
        payment = PaymentRequest(**payment_data)

        processor_to_use = best_processor_to_use
        if processor_to_use == "none":
            # Aguarda 1 segundo para ver se o health checker define um processador válido
            # await asyncio.sleep(1)
            processor_to_use = random.choice(["default", "default", "fallback"])

        success = await asyncio.to_thread(send_payment_to_processor, PROCESSOR_URLS[processor_to_use], payment)
        if success:
            await store_successful_payment(redis_client, processor_to_use, payment)
        else:
            # Reenfileira em caso de falha
            await redis_client.rpush(PAYMENT_QUEUE_KEY, payment_json)

async def create_payment_request(payment_data: dict) -> PaymentRequest:
    """Cria um objeto PaymentRequest a partir dos dados recebidos."""
    return PaymentRequest(
        correlationId=payment_data["correlationId"],
        amount=float(payment_data["amount"]),
        requestedAt=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    )
async def enqueue_payment(payment: PaymentRequest):
    redis_client = await get_redis_client()
    await redis_client.rpush(PAYMENT_QUEUE_KEY, json.dumps(payment.__dict__))

