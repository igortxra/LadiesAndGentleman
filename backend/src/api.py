import asyncio
import logging
from typing import Optional
from fastapi import HTTPException
from datetime import datetime
import redis.asyncio as aioredis
import os

from .workers import create_payment_request, enqueue_payment

logger = logging.getLogger(__name__)

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379")
PAYMENTS_SET_KEY_PREFIX = "payments:processed"
FIXED_PAYMENT_AMOUNT = 19.9

async def get_redis_client():
    return aioredis.from_url(REDIS_URL, decode_responses=True)

async def create_payment(payment: dict):
    """Recebe um pagamento, enfileira e retorna imediatamente."""
    try:
        payment_obj = await create_payment_request(payment)
        await enqueue_payment(payment_obj)
        return {"message": "Payment received and queued for processing"}
    except Exception as e:
        logger.error(f"Erro ao processar pagamento: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


async def get_payments_summary(from_time: Optional[str] = None, to_time: Optional[str] = None):
    """Endpoint para retornar resumo dos pagamentos filtrados por data a partir do Redis."""
    await asyncio.sleep(0.8)
    redis_client = await get_redis_client()
    try:
        summary = {}
        for processor in ["default", "fallback"]:
            key = f"{PAYMENTS_SET_KEY_PREFIX}:{processor}"
            
            min_score = "-inf"
            if from_time:
                dt_object = datetime.fromisoformat(from_time.replace('Z', '+00:00'))
                min_score = str(int(dt_object.timestamp() * 1000))

            max_score = "+inf"
            if to_time:
                dt_object = datetime.fromisoformat(to_time.replace('Z', '+00:00'))
                max_score = str(int(dt_object.timestamp() * 1000))

            total_requests = await redis_client.zcount(key, min_score, max_score)
            total_amount = total_requests * FIXED_PAYMENT_AMOUNT
            
            summary[processor] = {
                "totalRequests": total_requests,
                "totalAmount": round(total_amount, 2)
            }
            
        return summary
    except Exception as e:
        logger.error(f"Erro ao obter resumo de pagamentos do Redis: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")
    finally:
        await redis_client.close()
