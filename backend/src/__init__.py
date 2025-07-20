from fastapi import FastAPI, Query
from redis.asyncio import Redis
import os
import json


app = FastAPI(title="Rinha Backend API", version="1.0.0")


redis_url = os.getenv("REDIS_URL", "redis://redis:6379")
r = Redis.from_url(redis_url, decode_responses=False)
stream_name = "payments"


@app.post("/payments", status_code=200)
async def payments_endpoint(payment: dict):
    await r.xadd(stream_name, payment)
    return


@app.get("/payments-summary")
async def payments_summary_endpoint(
    from_datetime: str = Query(None, alias="from"), 
    to_datetime: str = Query(None, alias="to"),
):
    return {
        "default" : {
            "totalRequests": 0,
            "totalAmount": round(0 * 19.9, 2),
        },
        "fallback" : {
            "totalRequests": 0,
            "totalAmount": round(0 * 19.9, 2),
        }
    }


@app.get("/health", status_code=200)
async def health():
    return
