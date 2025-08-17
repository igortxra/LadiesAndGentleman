from datetime import datetime
from fastapi import FastAPI, Query
from redis.asyncio import Redis
import os


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
    def to_timestamp(dt_str):
        if not dt_str:
            return None
        return datetime.fromisoformat(dt_str.replace("Z", "+00:00")).timestamp()

    start_ts = to_timestamp(from_datetime) or 0
    end_ts = to_timestamp(to_datetime) or "+inf"

    default_count = await r.zcount(f"{stream_name}:default", start_ts, end_ts)
    fallback_count = await r.zcount(f"{stream_name}:fallback", start_ts, end_ts)

    return {
        "default": {
            "totalRequests": default_count,
            "totalAmount": round(default_count * 19.9, 2),
        },
        "fallback": {
            "totalRequests": fallback_count,
            "totalAmount": round(fallback_count * 19.9, 2),
        },
    }


@app.get("/health", status_code=200)
async def health():
    return
