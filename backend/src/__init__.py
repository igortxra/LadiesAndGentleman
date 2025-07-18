import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI, Query

from .workers import payment_worker, update_best_processor
from .api import create_payment, get_payments_summary



NUM_WORKERS = 100

@asynccontextmanager
async def lifespan(app: FastAPI):
    tasks = [asyncio.create_task(payment_worker()) for _ in range(NUM_WORKERS)]
    tasks.append(asyncio.create_task(update_best_processor()))
    yield
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)

app = FastAPI(
    title="Rinha Backend API", 
    version="1.0.0",
    lifespan=lifespan
)


@app.post("/payments", status_code=200)
async def payments_endpoint(payment: dict):
    return await create_payment(payment)


@app.get("/payments-summary")
async def payments_summary_endpoint(from_: str = Query(None, alias="from"), to: str = None):
    return await get_payments_summary(from_, to)


@app.get("/health")
async def health_endpoint():
    return {"status": "healthy"}
