from logging import Logger
from fastapi import FastAPI, Query, HTTPException
from datetime import datetime, timezone
import asyncio
import httpx
from contextlib import asynccontextmanager


# ADICIONAR UM BANCO DE DADOS PARA ENVIAR OS PAGAMETNOS
# COLD START DELAY PARA DAR TEMPO DE BAIXAR CPU DOS BACKENDS


# VARIAVEIS DE AMBIENTE NO FUTURO
BATCH_SIZE = 200
TARGET_URL = "http://payment-processor-default:8080"


payment_queue = asyncio.Queue()

# === Envia 1 pagamento e, se falhar, reenvia ===
async def send_payment(client: httpx.AsyncClient , payment):
    response = await client.post(f"{TARGET_URL}/payments", json=payment)
    if response.status_code != 200:
        await payment_queue.put(payment)
        await asyncio.sleep(1)

    if response.elapsed.total_seconds() >= 1:
        await asyncio.sleep(1)
    

# === Worker que processa lotes de 100 pagamentos ===
async def payment_worker():
    # async with httpx.AsyncClient() as client:
    while True:
        try:
            payment = await payment_queue.get_nowait()
            payment_queue.task_done()
        except Exception:
            continue
            

            # batch = []
            # while len(batch) < BATCH_SIZE:
            #     item = await payment_queue.get()
            #     batch.append(item)
            # 
            # # Envia as 100 requisições em paralelo com tratamento de erro
            # await asyncio.gather(*[
            #     send_payment(client, payment) for payment in batch
            # ])

# === FastAPI lifespan: inicia o worker no startup ===
@asynccontextmanager
async def lifespan(app: FastAPI):
    asyncio.create_task(payment_worker())
    yield


app = FastAPI(title="Rinha Backend API", version="1.0.0")
# app.router.lifespan_context = lifespan


# ROTAS
@app.post("/payments", status_code=200)
async def payments_endpoint(payment: dict):
    payment_queue.put_nowait(payment)


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

