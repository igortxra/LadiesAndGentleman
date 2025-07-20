from fastapi import FastAPI, Query


app = FastAPI(title="Rinha Backend API", version="1.0.0")


@app.post("/payments", status_code=200)
async def payments_endpoint(payment: dict):
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
