from typing import Dict

class PaymentRequest:
    def __init__(self, correlationId: str, amount: float, requestedAt: str):
        self.correlationId = correlationId
        self.amount = amount
        self.requestedAt = requestedAt


class PaymentResponse:
    def __init__(self, message: str):
        self.message = message


class PaymentSummary:
    def __init__(self, default: Dict[str, float], fallback: Dict[str, float]):
        self.default = default
        self.fallback = fallback


class HealthStatus:
    def __init__(self):
        self.status = {
            "default": {"failing": False, "minResponseTime": 0},
            "fallback": {"failing": False, "minResponseTime": 0}
        }

    def update(self, processor: str, failing: bool, min_response_time: float):
        self.status[processor]["failing"] = failing
        self.status[processor]["minResponseTime"] = min_response_time

    def get(self, processor: str):
        return self.status[processor]