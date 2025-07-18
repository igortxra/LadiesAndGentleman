import asyncio
import os
from typing import TypedDict

import httpx
import redis.asyncio as redis


class ProcessorHealth(TypedDict):
    name: str
    min_response_time: int
    failing: bool


class HealthChecker:
    def __init__(self):
        self.redis = redis.from_url(os.getenv("REDIS_URL", "redis://redis:6379"))
        self.default_url = os.getenv("PROCESSOR_DEFAULT_URL")
        self.fallback_url = os.getenv("PROCESSOR_FALLBACK_URL")
        self.check_interval = int(os.getenv("CHECK_INTERVAL", 5))
        self.min_response_time_limit_ms = int(os.getenv("MIN_RESPONSE_TIME_LIMIT_MS", 100))
        self.http_client = httpx.AsyncClient()

    async def check_health_of_processors(self):
        default_health = await self.check_processor("default", self.default_url)
        fallback_health = await self.check_processor("fallback", self.fallback_url)
        best_processor = self.determine_best_processor_based_on_health(
            default_health, fallback_health
        )
        await self.store_results(best_processor)

        print(f"✅ Best processor: {best_processor or 'none'}")  # REMOVE THIS LATER

    async def check_processor(self, name: str, url: str) -> ProcessorHealth:
        try:
            response = await self.http_client.get(f"{url}/payments/service-health")
            response.raise_for_status()
            health_response = response.json() or {}
            min_response_time = health_response.get("minResponseTime", 999999)
            failing = health_response.get("failing", True)
            return ProcessorHealth(
                name=name,
                min_response_time=min_response_time,
                failing=failing,
            )
        except Exception as e:
            print(f"❌ Health check for {name} failed: {e}")
            return ProcessorHealth(name=name, min_response_time=999999, failing=True)

    def determine_best_processor_based_on_health(
        self, default_health: ProcessorHealth, fallback_health: ProcessorHealth
    ) -> str | None:
        best = None

        # Se os dois estão funcionando
        if not default_health["failing"] and not fallback_health["failing"]:
            # Pega o mais rápido
            if (
                default_health["min_response_time"]
                <= fallback_health["min_response_time"]
            ):
                best = "default"
            else:
                best = "fallback"

        # Se um funciona e o outro não, pega o que está funcionando
        elif not default_health["failing"]:
            best = "default"
        elif not fallback_health["failing"]:
            best = "fallback"

        # Caso o escolhido seja muito lento, não há melhor escolha
        health_of_best = None
        if best == "default":
            health_of_best = default_health
        elif best == "fallback":
            health_of_best = fallback_health

        if (
            health_of_best
            and health_of_best["min_response_time"] > self.min_response_time_limit_ms
        ):
            best = None

        return best

    async def store_results(self, best_processor: str | None):
        # O valor 'none' será armazenado se best_processor for None
        await self.redis.set("processor:best", best_processor or "none", ex=30)


async def main():
    hc = HealthChecker()
    print(f"🚀 HealthChecker started - checking every {hc.check_interval}s")
    print(f"📊 'default' is choosen before first check")
    await hc.store_results("default")
    while True:
        try:
            await hc.check_health_of_processors()
        except Exception as e:
            print(f"❌ An error occurred during health check: {e}")
        await asyncio.sleep(hc.check_interval)

if __name__ == "__main__":
    asyncio.run(main())
