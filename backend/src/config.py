import os

# URLs dos processadores de pagamento
PROCESSOR_URLS = {
    "default": os.getenv("PAYMENT_PROCESSOR_URL_DEFAULT", "http://payment-processor-default:8080"),
    "fallback": os.getenv("PAYMENT_PROCESSOR_URL_FALLBACK", "http://payment-processor-fallback:8080"),
    "none": os.getenv("PAYMENT_PROCESSOR_URL_DEFAULT", "http://payment-processor-default:8080")
}
