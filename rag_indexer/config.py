import os

RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
EXCHANGE_NAME = os.getenv("EXCHANGE_NAME", "rag.index.topic")
ROUTING_KEY = os.getenv("ROUTING_KEY", "rag.index.*")
QUEUE_NAME = os.getenv("QUEUE_NAME", "rag.index.q")

RETRY_QUEUES = [
    ("rag.index.retry.30s.q", 30_000),
    ("rag.index.retry.5m.q", 300_000),
    ("rag.index.retry.1h.q", 3_600_000),
]
MAX_RETRIES = int(os.getenv("MAX_RETRIES", str(len(RETRY_QUEUES))))
DLQ_NAME = os.getenv("DLQ_NAME", "rag.index.dlq")

HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "60"))
