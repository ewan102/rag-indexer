"""Prometheus metric definitions for the RAG indexer."""

from prometheus_client import Counter, Histogram

MESSAGES_TOTAL = Counter(
    "rag_indexer_messages_total",
    "Total messages processed by the RAG indexer",
    ["action", "status", "partition"],
)

PROCESSING_DURATION = Histogram(
    "rag_indexer_processing_duration_seconds",
    "Time spent processing a single message",
    ["action"],
    buckets=[0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0, float("inf")],
)
