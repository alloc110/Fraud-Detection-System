"""
PyFlink DataStream job (ASYNC I/O version): consume transactions from Kafka
and call the FastAPI /predict fraud-detection endpoint WITHOUT blocking the
task thread for each request. This lets a single subtask have many
in-flight HTTP requests at once, which is what you want under high traffic.

Requirements:
    pip install apache-flink aiohttp
"""

import asyncio
import json
import logging

import aiohttp
from pyflink.common import Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    FlinkKafkaConsumer,
    FlinkKafkaProducer,
)
from pyflink.datastream.functions import RuntimeContext
from pyflink.datastream.async_wait import AsyncWaitOperator  # see note below
from pyflink.datastream.functions import AsyncFunction

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
INPUT_TOPIC = "transactions"
OUTPUT_TOPIC = "transactions.scored"
CONSUMER_GROUP = "flink-fraud-detector"

FRAUD_API_URL = "http://fraud-api:8000/predict"
API_TIMEOUT_SECONDS = 2.0

MAX_IN_FLIGHT_REQUESTS = 100     # concurrent requests per subtask
ASYNC_TIMEOUT_MS = 5000          # per-request timeout enforced by Flink itself


# ---------------------------------------------------------------------------
# Async map function
# ---------------------------------------------------------------------------
class AsyncFraudScoringFunction(AsyncFunction):
    """
    Non-blocking version: opens one aiohttp.ClientSession per subtask and
    reuses it for every request, letting many requests be in flight at once
    instead of one-at-a-time.
    """

    def open(self, runtime_context: RuntimeContext):
        self.logger = logging.getLogger("AsyncFraudScoringFunction")
        # one shared session + event loop per subtask instance
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self.session = aiohttp.ClientSession(
            loop=self.loop,
            timeout=aiohttp.ClientTimeout(total=API_TIMEOUT_SECONDS),
        )

    def close(self):
        self.loop.run_until_complete(self.session.close())
        self.loop.close()

    async def _score(self, tx: dict) -> dict:
        payload = {
            "step": tx.get("step"),
            "transaction_id": tx.get("transaction_id"),
            "source_user_id": tx.get("user"),
            "dest_user_id": tx.get("user_dest"),
            "amount": tx.get("amount"),
            "payment_method": tx.get("method"),
            "transaction_time": tx.get("tx_time"),
        }
        try:
            async with self.session.post(FRAUD_API_URL, json=payload) as resp:
                resp.raise_for_status()
                return await resp.json()
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            self.logger.error("Fraud API call failed for %s: %s",
                               payload.get("transaction_id"), e)
            return {
                "transaction_id": payload.get("transaction_id"),
                "error": "api_call_failed",
                "detail": str(e),
            }

    async def async_invoke(self, value: str, result_future):
        """
        Called by Flink's async I/O operator. Must resolve `result_future`
        with a list containing the output record(s) — never block here.
        """
        try:
            tx = json.loads(value)
        except json.JSONDecodeError:
            result_future.complete([json.dumps({"error": "invalid_json", "raw": value})])
            return

        result = await self._score(tx)
        enriched = {**tx, **result}
        result_future.complete([json.dumps(enriched)])


# ---------------------------------------------------------------------------
# Job definition
# ---------------------------------------------------------------------------
def build_job():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(4)

    kafka_consumer = FlinkKafkaConsumer(
        topics=INPUT_TOPIC,
        deserialization_schema=SimpleStringSchema(),
        properties={
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": CONSUMER_GROUP,
        },
    )

    kafka_producer = FlinkKafkaProducer(
        topic=OUTPUT_TOPIC,
        serialization_schema=SimpleStringSchema(),
        producer_config={"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS},
    )

    stream = env.add_source(kafka_consumer)

    # NOTE: PyFlink's Python Table/DataStream API historically has had
    # limited/unstable support for AsyncFunction compared to the Java API.
    # If `AsyncWaitOperator` isn't available in your PyFlink version, the
    # more reliable path is:
    #   1. Write this scoring function in Java/Scala using
    #      org.apache.flink.streaming.api.datastream.AsyncDataStream, or
    #   2. Stay on the synchronous MapFunction version but scale out
    #      parallelism + run a small pool of API replicas behind a
    #      load balancer, or
    #   3. Batch records in a window and call a `/predict_batch` endpoint,
    #      cutting the number of HTTP round-trips drastically.
    # Check `pyflink.datastream.async_wait` availability for your version
    # before relying on this file as-is.
    from pyflink.datastream.async_wait import AsyncDataStream

    scored = AsyncDataStream.unordered_wait(
        stream,
        AsyncFraudScoringFunction(),
        ASYNC_TIMEOUT_MS,
        capacity=MAX_IN_FLIGHT_REQUESTS,
    )

    scored.add_sink(kafka_producer)
    env.execute("fraud-detection-scoring-job-async")


if __name__ == "__main__":
    build_job()