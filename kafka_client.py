import json
import logging
import os
import signal
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

@dataclass
class KafkaConfig:
    bootstrap_servers: str = field(
        default_factory=lambda: os.environ.get(
            "BOOTSTRAP_SERVERS", "broker1:9092,broker2:9092,broker3:9092"
        )
    )
    security_protocol: str = "PLAINTEXT"
    # Uncomment for TLS / SASL in production:
    # ssl_ca_location: str = "/certs/ca.pem"
    # sasl_mechanism: str = "SCRAM-SHA-256"
    # sasl_username: str = ""
    # sasl_password: str = ""


@dataclass
class ProducerConfig(KafkaConfig):
    acks: str = "all"               # wait for all in-sync replicas
    retries: int = 5
    retry_backoff_ms: int = 300
    enable_idempotence: bool = True  # exactly-once delivery
    compression_type: str = "lz4"
    linger_ms: int = 10             # batch window
    batch_size: int = 65536         # 64 KB
    max_in_flight_requests: int = 5


@dataclass
class ConsumerConfig(KafkaConfig):
    group_id: str = "default-group"
    auto_offset_reset: str = "earliest"
    enable_auto_commit: bool = False  # manual commits only
    max_poll_interval_ms: int = 300_000
    session_timeout_ms: int = 45_000
    heartbeat_interval_ms: int = 15_000


# ---------------------------------------------------------------------------
# Producer
# ---------------------------------------------------------------------------

class KafkaProducer:
    def __init__(self, config: ProducerConfig) -> None:
        self._config = config
        self._producer = Producer(self._build_conf())
        logger.info("Producer ready | bootstrap=%s", config.bootstrap_servers)

    def _build_conf(self) -> Dict[str, Any]:
        return {
            "bootstrap.servers": self._config.bootstrap_servers,
            "security.protocol": self._config.security_protocol,
            "acks": self._config.acks,
            "retries": self._config.retries,
            "retry.backoff.ms": self._config.retry_backoff_ms,
            "enable.idempotence": self._config.enable_idempotence,
            "compression.type": self._config.compression_type,
            "linger.ms": self._config.linger_ms,
            "batch.size": self._config.batch_size,
            "max.in.flight.requests.per.connection": self._config.max_in_flight_requests,
        }

    def _on_delivery(self, err: Optional[KafkaError], msg: Any) -> None:
        if err:
            logger.error(
                "Delivery failed | topic=%s partition=%d error=%s",
                msg.topic(), msg.partition(), err,
            )
        else:
            logger.debug(
                "Delivered | topic=%s partition=%d offset=%d key=%s",
                msg.topic(), msg.partition(), msg.offset(),
                msg.key().decode() if msg.key() else None,
            )

    def produce(
        self,
        topic: str,
        value: Any,
        key: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> None:
        try:
            self._producer.produce(
                topic=topic,
                key=key.encode() if key else None,
                value=json.dumps(value).encode(),
                headers=list(headers.items()) if headers else None,
                on_delivery=self._on_delivery,
            )
            self._producer.poll(0)
        except BufferError:
            logger.warning("Producer queue full — flushing before retry")
            self._producer.flush(timeout=10)
            self.produce(topic, value, key, headers)
        except KafkaException:
            logger.exception("Failed to produce to topic=%s", topic)
            raise

    def flush(self, timeout: float = 30.0) -> None:
        remaining = self._producer.flush(timeout=timeout)
        if remaining:
            logger.warning("%d messages undelivered after flush timeout", remaining)

    def close(self) -> None:
        self.flush()
        logger.info("Producer closed")

    # Context-manager support
    def __enter__(self) -> "KafkaProducer":
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()


# ---------------------------------------------------------------------------
# Consumer
# ---------------------------------------------------------------------------

class KafkaConsumer:
    def __init__(self, config: ConsumerConfig, topics: List[str]) -> None:
        self._config = config
        self._topics = topics
        self._consumer = Consumer(self._build_conf())
        self._running = False
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

    def _build_conf(self) -> Dict[str, Any]:
        return {
            "bootstrap.servers": self._config.bootstrap_servers,
            "security.protocol": self._config.security_protocol,
            "group.id": self._config.group_id,
            "auto.offset.reset": self._config.auto_offset_reset,
            "enable.auto.commit": self._config.enable_auto_commit,
            "max.poll.interval.ms": self._config.max_poll_interval_ms,
            "session.timeout.ms": self._config.session_timeout_ms,
            "heartbeat.interval.ms": self._config.heartbeat_interval_ms,
        }

    def _handle_signal(self, signum: int, _frame: Any) -> None:
        logger.info("Signal %s received — shutting down", signum)
        self._running = False

    def _on_assign(self, _consumer: Any, partitions: list) -> None:
        logger.info("Partitions assigned: %s", [str(p) for p in partitions])

    def _on_revoke(self, consumer: Any, partitions: list) -> None:
        logger.info("Partitions revoked — committing offsets")
        try:
            consumer.commit(asynchronous=False)
        except KafkaException as exc:
            logger.warning("Commit on revoke failed: %s", exc)

    def consume(
        self,
        handler: Callable[[str, Any, Optional[str]], None],
        poll_timeout: float = 1.0,
        batch_size: int = 100,
    ) -> None:
        self._consumer.subscribe(
            self._topics,
            on_assign=self._on_assign,
            on_revoke=self._on_revoke,
        )
        self._running = True
        logger.info(
            "Consumer started | group=%s topics=%s",
            self._config.group_id, self._topics,
        )
        try:
            while self._running:
                messages = self._consumer.consume(
                    num_messages=batch_size, timeout=poll_timeout
                )
                if not messages:
                    continue

                for msg in messages:
                    if msg.error():
                        self._handle_error(msg.error())
                        continue
                    self._dispatch(msg, handler)

                self._consumer.commit(asynchronous=False)

        except KafkaException:
            logger.exception("Fatal consumer error")
            raise
        finally:
            self.close()

    def _dispatch(
        self,
        msg: Any,
        handler: Callable[[str, Any, Optional[str]], None],
    ) -> None:
        topic = msg.topic()
        key = msg.key().decode() if msg.key() else None
        try:
            value = json.loads(msg.value().decode())
        except json.JSONDecodeError as exc:
            logger.error("Deserialize failed | topic=%s key=%s error=%s", topic, key, exc)
            return
        try:
            handler(topic, value, key)
        except Exception:
            logger.exception("Handler raised | topic=%s key=%s", topic, key)

    def _handle_error(self, error: KafkaError) -> None:
        if error.code() == KafkaError._PARTITION_EOF:
            logger.debug("End of partition reached")
        elif error.code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
            logger.error("Unknown topic or partition: %s", error)
        elif error.fatal():
            raise KafkaException(error)
        else:
            logger.warning("Non-fatal consumer error: %s", error)

    def close(self) -> None:
        self._consumer.close()
        logger.info("Consumer closed")

    def __enter__(self) -> "KafkaConsumer":
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()


# ---------------------------------------------------------------------------
# Example usage
# ---------------------------------------------------------------------------

def order_handler(topic: str, value: Any, key: Optional[str]) -> None:
    logger.info("Message | topic=%s key=%s value=%s", topic, key, value)


if __name__ == "__main__":
    BOOTSTRAP = "broker1:9092,broker2:9092,broker3:9092"
    TOPIC = "orders"

    with KafkaProducer(ProducerConfig(bootstrap_servers=BOOTSTRAP)) as producer:
        producer.produce(TOPIC, value={"order_id": 1, "amount": 99.99}, key="user-1")
        producer.produce(TOPIC, value={"order_id": 2, "amount": 49.00}, key="user-2")

    with KafkaConsumer(
        ConsumerConfig(bootstrap_servers=BOOTSTRAP, group_id="orders-group"),
        topics=[TOPIC],
    ) as consumer:
        consumer.consume(handler=order_handler)
