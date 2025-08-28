from abc import ABC, abstractmethod
import os
import pika
import time
from pika import exceptions as pika_exceptions


QUEUE_NAME = "batches"
MAX_RETRIES = 3       # constant retry count
RETRY_DELAY = 5       # seconds between retries


class MessageQueueChannel(ABC):
    @abstractmethod
    def basic_publish(self, exchange: str, routing_key: str, body: str) -> None:
        pass


class RabbitMQChannel(MessageQueueChannel):
    def __init__(self) -> None:
        self._connect()

    def _connect(self):
        self.channel = rabbitmq_channel()

    def basic_publish(self, exchange: str, routing_key: str, body: str) -> None:
        attempt = 0
        while attempt < MAX_RETRIES:
            try:
                self.channel.basic_publish(
                    exchange=exchange,
                    routing_key=routing_key,
                    body=body,
                )
                return

            except (
                pika_exceptions.AuthenticationError,
                pika_exceptions.ProbableAuthenticationError,
                pika_exceptions.ProbableAccessDeniedError,
            ) as e:
                print(f"[RabbitMQChannel] Unrecoverable error: {e}")
                raise

            except (
                    pika_exceptions.AMQPConnectionError,
                    pika_exceptions.ConnectionClosed,
                    pika_exceptions.ConnectionClosedByBroker,
                    pika_exceptions.ConnectionClosedByClient,
                    pika_exceptions.ConnectionWrongStateError,
                    pika_exceptions.StreamLostError,
                    pika_exceptions.AMQPHeartbeatTimeout,
                    pika_exceptions.ChannelClosed,
                    pika_exceptions.ChannelClosedByBroker,
                    pika_exceptions.ChannelClosedByClient,
                    pika_exceptions.ChannelWrongStateError,
                    pika_exceptions.UnroutableError,
                    pika_exceptions.NackError,
                    pika_exceptions.AMQPError,
            ) as e:
                attempt += 1
                print(f"[RabbitMQChannel] Recoverable error ({e}), retry {attempt}/{MAX_RETRIES}...")
                time.sleep(RETRY_DELAY)
                self._connect()

            except Exception as e:
                attempt += 1
                print(f"[RabbitMQChannel] Unexpected error: {e}, retry {attempt}/{MAX_RETRIES}")
                time.sleep(RETRY_DELAY)
                self._connect()

            raise RuntimeError(f"Failed to publish after {MAX_RETRIES} retries")


def rabbitmq_channel() -> pika.adapters.blocking_connection.BlockingChannel:

    connection = pika.BlockingConnection(
        pika.URLParameters(os.environ["RABBITMQ_CONNECTION_STRING"])
    )
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME)
    return channel
