import io
import json
import argparse
import trafilatura
from warcio.archiveiterator import WARCIterator
from prometheus_client import start_http_server, Counter
from transformers import AutoTokenizer

from commoncrawl import BASE_URL, CCDownloader, Downloader
from rabbitmq import QUEUE_NAME, rabbitmq_channel
from ObjectStore import ObjectStore


class Worker:
    MIN_DOC_LENGTH = 500
    MAX_DOC_LENGTH = 1_000_000

    def __init__(self, store: ObjectStore, downloader: Downloader, port: int = 9100):
        self.store = store
        self.downloader = downloader
        self.port = port
        self.tokenizer = AutoTokenizer.from_pretrained("gpt2")

        # Prometheus counters
        self.batch_counter = Counter("worker_batches", "Number of consumed batches")
        self.docs_received = Counter("worker_docs_received", "Number of documents received by worker")
        self.docs_processed = Counter("worker_docs_processed", "Number of documents successfully processed")
        self.bytes_downloaded = Counter("worker_bytes_downloaded_total", "Total number of bytes downloaded")
        self.docs_filtered = Counter("worker_docs_filtered", "Number of documents filtered (too short/long)")

    def _is_valid_doc(self, text: str) -> bool:
        if text is None:
            return False
        return self.MIN_DOC_LENGTH <= len(text) <= self.MAX_DOC_LENGTH

    def process_batch(self, ch, method, _properties, body):
        batch = json.loads(body)
        print(f"[Worker] Received batch of size {len(batch)}")

        self.docs_received.inc(len(batch))

        for item in batch:
            try:
                data = self.downloader.download_and_unzip(
                    item["metadata"]["filename"],
                    int(item["metadata"]["offset"]),
                    int(item["metadata"]["length"]),
                )
                self.bytes_downloaded.inc(len(data))

                for record in WARCIterator(io.BytesIO(data)):
                    if record.rec_type == "response":
                        _text = trafilatura.extract(record.content_stream().read())
                        if not self._is_valid_doc(_text):
                            self.docs_filtered.inc()
                            continue

                        tokens = self.tokenizer.encode(_text)

                        doc = {
                            "url": item["surt_url"],
                            "timestamp": item["timestamp"],
                            "content": _text,
                            "tokens": tokens,
                            "metadata": item["metadata"],
                        }
                        self.store.save(doc)
                        self.docs_processed.inc()
            except Exception as e:
                print(f"[Worker] Error processing doc: {e}")

        self.batch_counter.inc()
        print(
            f"[Worker] Batches consumed: {self.batch_counter._value.get()} | "
            f"Docs received: {self.docs_received._value.get()} | "
            f"Docs processed: {self.docs_processed._value.get()}",
            f"Bytes downloaded: {self.bytes_downloaded._value.get()}",
            flush=True,
        )
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def run(self):
        start_http_server(self.port)
        channel = rabbitmq_channel()
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue=QUEUE_NAME, on_message_callback=self.process_batch)
        print(f"[Worker] Starting consuming")
        channel.start_consuming()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Worker to consume batches and save to object store")
    parser.add_argument("--store-url", type=str, default=None, help="Object store endpoint URL (e.g. http://localhost:9000)")
    parser.add_argument("--store-bucket", type=str, default=None, help="Object store bucket name (default: worker)")
    parser.add_argument("--store-user", type=str, default=None, help="Object store access key (default: minioadmin)")
    parser.add_argument("--store-pass", type=str, default=None, help="Object store secret key (default: minioadmin)")
    parser.add_argument("--port", type=int, default=9100, help="Prometheus metrics port (default: 9100)")
    return parser.parse_args()


def main():
    args = parse_args()

    store = ObjectStore(
        url=args.store_url,
        bucket=args.store_bucket,
        user=args.store_user,
        password=args.store_pass,
    )
    downloader = CCDownloader(BASE_URL)
    worker = Worker(store, downloader, port=args.port)
    worker.run()


if __name__ == "__main__":
    main()
