import json
import argparse
import requests
from pathlib import Path
from typing import Any, Mapping, Sequence
from prometheus_client import Counter, start_http_server
from tqdm import tqdm

from commoncrawl import (
    BASE_URL,
    crawl_path,
    CCDownloader,
    CSVIndexReader,
    Downloader,
    IndexReader,
    CCIndexDownloader,
)
from rabbitmq import QUEUE_NAME, MessageQueueChannel, RabbitMQChannel


BATCH_SIZE = 50

batch_counter = Counter("batcher_batches_sent", "Number of published batches")
batcher_docs_seen = Counter("batcher_documents_total","Total number of documents seen by the batcher")
batcher_docs_filtered = Counter("batcher_documents_filtered","Number of documents filtered out by the batcher")
batcher_docs_sent = Counter("batcher_documents_sent","Number of documents sent by the batcher")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Batcher")
    parser.add_argument(
        "--crawl-version", type=str, default="CC-MAIN-2024-30", help="Common Crawl version"
    )
    parser.add_argument(
        "--output-folder", type=str, default="output", help="Folder for downloaded index"
    )
    return parser.parse_args()


def publish_batch(
    channel: MessageQueueChannel,
    batch: Sequence[Mapping[str, Any]],
) -> None:
    #print("Pushing batch of size", len(batch))
    channel.basic_publish(
        exchange="",
        routing_key=QUEUE_NAME,
        body=json.dumps(batch),
    )
    batch_counter.inc()


def process_index(index: IndexReader, channel: MessageQueueChannel, downloader: Downloader, batch_size: int) -> None:
    lines = list(index)   # load all lines so we know total count
    found_urls = []

    for cdx_chunk in tqdm(lines, desc="Processing cluster.idx", unit="chunks"):
        data = downloader.download_and_unzip(
            cdx_chunk[1], int(cdx_chunk[2]), int(cdx_chunk[3])
        ).decode("utf-8")

        for line in data.split("\n"):
            if not line:
                continue
            batcher_docs_seen.inc()
            values = line.split(" ")
            metadata = json.loads("".join(values[2:]))
            if "languages" in metadata and "eng" in metadata["languages"] and metadata["status"] == "200":
                batcher_docs_filtered.inc()
                found_urls.append(
                    {"surt_url": values[0], "timestamp": values[1], "metadata": metadata}
                )

            if len(found_urls) >= batch_size:
                publish_batch(channel, found_urls)
                batcher_docs_sent.inc(len(found_urls))
                found_urls = []

    if found_urls:
        publish_batch(channel, found_urls)
        batcher_docs_sent.inc(len(found_urls))

    if len(found_urls) > 0:
        publish_batch(channel, found_urls)
        batcher_docs_sent.inc(len(found_urls))
        print(f"[Batcher] Final batch Sent: {batcher_docs_sent._value.get()}")


def main() -> None:
    args = parse_args()
    start_http_server(9101)

    idx_file = CCIndexDownloader(args.crawl_version, args.output_folder).download()

    channel = RabbitMQChannel()
    downloader = CCDownloader(f"{BASE_URL}/{crawl_path(args.crawl_version)}")
    index_reader = CSVIndexReader(idx_file)

    process_index(index_reader, channel, downloader, BATCH_SIZE)


if __name__ == "__main__":
    main()
