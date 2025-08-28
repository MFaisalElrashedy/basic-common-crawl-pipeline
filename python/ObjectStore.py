import os
import json
import datetime
import boto3
import argparse
import time


class ObjectStore:
    def __init__(self, url: str = None, bucket: str = None, user: str = None, password: str = None):
        endpoint_url = url or os.getenv("OBJECT_STORE_URL", "http://localhost:9000")
        bucket_name = bucket or os.getenv("OBJECT_STORE_BUCKET", "worker")
        user = user or os.getenv("OBJECT_STORE_USER", "minioadmin")
        password = password or os.getenv("OBJECT_STORE_PASS", "minioadmin")

        self.s3 = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=user,
            aws_secret_access_key=password,
        )
        self.bucket_name = bucket_name
        self._ensure_bucket()

    def _ensure_bucket(self):
        """Create the bucket if it doesn't exist."""
        try:
            self.s3.head_bucket(Bucket=self.bucket_name)
        except self.s3.exceptions.ClientError:
            self.s3.create_bucket(Bucket=self.bucket_name)
            print(f"[ObjectStore] Created bucket '{self.bucket_name}'")

    def save(self, doc: dict) -> None:
        """Save a document as JSONL into a unique daily file (timestamped)."""
        now = datetime.datetime.utcnow()
        today = now.date().isoformat()

        ticks = int(time.time() * 1000)
        key = f"{today}/{ticks}.jsonl"

        line = json.dumps(doc, ensure_ascii=False) + "\n"

        self.s3.put_object(
            Bucket=self.bucket_name,
            Key=key,
            Body=line.encode("utf-8"),
            ContentType="application/json",
        )
        print(f"[ObjectStore] Saved doc to {self.bucket_name}/{key}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Test ObjectStore connection")
    parser.add_argument("--url", type=str, default=None, help="Object store endpoint URL")
    parser.add_argument("--bucket", type=str, default=None, help="Bucket name")
    parser.add_argument("--user", type=str, default=None, help="Access key")
    parser.add_argument("--password", type=str, default=None, help="Secret key")
    return parser.parse_args()


def main():
    args = parse_args()
    store = ObjectStore(args.url, args.bucket, args.user, args.password)

    test_doc = {
        "url": "http://example.com",
        "timestamp": datetime.datetime.utcnow().isoformat(),
        "content": "Hello from ObjectStore test!",
    }

    store.save(test_doc)
    print("[CLI] Upload test complete")


if __name__ == "__main__":
    main()
