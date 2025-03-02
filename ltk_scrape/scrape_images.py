import argparse
import io
from multiprocessing import Process, Queue
import sys
import time
import traceback
from typing import Any

import requests
from PIL import Image

from .db import DB


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db_path", type=str, default="db.db")
    parser.add_argument(
        "--image_type", type=str, default="product", help="'product' or 'ltk'"
    )
    parser.add_argument("--only_with_price", action="store_true")
    parser.add_argument("--only_with_name", action="store_true")
    parser.add_argument("--sort_by_recent", action="store_true")
    parser.add_argument("--proxy", type=str, default=None)
    parser.add_argument("--concurrency", type=int, default=4)
    parser.add_argument("--batch_size", type=int, default=10000)
    args = parser.parse_args()

    db = DB(args.db_path)

    proxies = None if args.proxy is None else {"http": args.proxy, "https": args.proxy}
    req_queue = Queue()
    resp_queue = Queue(maxsize=args.concurrency)
    fetchers = [
        Fetcher(proxies=proxies, req_queue=req_queue, resp_queue=resp_queue)
        for _ in range(args.concurrency)
    ]

    try:
        while True:
            t1 = time.time()
            unvisited = db.missing_images(
                args.image_type,
                args.batch_size,
                only_with_price=args.only_with_price,
                only_with_name=args.only_with_name,
                sort_by_recent=args.sort_by_recent,
            )
            t2 = time.time()
            print("took", t2 - t1, "seconds to find unvisited images")
            if not len(unvisited):
                print("no more remaining images to download")
                break
            for id, url in unvisited:
                req_queue.put((id, url))
            for _ in range(len(unvisited)):
                id, data, err = resp_queue.get()
                if data is None:
                    print(f"error for {id}: {err}")
                    db.insert_image(args.image_type, id, blob=None, error=err)
                else:
                    print(f"fetched {id}")
                    db.insert_image(args.image_type, id, blob=data)
    finally:
        for fetcher in fetchers:
            fetcher.kill()


class Fetcher:
    def __init__(
        self, proxies: Any, req_queue: Queue, resp_queue: Queue, *args, **kwargs
    ):
        self.client_args = args
        self.client_kwargs = kwargs
        self.proc = Process(
            target=Fetcher._worker,
            args=(proxies, req_queue, resp_queue),
            name="fetcher-worker",
            daemon=True,
        )
        self.proc.start()

    def kill(self):
        self.proc.kill()
        self.proc.join()

    @staticmethod
    def _worker(proxies, req_queue, resp_queue):
        with requests.Session() as sess:
            while True:
                id, url = req_queue.get()
                try:
                    result_image = sess.get(
                        url, stream=True, timeout=5, proxies=proxies
                    ).content
                    # Make sure the image is actually valid.
                    Image.open(io.BytesIO(result_image)).load()
                except KeyboardInterrupt:
                    traceback.print_exc()
                    sys.exit(1)
                except requests.exceptions.ReadTimeout as exc:
                    resp_queue.put((id, None, str(exc)))
                    continue
                except Exception as exc:
                    if "SOCKSHTTP" in str(exc):
                        time.sleep(1.0)
                    resp_queue.put((id, None, str(exc)))
                    continue
                resp_queue.put((id, result_image, None))


if __name__ == "__main__":
    main()
