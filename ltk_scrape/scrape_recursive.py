import argparse
from threading import Thread
from queue import Queue
import time

from .client import LTKClient
from .db import DB


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start_url", type=str, default=None)
    parser.add_argument("--db_path", type=str, default="db.db")
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--proxy", type=str, default=None)
    args = parser.parse_args()

    db = DB(args.db_path)
    req_queue = Queue(maxsize=50)
    fetchers = [Fetcher(req_queue, proxy=args.proxy) for _ in range(args.workers)]

    try:
        if args.start_url is not None:
            first_resp = Queue()
            req_queue.put((None, args.start_url, first_resp))
            _, results, exc = first_resp.get()
            if exc is not None:
                raise exc
            db.upsert_ltks(list(results.ltks.values()))
            db.upsert_products(list(results.products.values()))

        while True:
            t1 = time.time()
            unvisited = db.unvisited_ltks(limit=50)
            t2 = time.time()
            print(f"took {t2 - t1} seconds to find unvisited LTKs")
            if not len(unvisited):
                print("no more remaining posts")
                break
            resp_queue = Queue()
            for id, url in unvisited:
                req_queue.put((id, url, resp_queue))
            for _ in range(len(unvisited)):
                id, results, exc = resp_queue.get()
                if exc is not None:
                    if isinstance(
                        exc, KeyboardInterrupt
                    ) or "net::ERR_PROXY_CONNECTION_FAILED" in str(exc):
                        raise exc
                    else:
                        print(f"failed id: {id} (error: {exc})")
                        db.mark_visited_ltk(id, error=str(exc))
                else:
                    print(f"fetched id: {id}")
                    db.upsert_ltks(list(results.ltks.values()))
                    db.upsert_products(list(results.products.values()))
                    db.mark_visited_ltk(id, error=None)
    finally:
        for _ in fetchers:
            req_queue.put(None)
        for f in fetchers:
            f.thread.join()


class Fetcher:
    def __init__(self, queue: Queue, *args, **kwargs):
        self.queue = queue
        self.client_args = args
        self.client_kwargs = kwargs
        self.thread = Thread(target=self._worker, name="fetcher-thread")
        self.thread.start()

    def _worker(self):
        client = LTKClient(*self.client_args, **self.client_kwargs)
        while True:
            req = self.queue.get()
            if req is None:
                return
            id, url, resp_queue = req
            try:
                results = client.fetch_post(url)
                resp_queue.put((id, results, None))
            except Exception as exc:
                resp_queue.put((id, None, exc))


if __name__ == "__main__":
    main()
