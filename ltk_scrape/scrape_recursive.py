import argparse

from .client import LTKClient
from .db import DB


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start_url", type=str, default=None)
    parser.add_argument("--db_path", type=str, default="db.db")
    parser.add_argument("--proxy", type=str, default=None)
    args = parser.parse_args()

    db = DB(args.db_path)
    client = LTKClient(proxy=args.proxy)

    if args.start_url is not None:
        results = client.fetch_post(args.start_url)
        db.upsert_ltks(list(results.ltks.values()))
        db.upsert_products(list(results.products.values()))

    while True:
        unvisited = db.unvisited_ltks(limit=50)
        if not len(unvisited):
            print("no more remaining posts")
            break
        for id, url in unvisited:
            print(f"fetching: {id} ...")
            try:
                results = client.fetch_post(url)
            except KeyboardInterrupt:
                raise
            except Exception as exc:
                if "net::ERR_PROXY_CONNECTION_FAILED" in str(exc):
                    raise
                db.mark_visited_ltk(id, error=str(exc))
                continue
            db.upsert_ltks(list(results.ltks.values()))
            db.upsert_products(list(results.products.values()))
            db.mark_visited_ltk(id, error=None)


if __name__ == "__main__":
    main()
