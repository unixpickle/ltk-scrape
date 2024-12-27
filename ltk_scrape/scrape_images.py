import argparse
import io
import time

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
    parser.add_argument("--proxy", type=str, default=None)
    args = parser.parse_args()

    db = DB(args.db_path)

    proxies = None if args.proxy is None else {"http": args.proxy, "https": args.proxy}

    with requests.Session() as sess:
        while True:
            t1 = time.time()
            unvisited = db.missing_images(
                args.image_type, 1000, only_with_price=args.only_with_price
            )
            t2 = time.time()
            print("took", t2 - t1, "seconds to find unvisited images")
            if not len(unvisited):
                print("no more remaining images to download")
                break
            for id, url in unvisited:
                print(f"fetching: {id} ...")
                try:
                    result_image = sess.get(
                        url, stream=True, timeout=5, proxies=proxies
                    ).content
                    # Make sure the image is actually valid.
                    Image.open(io.BytesIO(result_image)).load()
                except KeyboardInterrupt:
                    raise
                except requests.exceptions.ReadTimeout as exc:
                    db.insert_image(args.image_type, id, blob=None, error=str(exc))
                    continue
                except Exception as exc:
                    if "SOCKSHTTP" in str(exc):
                        time.sleep(1.0)
                    db.insert_image(args.image_type, id, blob=None, error=str(exc))
                    continue
                db.insert_image(args.image_type, id, blob=result_image)


if __name__ == "__main__":
    main()
