import argparse
import io

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
    args = parser.parse_args()

    db = DB(args.db_path)

    with requests.Session() as sess:
        while True:
            unvisited = db.missing_images(
                args.image_type, 50, only_with_price=args.only_with_price
            )
            if not len(unvisited):
                print("no more remaining images to download")
                break
            for id, url in unvisited:
                print(f"fetching: {id} ...")
                try:
                    result_image = sess.get(url, stream=True).content
                    # Make sure the image is actually valid.
                    Image.open(io.BytesIO(result_image)).load()
                except KeyboardInterrupt:
                    raise
                except Exception as exc:
                    db.insert_image(args.image_type, id, blob=None, error=str(exc))
                    continue
                db.insert_image(args.image_type, id, blob=result_image)


if __name__ == "__main__":
    main()
