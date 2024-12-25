import argparse
import io
import re

import requests
from PIL import Image

from .db import DB


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db_path", type=str, default="db.db")
    args = parser.parse_args()

    db = DB(args.db_path)

    with requests.Session() as sess:
        while True:
            unvisited = db.missing_usernames(50)
            if not len(unvisited):
                print("no more remaining usernames to fetch")
                break
            for id, url in unvisited:
                print(f"fetching: {id} ...")
                try:
                    response = requests.head(url, allow_redirects=False, timeout=10)

                    redirect_location = response.headers.get("Location")
                    if redirect_location and response.status_code in (
                        301,
                        302,
                        303,
                        307,
                        308,
                    ):
                        # Regex pattern to match the username part of the URL
                        match = re.search(
                            r"https://www\.shopltk\.com/explore/([^/]+)/",
                            redirect_location,
                        )
                        if match:
                            username = match.group(1)
                        else:
                            raise ValueError(
                                f"username not found in redirect URL: {redirect_location}"
                            )
                    else:
                        raise ValueError(f"no redirect location found for {url}")
                except KeyboardInterrupt:
                    raise
                except Exception as exc:
                    db.insert_username(id, username=None, error=str(exc))
                    continue
                db.insert_username(id, username)


if __name__ == "__main__":
    main()
