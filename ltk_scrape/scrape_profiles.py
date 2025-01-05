import argparse
from typing import Any, Dict, List
import requests

from ltk_scrape.client import maybe_parse_float, parse_timestamp

from .db import DB, LTK, Product, ProductDetails


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db_path", type=str, default="db.db")
    parser.add_argument("--max_per_user", type=int, default=50)
    parser.add_argument("--proxy", type=str, default=None)
    args = parser.parse_args()

    db = DB(args.db_path)
    proxies = None if args.proxy is None else {"http": args.proxy, "https": args.proxy}

    profile_id_to_count = db.profile_id_counts()

    with requests.Session() as sess:
        for profile, count in sorted(profile_id_to_count.items(), key=lambda x: x[1]):
            print(f"scraping profile {profile} which had {count} existing posts...")
            payload = {
                "query": "",
                "ranking": "recent",
                "profile_id": profile,
                "page": 0,
                "limit": args.max_per_user,
                "analytics": ["version:3.458.0-COA-1609.1", "platform:web"],
                "filters": [],
            }
            response = sess.post(
                "https://api-gateway.rewardstyle.com/api/ltk/v2/search/shop",
                timeout=10,
                proxies=proxies,
                json=payload,
            ).json()
            post_ids = list(set(x["objectID"] for x in response["hits"]))
            scrape_ids = db.unscraped_ltks(post_ids)
            print(f"scraping {len(scrape_ids)} posts...")

            resp = fetch_all_ltks(sess, proxies, scrape_ids)

            all_product_ids = list(set(obj["id"] for obj in resp["products"]))
            scrape_product_ids = db.unscraped_products(all_product_ids)

            detail_ids = list(
                set(
                    obj["product_details_id"]
                    for obj in resp["products"]
                    if obj["id"] in scrape_product_ids
                )
            )

            details_resp = fetch_all_product_details(sess, proxies, detail_ids)

            product_details = {
                data["id"]: ProductDetails(
                    id=data["id"],
                    name=data["name"],
                    advertiser_name=data["advertiser_name"],
                    advertiser_parent_id=data["advertiser_parent_id"],
                    price=maybe_parse_float(data["price"]),
                    local_price=maybe_parse_float(data["local_price"]),
                    currency=data["currency"],
                    retailer_id=data["retailer_id"],
                    retailer_ids=data["retailer_ids"],
                    min_price=data["min_price"],
                    min_sale_price=data["min_sale_price"],
                    max_price=data["max_price"],
                    max_sale_price=data["max_sale_price"],
                    top_level_category=data["top_level_category"],
                )
                for data in details_resp["product_details"]
            }

            media_objects = {obj["id"]: obj for obj in resp["media_objects"]}

            ltks = {
                data["id"]: LTK(
                    id=data["id"],
                    hero_image=data["hero_image"],
                    hero_image_width=data["hero_image_width"],
                    hero_image_height=data["hero_image_height"],
                    video_url=media_objects.get(data.get("video_media_id"), {}).get(
                        "media_cdn_url"
                    ),
                    profile_id=data["profile_id"],
                    profile_user_id=data["profile_user_id"],
                    status=data["status"],
                    caption=data["caption"],
                    share_url=data["share_url"],
                    date_created=parse_timestamp(data["date_created"]),
                    date_updated=parse_timestamp(data["date_updated"]),
                    date_published=parse_timestamp(data["date_published"]),
                    product_ids=data.get("product_ids", []),
                    fetched_at=data.get("fetched_at"),
                )
                for data in resp["ltks"]
                if data["id"] in scrape_ids
            }

            products = {
                data["id"]: Product(
                    id=data["id"],
                    ltk_id=data["ltk_id"],
                    hyperlink=data["hyperlink"],
                    image_url=data["image_url"],
                    retailer_display_name=data["retailer_display_name"],
                    retailer_id=data["retailer_id"],
                    fetched_at=data.get("fetched_at"),
                    details=product_details.get(data["product_details_id"]),
                )
                for data in resp["products"]
                if data["id"] in scrape_product_ids
            }

            db.upsert_ltks(list(ltks.values()))
            db.upsert_products(list(products.values()))


def fetch_all_ltks(
    sess: requests.Session, proxies: Any, ids: List[str], batch: int = 50
) -> Dict[str, Any]:
    if not len(ids):
        return dict(products=[], media_objects=[], ltks=[])
    all_results = []
    for i in range(0, len(ids), batch):
        url = "https://api-gateway.rewardstyle.com/api/ltk/v2/ltks"
        query_params = [f"ids[]={id}" for id in ids[i : i + batch]]
        query_params.extend([f"limit={batch}", "link_types\[\]=LTK_WEB"])
        resp = sess.get(
            url + "?" + "&".join(query_params), timeout=10, proxies=proxies
        ).json()
        all_results.append(resp)
    result = all_results[0]
    for next_result in all_results[1:]:
        for k in ["products", "media_objects", "ltks"]:
            result[k].extend(next_result[k])
    return result


def fetch_all_product_details(
    sess: requests.Session, proxies: Any, ids: List[str], batch: int = 50
) -> Dict[str, Any]:
    if not len(ids):
        return dict(product_details=[])
    all_results = []
    for i in range(0, len(ids), batch):
        url = "https://api-gateway.rewardstyle.com/api/ltk/v2/product_details/"
        query_params = [f"ids[]={id}" for id in ids[i : i + batch]]
        resp = sess.get(
            url + "?" + "&".join(query_params), timeout=10, proxies=proxies
        ).json()
        all_results.append(resp)
    result = all_results[0]
    for next_result in all_results[1:]:
        for k in ["product_details"]:
            result[k].extend(next_result[k])
    return result


if __name__ == "__main__":
    main()
