import json
import shutil
from dataclasses import dataclass
from typing import Dict, Optional
from datetime import datetime

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service

from .db import LTK, Product, ProductDetails


@dataclass
class LTKPost:
    ltks: Dict[str, LTK]
    products: Dict[str, Product]


class LTKClient:
    def __init__(self):
        options = Options()
        options.add_argument("--headless")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")

        service = Service(shutil.which("chromedriver"))
        self.driver = webdriver.Chrome(service=service, options=options)

    def __del__(self):
        self.driver.quit()

    def fetch_post(self, post_url: str) -> LTKPost:
        self.driver.get(post_url)
        self.driver.implicitly_wait(10)

        script = "return JSON.stringify(__NUXT__.state.ltks.ltks);"
        ltks_json = self.driver.execute_script(script)
        ltks_data = json.loads(ltks_json)

        script = "return JSON.stringify(__NUXT__.state.products.products);"
        products_json = self.driver.execute_script(script)
        products_data = json.loads(products_json)

        script = "return JSON.stringify(__NUXT__.state['media-objects'].mediaObjects);"
        media_objects_json = self.driver.execute_script(script)
        media_objects = json.loads(media_objects_json)

        script = (
            "return JSON.stringify(__NUXT__.state['product-details'].productDetails);"
        )
        product_details_json = self.driver.execute_script(script)
        product_details_data = json.loads(product_details_json)

        ltks = {
            ltk_id: LTK(
                id=ltk_id,
                hero_image=data["heroImage"],
                hero_image_width=data["heroImageWidth"],
                hero_image_height=data["heroImageHeight"],
                video_url=media_objects.get(data.get("videoMediaId"), {}).get(
                    "mediaCdnUrl"
                ),
                profile_id=data["profileId"],
                profile_user_id=data["profileUserId"],
                status=data["status"],
                caption=data["caption"],
                share_url=data["shareUrl"],
                date_created=parse_timestamp(data["dateCreated"]),
                date_updated=parse_timestamp(data["dateUpdated"]),
                date_published=parse_timestamp(data["datePublished"]),
                product_ids=data.get("productIds", []),
                fetched_at=data["fetchedAt"],
            )
            for ltk_id, data in ltks_data.items()
        }

        product_details = {
            id: ProductDetails(
                id=data["id"],
                name=data["name"],
                advertiser_name=data["advertiserName"],
                advertiser_parent_id=data["advertiserParentId"],
                price=maybe_parse_float(data["price"]),
                local_price=maybe_parse_float(data["localPrice"]),
                currency=data["currency"],
                retailer_id=data["retailerId"],
                retailer_ids=data["retailerIds"],
                min_price=data["minPrice"],
                min_sale_price=data["minSalePrice"],
                max_price=data["maxPrice"],
                max_sale_price=data["maxSalePrice"],
                top_level_category=data["topLevelCategory"],
            )
            for id, data in product_details_data.items()
        }

        products = {
            product_id: Product(
                id=product_id,
                ltk_id=data["ltkId"],
                hyperlink=data["hyperlink"],
                image_url=data["imageUrl"],
                retailer_display_name=data["retailerDisplayName"],
                retailer_id=data["retailerId"],
                fetched_at=data["fetchedAt"],
                details=product_details.get(data["productDetailsId"]),
            )
            for product_id, data in products_data.items()
        }

        return LTKPost(ltks=ltks, products=products)


def maybe_parse_float(x: str) -> Optional[float]:
    try:
        return float(x)
    except ValueError:
        return None


def parse_timestamp(ts: str) -> int:
    parsed_date = datetime.fromisoformat(ts)
    return int(parsed_date.timestamp())
