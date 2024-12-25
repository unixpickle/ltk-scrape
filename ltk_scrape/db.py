import json
import random
import sqlite3
import time
from dataclasses import asdict, dataclass
from typing import Callable, List, Literal, Optional, Tuple

ImageSource = Literal["product", "ltk"]


@dataclass
class ProductDetails:
    id: str
    name: str
    advertiser_name: str
    advertiser_parent_id: str
    price: Optional[float]
    local_price: Optional[float]
    currency: str
    retailer_id: str
    retailer_ids: List[str]
    min_price: str
    min_sale_price: str
    max_price: str
    max_sale_price: str
    top_level_category: str


@dataclass
class Product:
    id: str
    ltk_id: str
    hyperlink: str
    image_url: str
    retailer_display_name: str
    retailer_id: str
    fetched_at: int
    details: Optional[ProductDetails]


@dataclass
class LTK:
    id: str
    hero_image: str
    hero_image_width: int
    hero_image_height: int
    video_url: str
    profile_id: str
    profile_user_id: str
    status: str
    caption: str
    share_url: str
    date_created: str
    date_updated: str
    date_published: str
    product_ids: List[str]
    fetched_at: int


def retry_if_busy(fn: Callable) -> Callable:
    def new_fn(*args, **kwargs):
        while True:
            try:
                return fn(*args, **kwargs)
            except sqlite3.OperationalError as e:
                if "database is locked" in str(e):
                    print("DB was busy...")
                    time.sleep(random.random())
                else:
                    raise

    return new_fn


class DB:
    def __init__(self, filename: str):
        self.connection = sqlite3.connect(filename)
        self._initialize_tables()

    @retry_if_busy
    def _initialize_tables(self):
        self.connection.execute(
            """
            CREATE TABLE IF NOT EXISTS ltks (
                id TEXT PRIMARY KEY,
                hero_image TEXT,
                hero_image_width INTEGER,
                hero_image_height INTEGER,
                video_url TEXT,
                profile_id TEXT,
                profile_user_id TEXT,
                status TEXT,
                caption TEXT,
                share_url TEXT,
                date_created INTEGER,  -- Store as epoch time
                date_updated INTEGER,  -- Store as epoch time
                date_published INTEGER,  -- Store as epoch time
                product_ids TEXT,  -- Comma-separated or JSON string
                fetched_at INTEGER
            );
            """
        )
        self.connection.execute(
            """
            CREATE TABLE IF NOT EXISTS products (
                id TEXT PRIMARY KEY,
                ltk_id TEXT,
                hyperlink TEXT,
                image_url TEXT,
                retailer_display_name TEXT,
                fetched_at INTEGER,
                details_id TEXT,
                name TEXT,
                advertiser_name TEXT,
                advertiser_parent_id TEXT,
                price REAL,
                local_price REAL,
                currency TEXT,
                retailer_id TEXT,
                retailer_ids TEXT,
                min_price TEXT,
                min_sale_price TEXT,
                max_price TEXT,
                max_sale_price TEXT,
                top_level_category TEXT
            );
            """
        )
        self.connection.execute(
            """
            CREATE TABLE IF NOT EXISTS visited_ltks (
                id TEXT PRIMARY KEY,
                error TEXT
            );
            """
        )
        self.connection.execute(
            """
            CREATE TABLE IF NOT EXISTS product_images (
                id TEXT PRIMARY KEY,
                data BLOB,
                error TEXT
            );
            """
        )
        self.connection.execute(
            """
            CREATE TABLE IF NOT EXISTS ltk_hero_images (
                id TEXT PRIMARY KEY,
                data BLOB,
                error TEXT
            );
            """
        )
        self.connection.commit()

    @retry_if_busy
    def upsert_products(self, products: List[Product]):
        cursor = self.connection.cursor()
        for product in products:
            obj = asdict(product)
            if product.details is not None:
                detail_obj = asdict(product.details)
                detail_obj["details_id"] = detail_obj.pop("id")
                detail_obj["retailer_ids"] = ",".join(detail_obj["retailer_ids"])
                obj.update(detail_obj)
            for key in [
                "details_id",
                "name",
                "advertiser_name",
                "advertiser_parent_id",
                "price",
                "local_price",
                "currency",
                "retailer_id",
                "retailer_ids",
                "min_price",
                "min_sale_price",
                "max_price",
                "max_sale_price",
                "top_level_category",
            ]:
                if key not in obj:
                    obj[key] = None
            cursor.execute(
                """
                INSERT OR REPLACE INTO products (
                    id, ltk_id, hyperlink, image_url, retailer_display_name, fetched_at,
                    details_id, name, advertiser_name, advertiser_parent_id, price,
                    local_price, currency, retailer_id, retailer_ids, min_price,
                    min_sale_price, max_price, max_sale_price, top_level_category
                )
                VALUES (
                    :id, :ltk_id, :hyperlink, :image_url, :retailer_display_name, :fetched_at,
                    :details_id, :name, :advertiser_name, :advertiser_parent_id, :price,
                    :local_price, :currency, :retailer_id, :retailer_ids, :min_price,
                    :min_sale_price, :max_price, :max_sale_price, :top_level_category
                );
                """,
                obj,
            )
        self.connection.commit()

    @retry_if_busy
    def upsert_ltks(self, ltks: List[LTK]):
        cursor = self.connection.cursor()
        for ltk in ltks:
            product_ids_str = ",".join(ltk.product_ids)
            ltk_data = asdict(ltk)
            ltk_data["product_ids"] = product_ids_str

            cursor.execute(
                """
                INSERT OR REPLACE INTO ltks (
                    id, hero_image, hero_image_width, hero_image_height, video_url, profile_id,
                    profile_user_id, status, caption, share_url, date_created,
                    date_updated, date_published, product_ids, fetched_at
                )
                VALUES (
                    :id, :hero_image, :hero_image_width, :hero_image_height, :video_url, :profile_id,
                    :profile_user_id, :status, :caption, :share_url, :date_created,
                    :date_updated, :date_published, :product_ids, :fetched_at
                )
                """,
                ltk_data,
            )
        self.connection.commit()

    @retry_if_busy
    def get_products(self, ids: List[str]) -> List[Product]:
        cursor = self.connection.cursor()
        query = f"""
        SELECT id, ltk_id, hyperlink, image_url, retailer_display_name, fetched_at, 
            details_id, name, advertiser_name, advertiser_parent_id, price, 
            local_price, currency, retailer_id, retailer_ids, min_price, 
            min_sale_price, max_price, max_sale_price, top_level_category
        FROM products
        WHERE id IN ({','.join('?' for _ in ids)})
        """
        cursor.execute(query, ids)
        rows = cursor.fetchall()

        products = []
        for row in rows:
            (
                id,
                ltk_id,
                hyperlink,
                image_url,
                retailer_display_name,
                fetched_at,
                details_id,
                name,
                advertiser_name,
                advertiser_parent_id,
                price,
                local_price,
                currency,
                retailer_id,
                retailer_ids,
                min_price,
                min_sale_price,
                max_price,
                max_sale_price,
                top_level_category,
            ) = row

            details = None
            if details_id:
                details = ProductDetails(
                    id=details_id,
                    name=name,
                    advertiser_name=advertiser_name,
                    advertiser_parent_id=advertiser_parent_id,
                    price=price,
                    local_price=local_price,
                    currency=currency,
                    retailer_id=retailer_id,
                    retailer_ids=json.loads(retailer_ids) if retailer_ids else [],
                    min_price=min_price,
                    min_sale_price=min_sale_price,
                    max_price=max_price,
                    max_sale_price=max_sale_price,
                    top_level_category=top_level_category,
                )

            product = Product(
                id=id,
                ltk_id=ltk_id,
                hyperlink=hyperlink,
                image_url=image_url,
                retailer_display_name=retailer_display_name,
                retailer_id=retailer_id,
                fetched_at=fetched_at,
                details=details,
            )
            products.append(product)

        return products

    @retry_if_busy
    def has_visited_ltk(self, id: str) -> Tuple[bool, Optional[str]]:
        cursor = self.connection.cursor()
        cursor.execute("SELECT error FROM visited_ltks WHERE id = ?", (id,))
        result = cursor.fetchone()
        if result:
            return True, result[0]  # Visited, return error if present
        return False, None  # Not visited

    @retry_if_busy
    def mark_visited_ltk(self, id: str, error: Optional[str]):
        cursor = self.connection.cursor()
        cursor.execute(
            """
            INSERT OR REPLACE INTO visited_ltks (id, error)
            VALUES (?, ?)
            """,
            (id, error),
        )
        self.connection.commit()

    @retry_if_busy
    def unvisited_ltks(self, limit: int) -> List[Tuple[str, str]]:
        query = """
        SELECT ltks.id, ltks.share_url
        FROM ltks
        LEFT JOIN visited_ltks ON ltks.id = visited_ltks.id
        WHERE visited_ltks.id IS NULL
        LIMIT ?;
        """
        result = self.connection.execute(query, (limit,)).fetchall()
        return [tuple(x) for x in result]

    @retry_if_busy
    def missing_images(
        self, source: ImageSource, limit: int, only_with_price: bool = False
    ) -> List[Tuple[str, str]]:
        """Get a collection of (id, url) tuples."""

        image_table = "product_images" if source == "product" else "ltk_hero_images"
        listing_table = "products" if source == "product" else "ltks"
        url_field = "image_url" if source == "product" else "hero_image"
        where_clause = (
            f"AND {listing_table}.price is not null"
            if source == "product" and only_with_price
            else ""
        )

        query = f"""
        SELECT {listing_table}.id, {listing_table}.{url_field}
        FROM {listing_table}
        LEFT JOIN {image_table} ON {image_table}.id = {listing_table}.id
        WHERE {image_table}.id IS NULL {where_clause}
        LIMIT ?;
        """
        result = self.connection.execute(query, (limit,)).fetchall()
        return [tuple(x) for x in result]

    @retry_if_busy
    def insert_image(
        self,
        source: ImageSource,
        id: str,
        blob: Optional[bytes],
        error: Optional[str] = None,
    ):
        table = "product_images" if source == "product" else "ltk_hero_images"
        cursor = self.connection.cursor()
        cursor.execute(
            f"INSERT OR REPLACE INTO {table} (id, data, error) VALUES (?, ?, ?)",
            (id, blob, error),
        )
        self.connection.commit()
