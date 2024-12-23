import sqlite3
from dataclasses import asdict, dataclass
from typing import List, Optional, Tuple


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


class DB:
    def __init__(self, filename: str):
        self.connection = sqlite3.connect(filename)
        self._initialize_tables()

    def _initialize_tables(self):
        self.connection.execute(
            """
            CREATE TABLE IF NOT EXISTS ltks (
                id TEXT PRIMARY KEY,
                hero_image TEXT,
                hero_image_width INTEGER,
                hero_image_height INTEGER,
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
        self.connection.commit()

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
                INSERT INTO products (id, ltk_id, hyperlink, image_url, retailer_display_name, fetched_at,
                                      details_id, name, advertiser_name, advertiser_parent_id, price,
                                      local_price, currency, retailer_id, retailer_ids, min_price,
                                      min_sale_price, max_price, max_sale_price, top_level_category)
                VALUES (:id, :ltk_id, :hyperlink, :image_url, :retailer_display_name, :fetched_at,
                        :details_id, :name, :advertiser_name, :advertiser_parent_id, :price,
                        :local_price, :currency, :retailer_id, :retailer_ids, :min_price,
                        :min_sale_price, :max_price, :max_sale_price, :top_level_category)
                ON CONFLICT(id) DO UPDATE SET
                    ltk_id = excluded.ltk_id,
                    hyperlink = excluded.hyperlink,
                    image_url = excluded.image_url,
                    retailer_display_name = excluded.retailer_display_name,
                    fetched_at = excluded.fetched_at,
                    details_id = excluded.details_id,
                    name = excluded.name,
                    advertiser_name = excluded.advertiser_name,
                    advertiser_parent_id = excluded.advertiser_parent_id,
                    price = excluded.price,
                    local_price = excluded.local_price,
                    currency = excluded.currency,
                    retailer_id = excluded.retailer_id,
                    retailer_ids = excluded.retailer_ids,
                    min_price = excluded.min_price,
                    min_sale_price = excluded.min_sale_price,
                    max_price = excluded.max_price,
                    max_sale_price = excluded.max_sale_price,
                    top_level_category = excluded.top_level_category
                    ;
                """,
                obj,
            )
        self.connection.commit()

    def upsert_ltks(self, ltks: List[LTK]):
        cursor = self.connection.cursor()
        for ltk in ltks:
            product_ids_str = ",".join(ltk.product_ids)
            ltk_data = asdict(ltk)
            ltk_data["product_ids"] = product_ids_str

            cursor.execute(
                """
                INSERT INTO ltks (
                    id, hero_image, hero_image_width, hero_image_height, profile_id,
                    profile_user_id, status, caption, share_url, date_created,
                    date_updated, date_published, product_ids, fetched_at
                )
                VALUES (
                    :id, :hero_image, :hero_image_width, :hero_image_height, :profile_id,
                    :profile_user_id, :status, :caption, :share_url, :date_created,
                    :date_updated, :date_published, :product_ids, :fetched_at
                )
                ON CONFLICT(id) DO UPDATE SET
                    hero_image = excluded.hero_image,
                    hero_image_width = excluded.hero_image_width,
                    hero_image_height = excluded.hero_image_height,
                    profile_id = excluded.profile_id,
                    profile_user_id = excluded.profile_user_id,
                    status = excluded.status,
                    caption = excluded.caption,
                    share_url = excluded.share_url,
                    date_created = excluded.date_created,
                    date_updated = excluded.date_updated,
                    date_published = excluded.date_published,
                    product_ids = excluded.product_ids,
                    fetched_at = excluded.fetched_at;
                """,
                ltk_data,
            )
        self.connection.commit()

    def has_visited_ltk(self, id: str) -> Tuple[bool, Optional[str]]:
        cursor = self.connection.cursor()
        cursor.execute("SELECT error FROM visited_ltks WHERE id = ?", (id,))
        result = cursor.fetchone()
        if result:
            return True, result[0]  # Visited, return error if present
        return False, None  # Not visited

    def mark_visited_ltk(self, id: str, error: Optional[str]):
        cursor = self.connection.cursor()
        cursor.execute(
            """
            INSERT INTO visited_ltks (id, error)
            VALUES (?, ?)
            ON CONFLICT(id) DO UPDATE SET
                error = excluded.error;
            """,
            (id, error),
        )
        self.connection.commit()

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
