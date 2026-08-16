import os
from contextlib import closing
from datetime import timedelta
from decimal import Decimal
from typing import Annotated, Literal

import ijson
import psycopg2
import requests
from fastapi import FastAPI, Query, Request
from pydantic import BaseModel
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address

CENTS_PER_DOLLAR = Decimal(100)
HTTP_TIMEOUT = timedelta(seconds=30)
REQUEST_LIMIT = timedelta(seconds=1)


class FinishEntry(BaseModel):
    url: str
    price: str


class VendorEntry(BaseModel):
    nonfoil: FinishEntry | None = None
    foil: FinishEntry | None = None
    etched: FinishEntry | None = None


class PriceEntry(BaseModel):
    manaPool: VendorEntry | None = None
    cardKingdom: VendorEntry | None = None
    cardsphere: VendorEntry | None = None


conn = psycopg2.connect(os.environ["DATABASE_URL"])
headers = {"User-Agent": "Scrybuy API/2.0.0"}


def _insert_price(
    cur: psycopg2.extensions.cursor,
    scryfall_id: str,
    vendor: Literal["card_kingdom", "cardsphere", "mana_pool"],
    finish: Literal["nonfoil", "foil", "etched"],
    price: Decimal,
    url: str,
) -> None:
    cur.execute(
        """
        INSERT INTO prices (scryfall_id, vendor, finish, price, url)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (scryfall_id, vendor, finish) DO NOTHING
        """,
        (scryfall_id, vendor, finish, price, url),
    )


def fetch_card_kingdom_prices() -> None:
    with (
        closing(conn),
        conn,
        conn.cursor() as cur,
        requests.get(
            "https://api.cardkingdom.com/api/pricelist",
            stream=True,
            timeout=HTTP_TIMEOUT.total_seconds(),
            headers=headers,
        ) as resp,
    ):
        resp.raise_for_status()
        resp.raw.decode_content = True
        cur.execute("DELETE FROM prices WHERE vendor = 'card_kingdom'")
        for item in ijson.items(resp.raw, "data.item"):
            if item["scryfall_id"] is None:
                continue
            url = f"https://www.cardkingdom.com/{item['url']}"
            if "etched" in item["url"]:
                _insert_price(
                    cur,
                    item["scryfall_id"],
                    "card_kingdom",
                    "etched",
                    Decimal(item["price_retail"]),
                    url,
                )
            elif "foil" in item["url"]:
                _insert_price(
                    cur,
                    item["scryfall_id"],
                    "card_kingdom",
                    "foil",
                    Decimal(item["price_retail"]),
                    url,
                )
            else:
                _insert_price(
                    cur,
                    item["scryfall_id"],
                    "card_kingdom",
                    "nonfoil",
                    Decimal(item["price_retail"]),
                    url,
                )


def fetch_mana_pool_prices() -> None:
    with (
        closing(conn),
        conn,
        conn.cursor() as cur,
        requests.get(
            "https://manapool.com/api/v1/prices/singles",
            stream=True,
            timeout=HTTP_TIMEOUT.total_seconds(),
            headers=headers,
        ) as resp,
    ):
        resp.raise_for_status()
        resp.raw.decode_content = True
        cur.execute("DELETE FROM prices WHERE vendor = 'mana_pool'")
        for item in ijson.items(resp.raw, "data.item"):
            if item["scryfall_id"] is None:
                continue
            if item["price_cents"]:
                _insert_price(
                    cur,
                    item["scryfall_id"],
                    "mana_pool",
                    "nonfoil",
                    Decimal(item["price_cents"]) / CENTS_PER_DOLLAR,
                    item["url"],
                )
            if item["price_cents_foil"]:
                _insert_price(
                    cur,
                    item["scryfall_id"],
                    "mana_pool",
                    "foil",
                    Decimal(item["price_cents_foil"]) / CENTS_PER_DOLLAR,
                    item["url"],
                )
            if item["price_cents_etched"]:
                _insert_price(
                    cur,
                    item["scryfall_id"],
                    "mana_pool",
                    "etched",
                    Decimal(item["price_cents_etched"]) / CENTS_PER_DOLLAR,
                    item["url"],
                )


limiter = Limiter(key_func=get_remote_address)
app = FastAPI(title="ScryBuy API", version="2.0.0")
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)


@app.get("/prices", summary="Get price information for the given Scryfall IDs.")
@limiter.limit(f"{int(REQUEST_LIMIT.total_seconds())}/second")
async def get_prices(
    request: Request,
    scryfall_ids: Annotated[
        list[str],
        Query(
            alias="id",
            title="Scryfall IDs",
            description="List of Scryfall card IDs to retrieve price information for.",
        ),
    ] = [],
) -> dict[str, PriceEntry]:
    price_entries: dict[str, PriceEntry] = {}
    with conn, conn.cursor() as cur:
        cur.execute(
            "SELECT scryfall_id, vendor, finish, price, url FROM prices WHERE scryfall_id = ANY (%s)",
            (scryfall_ids,),
        )
        for scryfall_id, vendor, finish, price, url in cur:
            finish_entry = FinishEntry(url=url, price=f"${price:.2f}")
            price_entry = price_entries.setdefault(scryfall_id, PriceEntry())

            if vendor == "card_kingdom":
                if price_entry.cardKingdom is None:
                    price_entry.cardKingdom = VendorEntry()
                vendor_entry = price_entry.cardKingdom
            elif vendor == "cardsphere":
                if price_entry.cardsphere is None:
                    price_entry.cardsphere = VendorEntry()
                vendor_entry = price_entry.cardsphere
            elif vendor == "mana_pool":
                if price_entry.manaPool is None:
                    price_entry.manaPool = VendorEntry()
                vendor_entry = price_entry.manaPool

            if finish == "nonfoil":
                vendor_entry.nonfoil = finish_entry
            elif finish == "foil":
                vendor_entry.foil = finish_entry
            elif finish == "etched":
                vendor_entry.etched = finish_entry

    return price_entries
