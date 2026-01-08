import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from pathlib import Path

import ijson
from fastapi import FastAPI, Query, Request
from httpx import URL, AsyncClient
from pydantic import BaseModel
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address
from typing_extensions import Annotated, Literal


class FinishEntry(BaseModel):
    url: str
    price: str


class VendorEntry(BaseModel):
    nonfoil: FinishEntry | None = None
    foil: FinishEntry | None = None
    etched: FinishEntry | None = None


class Price(BaseModel):
    manaPool: VendorEntry | None = None
    cardKingdom: VendorEntry | None = None


DATA_DIR = Path("/data") if Path("/data").exists() else Path("./data")
DATA_DIR.mkdir(exist_ok=True)

prices: dict[str, Price] = {}


def format_price(price: float, currency: Literal["$", "€", "£"]) -> str:
    return f"{currency}{price:.2f}"


def is_path_fresh(path: Path, max_age: timedelta) -> bool:
    return path.exists() and (
        datetime.now() - datetime.fromtimestamp(path.stat().st_mtime) < max_age
    )


async def get_and_save(
    url: URL,
    path: Path,
) -> None:
    async with AsyncClient(timeout=30) as client:
        async with client.stream(
            "GET", url, headers={"User-Agent": "Scrybuy API/1.0.0"}
        ) as response:
            response.raise_for_status()
            with open(path, "wb") as file:
                async for chunk in response.aiter_bytes():
                    file.write(chunk)


async def load_mana_pool_prices():
    path = DATA_DIR / "mana_pool_prices.json"

    if not is_path_fresh(path, timedelta(hours=0.5)):
        await get_and_save(URL("https://manapool.com/api/v1/prices/singles"), path)

    with open(path, "rb") as file:
        for item in ijson.items(file, "data.item"):
            if item["scryfall_id"] is None:
                continue
            scryfall_id = item["scryfall_id"]
            if scryfall_id not in prices:
                prices[scryfall_id] = Price()
            entry = prices[scryfall_id].manaPool or VendorEntry()
            if item["price_cents"]:
                entry.nonfoil = FinishEntry(
                    url=item["url"],
                    price=format_price(item["price_cents"] / 100.0, "$"),
                )
            if item["price_cents_foil"]:
                entry.foil = FinishEntry(
                    url=f"{item['url']}?finish=foil",
                    price=format_price(item["price_cents_foil"] / 100.0, "$"),
                )
            if item["price_cents_etched"]:
                entry.etched = FinishEntry(
                    url=f"{item['url']}?finish=foil",
                    price=format_price(item["price_cents_etched"] / 100.0, "$"),
                )
            prices[scryfall_id].manaPool = entry


async def load_card_kingdom_prices():
    path = DATA_DIR / "card_kingdom_prices.json"

    if not is_path_fresh(path, timedelta(hours=0.5)):
        await get_and_save(URL("https://api.cardkingdom.com/api/pricelist"), path)

    with open(path, "rb") as file:
        for item in ijson.items(file, "data.item"):
            if item["scryfall_id"] is None:
                continue
            scryfall_id = item["scryfall_id"]
            if scryfall_id not in prices:
                prices[scryfall_id] = Price()
            entry = prices[scryfall_id].cardKingdom or VendorEntry()
            subentry = FinishEntry(
                url=f"https://www.cardkingdom.com{item['url']}",
                price=format_price(float(item["price_retail"]), "$"),
            )
            if "etched" in item["url"]:
                entry.etched = subentry
            elif "foil" in item["url"]:
                entry.foil = subentry
            else:
                entry.nonfoil = subentry
            prices[scryfall_id].cardKingdom = entry


async def refresh_prices():
    try:
        await asyncio.gather(
            load_mana_pool_prices(),
            load_card_kingdom_prices(),
        )
    except Exception as e:
        print(f"Error refreshing prices: {e}")


async def periodic_refresh(interval: timedelta):
    while True:
        await asyncio.sleep(interval.total_seconds())
        await refresh_prices()


@asynccontextmanager
async def lifespan(
    app: FastAPI,
) -> AsyncIterator[None]:
    await refresh_prices()
    task = asyncio.create_task(periodic_refresh(timedelta(hours=1)))
    yield
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


limiter = Limiter(key_func=get_remote_address)
app = FastAPI(title="ScryBuy API", version="1.0.0", lifespan=lifespan)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)


@app.get("/prices", summary="Get price information for the given Scryfall IDs.")
@limiter.limit("1/second")
async def get_prices(
    request: Request,
    scryfall_ids: Annotated[
        list[str],
        Query(
            alias="id",
            title="Scryfall IDs",
            description="List of Scryfall card IDs to retrieve price information for",
        ),
    ] = [],
) -> list[Price]:
    return [
        prices.get(scryfall_id.replace('"', ""), Price())
        for scryfall_id in scryfall_ids
    ]
