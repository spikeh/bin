import asyncio
import re
import sqlite3
from datetime import datetime

from pyppeteer import launch


async def scrape(browser):
    url = "https://www.fidelity.co.uk/factsheet-data/factsheet/GB00BMJJJF91-hsbc-ftse-all-world-index-c-acc/key-statistics"
    page = await browser.newPage()
    await page.goto(url)
    price_str = await page.Jeval("h3.detail_value", "(element) => element.textContent")
    price = re.search("£(\d+\.\d+)$", price_str.strip())
    price_updated = await page.Jeval(
        "div.detail__price-updated > div:first-child",
        "(element) => element.textContent",
    )
    date_str = re.search("Prices updated as at (.*)$", price_updated)
    date_parsed = datetime.strptime(date_str.group(1).strip(), "%d %b %Y")
    return [("GB00BMJJJF91", date_parsed.strftime("%Y-%m-%d"), float(price.group(1)))]


async def main():
    con = sqlite3.connect("stocks.db")
    cur = con.cursor()
    cur.execute(
        "CREATE TABLE IF NOT EXISTS stocks (isin TEXT NOT NULL, date TEXT NOT NULL, price REAL NOT NULL, PRIMARY KEY (isin, date))"
    )

    browser = await launch()
    data = await scrape(browser)

    cur.executemany("INSERT OR IGNORE INTO stocks VALUES (?, ?, ?)", data)
    await browser.close()
    con.commit()
    con.close()


asyncio.get_event_loop().run_until_complete(main())
