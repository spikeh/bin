import asyncio
import sqlite3
from datetime import datetime

from pyppeteer import launch

ISINS = [
    "GB00B59G4Q73",  # World ex UK
    "GB00B3X7QG63",  # UK
    "IE00B50MZ724",  # EM
]


async def scrape(isin, browser):
    url = "https://markets.ft.com/data/funds/tearsheet/historical?s={}:GBP".format(isin)
    page = await browser.newPage()
    await page.goto(url)
    table = await page.querySelector(
        "table.mod-tearsheet-historical-prices__results > tbody"
    )
    rows = await table.querySelectorAll("tr")
    data = []
    for row in rows:
        date = await row.querySelector("td:first-child > span:first-child")
        date_str = await (await date.getProperty("textContent")).jsonValue()
        price = await row.querySelector("td:nth-child(2)")
        price_str = await (await price.getProperty("textContent")).jsonValue()
        date_parsed = datetime.strptime(date_str, "%A, %B %d, %Y")
        data.append((isin, date_parsed.strftime("%Y-%m-%d"), float(price_str)))
    data.sort()
    return data


async def main():
    con = sqlite3.connect("stocks.db")
    cur = con.cursor()
    cur.execute(
        "CREATE TABLE IF NOT EXISTS stocks (isin TEXT NOT NULL, date TEXT NOT NULL, price REAL NOT NULL, PRIMARY KEY (isin, date))"
    )

    browser = await launch(executablePath="/usr/bin/chromium")
    work = [scrape(isin, browser) for isin in ISINS]
    data = await asyncio.gather(*work)

    for d in data:
        cur.executemany("INSERT OR IGNORE INTO stocks VALUES (?, ?, ?)", d)
    await browser.close()
    con.commit()
    con.close()


asyncio.get_event_loop().run_until_complete(main())
