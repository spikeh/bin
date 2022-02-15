import asyncio
import sqlite3
from datetime import datetime
from decimal import Decimal

from pyppeteer import launch

MS_ID_TO_ISIN = {
    "VAUSA0K80G": "GB00B6VZKW79",  # World ex UK
    "VAUSA0K80E": "GB00B50Z6894",  # UK
    "VAUSA0K804": "GB00B79N5699",  # EM
}


async def scrape(ms_id, browser):
    url = "https://www.morningstar.co.uk/uk/snapshot/snapshot.aspx?id={}&investmentType=SA".format(
        ms_id
    )
    page = await browser.newPage()
    await page.setJavaScriptEnabled(False)
    await page.goto(url)
    date_str = await page.Jeval(
        "table.overviewKeyStatsTable tr:nth-child(2) td:nth-child(1) > span",
        "(element) => element.textContent",
    )
    date_parsed = datetime.strptime(date_str, "%d/%m/%Y")
    nav = await page.Jeval(
        "table.overviewKeyStatsTable tr:nth-child(2) td:nth-child(3)",
        "(element) => element.textContent",
    )
    return (
        MS_ID_TO_ISIN[ms_id],
        date_parsed.strftime("%Y-%m-%d"),
        float(format(Decimal.from_float(float(nav.split()[1]) / 100), ".4f")),
    )


async def main():
    con = sqlite3.connect("stocks.db")
    cur = con.cursor()
    cur.execute(
        "CREATE TABLE IF NOT EXISTS stocks (isin TEXT NOT NULL, date TEXT NOT NULL, price REAL NOT NULL, PRIMARY KEY (isin, date))"
    )

    browser = await launch(executablePath="/usr/bin/chromium")
    work = [scrape(ms_id, browser) for ms_id in MS_ID_TO_ISIN]
    data = await asyncio.gather(*work)
    cur.executemany("INSERT OR IGNORE INTO stocks VALUES (?, ?, ?)", data)

    await browser.close()
    con.commit()
    con.close()


asyncio.get_event_loop().run_until_complete(main())
