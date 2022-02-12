import asyncio
import sqlite3

import requests


API_KEY = ""

ISINS = {
    "GOOG": "US02079K1079",
    "FB": "US30303M1027",
    "HMEF.LON": "IE00B5SSQT16",
    "IEEM.LON": "IE00B0M63177",
    "VWRP.LON": "IE00BK5BQT80",
    "VWRL.LON": "IE00B3RBWM25",
    "VWRD.LON": "VWRD.L",
    "VEVE.LON": "IE00BKX55T58",
}


async def query(symbol, isin):
    while True:
        url = "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={}&apikey={}".format(
            symbol, API_KEY
        )
        r = requests.get(url)
        json = r.json()
        data = []
        if "Note" not in json:
            # process json
            for date, d in json["Time Series (Daily)"].items():
                price = d["4. close"]
                data.append((isin, date, float(price)))
            return data
        await asyncio.sleep(61)


async def main():
    con = sqlite3.connect("stocks.db")
    cur = con.cursor()
    cur.execute(
        "CREATE TABLE IF NOT EXISTS stocks (isin TEXT NOT NULL, date TEXT NOT NULL, price REAL NOT NULL, PRIMARY KEY (isin, date))"
    )

    work = [query(symbol, isin) for symbol, isin in ISINS.items()]
    data = await asyncio.gather(*work)

    for d in data:
        cur.executemany("INSERT OR IGNORE INTO stocks VALUES (?, ?, ?)", d)
    await browser.close()
    con.commit()
    con.close()


asyncio.get_event_loop().run_until_complete(main())
