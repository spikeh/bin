import sqlite3

from flask import Flask, g

app = Flask(__name__)

DATABASE = "/home/spikeh/stocks.db"


def get_db():
    db = getattr(g, "_database", None)
    if db is None:
        db = g._database = sqlite3.connect(DATABASE)
    db.row_factory = sqlite3.Row
    return db


@app.teardown_appcontext
def close_connection(exception):
    db = getattr(g, "_database", None)
    if db is not None:
        db.close()


def query_db(query, args=(), one=False):
    cur = get_db().execute(query, args)
    rv = cur.fetchall()
    cur.close()
    return (rv[0] if rv else None) if one else rv


@app.route("/<isin>/<date>")
def get_for_isin(isin, date):
    ret = {
        "isin": isin,
        "data": [],
    }
    for row in query_db(
        "SELECT * FROM stocks WHERE isin = ? AND date <= ?", (isin, date)
    ):
        ret["data"].append([row["date"], row["price"]])
    return ret
