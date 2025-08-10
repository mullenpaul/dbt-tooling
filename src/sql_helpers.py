import sqlite3


def create_sqlite(path):
    con = sqlite3.connect(path)
    con.close()