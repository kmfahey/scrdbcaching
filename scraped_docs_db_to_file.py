#!/usr/bin/python3

import decouple
import os
import sys

from logging import Logger
from MySQLdb.cursors import DictCursor
from MySQLdb import Connect, Connection
from scrapygenl import set_up_logging, text2slug


MYSQL_USERNAME: str = decouple.config("MYSQL_USERNAME")
MYSQL_PASSWORD: str = decouple.config("MYSQL_PASSWORD")
MYSQL_DATABASE: str = decouple.config("MYSQL_DATABASE")
MYSQL_HOST: str = decouple.config("MYSQL_HOST")
MYSQL_CHARSET: str = decouple.config("MYSQL_CHARSET")


if len(sys.argv) < 3:
    print(
        "Please specify at minimum 2 arguments: a directory and a numerical "
        "ID of a row in the scraping.pages2 MySQL database."
    )
    exit(1)

dir: str = sys.argv[1]

if not os.path.exists(dir):
    print("The first argument must be a path to a directory that exists.")
    exit(1)

ids: list[str | int] = list(sys.argv[2:])

try:
    for i in range(len(ids)):
        ids[i] = int(ids[i])
except ValueError:
    print(
        f"Every argument after the first must be an integer. (Argument "
        f"'{ids[i]}' is not an integer."
    )
    exit(1)

logger: Logger = set_up_logging(sys.argv[0].removesuffix((".py")))

logger.info("opening connection to database")

dbconn: Connection = Connect(
    user=MYSQL_USERNAME,
    password=MYSQL_PASSWORD,
    database=MYSQL_DATABASE,
    host=MYSQL_HOST,
    charset=MYSQL_CHARSET,
)
dbcurs: DictCursor = dbconn.cursor(DictCursor)

for _id in ids:
    dbcurs.execute(
        "SELECT url, body FROM pages2 WHERE _id = %(_id)s;", dict(_id=_id)
    )
    record: None | dict[str, str] = dbcurs.fetchone()
    if record is None:
        print(
            f"ERROR: value of _id {_id} does not correspond to a row in the "
            "scraping.pages2 table!"
        )
        exit(1)
    url_slug = text2slug(record["url"])
    if not url_slug.endswith((".html", ".htm")):
        url_slug += ".html"
    filename = os.path.join(dir, url_slug)
    with open(filename, "wt") as fh:
        fh.write(record["body"])
    logger.info(f"id {_id}: wrote {len(record['body'])} bytes to file {filename}")

