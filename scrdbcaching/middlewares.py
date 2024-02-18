#!/usr/bin/python3

import json
import logging
import os
import time

from mysql.connector.cursor import MySQLCursor
from mysql.connector.pooling import MySQLConnectionPool, PooledMySQLConnection
from scrapy.crawler import Crawler  # type: ignore[import-untyped]
from scrapy.exceptions import CloseSpider  # type: ignore[import-untyped]
from scrapy.http import Request, HtmlResponse  # type: ignore[import-untyped]
from scrapy import Spider, signals  # type: ignore[import-untyped]
from typing import Any

from .spider import UrlSetSpider


__all__ = ("DatabaseCachingMiddleware", "RenderingRateLimitingMiddleware")


# downloader middleware that checks the database for a record corresponding to
# the URL before letting it get requested. If a record exists for that URL it's
# retrieved, an HtmlResponse object is deserialized from its attributes, and
# returned in place of a retrieved one. This is done to save API credits and
# time.
class DatabaseCachingMiddleware:
    db_user: str
    db_password: str
    db_host: str
    db_database: str
    db_charset: str
    credits_used: int
    credits_threshold: int
    concurrent_requests: int
    db_conx_pool: MySQLConnectionPool
    stop_triggered: bool

    def __init__(
        self,
        db_user: str,
        db_password: str,
        db_host: str,
        db_database: str,
        db_charset: str,
        concurrent_requests: int,
        credits_used: int,
        credits_threshold: int,
        crawler: Crawler,
    ) -> None:
        self.db_user = db_user
        self.db_password = db_password
        self.db_host = db_host
        self.db_database = db_database
        self.db_charset = db_charset
        self.concurrent_requests = concurrent_requests

        # These are two commandline arguments that are saved to the spider
        # object during its constructor, then retrieved from there by
        # from_crawler() and passed to *this* constructor.
        self.credits_used = int(credits_used)
        self.credits_threshold = int(credits_threshold)
        logging.info(
            f"DatabaseCachingMiddleware.__init__(): set credits_used={credits_used}, "
            f"credits_threshold={credits_threshold}"
        )
        self.crawler = crawler
        self.stop_triggered = False
        self.crawler.signals.connect(
            self.on_spider_closed, signal=signals.spider_closed
        )

        # Instancing the connection pool.
        pool_name = self.__class__.__name__
        logging.info(
            f"DatabaseCachingMiddleware.__init__(): opening database connection "
            f"pool '{pool_name}'"
        )
        self.db_conx_pool = MySQLConnectionPool(
            pool_name=pool_name,
            pool_size=self.concurrent_requests,
            pool_reset_session=True,
            user=self.db_user,
            password=self.db_password,
            host=self.db_host,
            database=self.db_database,
            charset=self.db_charset,
        )

    def on_spider_closed(self, spider: UrlSetSpider, reason: str) -> None:
        self.stop_triggered = True

    @classmethod
    def from_crawler(cls, crawler: Crawler) -> object:
        return cls(
            db_user=crawler.settings.get("MYSQL_USERNAME"),
            db_password=crawler.settings.get("MYSQL_PASSWORD"),
            db_host=crawler.settings.get("MYSQL_HOST"),
            db_database=crawler.settings.get("MYSQL_DATABASE"),
            db_charset=crawler.settings.get("MYSQL_CHARSET"),
            concurrent_requests=crawler.settings.get("CONCURRENT_REQUESTS"),
            # Passing these two values, which are commandline arguments
            # furnished to the spider constructor, to the middleware constructor
            # where they're needed.
            credits_used=crawler.spider.credits_used,
            credits_threshold=crawler.spider.credits_threshold,
            crawler=crawler,
        )

    def close_spider(self, spider: Spider) -> None:
        # Close the database connection
        if hasattr(self, "db_conx_pool"):
            delattr(self, "db_conx_pool")

    def process_request(
        self, request: Request, spider: UrlSetSpider
    ) -> HtmlResponse | None:
        # Getting a connection from the pool
        db_conn: PooledMySQLConnection = self.db_conx_pool.get_connection()
        db_cursor: MySQLCursor = db_conn.cursor()
        url: str = request.url
        # Check the database for the cached page
        try:
            # Checking the database for a record with this url. The columns of
            # the pages2 table correspond to the constructor arguments needed to
            # instance an HtmlResponse, plus 'date' for If-Modified-Since usage
            # where needed.
            db_cursor.execute(
                "SELECT url, status, encoding, headers, body FROM pages2 WHERE url = %(url)s;",
                dict(url=url),
            )
            row: tuple[Any, ...] = db_cursor.fetchone()  # type:ignore
            if row is not None:
                # A record was found, so an HtmlResponse object is deserialized
                # from its values and returned in place of having to retrieve
                # one over the network.
                logging.info(
                    f"DatabaseCachingMiddleware.process_request(): loaded record "
                    f"from database for URL '{url}'; deserializing HtmlResponse "
                    f"object"
                )
                # Converting the row to a dict.
                row_dict: dict[str, str | int] = dict(
                    zip(("url", "status", "encoding", "headers", "body"), row)
                )
                logging.info(
                    f"DatabaseCachingMiddleware.process_request(): adding to "
                    f"URLs deserialized set URL '{url}'"
                )
                spider.urls_deserialized.add(url)
                return HtmlResponse(
                    url=row_dict["url"],
                    status=row_dict["status"],
                    encoding=row_dict["encoding"],
                    # n.b. the value for `headers` is a dict serialized via
                    # JSON. HtmlResponse expects all its values to be bytes not
                    # strs so this is needed to cast everything back to bytes.
                    headers={
                        key.encode(row_dict["encoding"]): [
                            v.encode(row_dict["encoding"]) for v in value
                        ]
                        for key, value in json.loads(row_dict["headers"]).items()  # type: ignore[arg-type]
                    },
                    body=row_dict["body"].encode(row_dict["encoding"]),  # type: ignore[union-attr, arg-type]
                    # The associated Request object is an optional but
                    # nice-to-have for an HtmlResponse object. Obv. the original
                    # can't be used, nor is it worth de/serializing, but the one
                    # that's an argument to this method will do just as well.
                    request=request,
                )
            elif self.stop_triggered or self.credits_used >= self.credits_threshold:
                if self.credits_used >= self.credits_threshold:
                    # We've met or exceeded the number of API credits this execution
                    # is authorized to use. That's it, close it down, we're done
                    # here.
                    logging.critical(
                        "DatabaseCachingMiddleware.process_request(): credits "
                        f"used {self.credits_used} meets or exceeds threshold "
                        f"{self.credits_threshold}; SHUTTING DOWN SPIDER"
                    )
                    self.crawler.engine.close_spider(
                        spider,
                        f"API credits exhausted: {self.credits_used}/"
                        f"{self.credits_threshold}",
                    )
                else:
                    raise CloseSpider(
                        "DatabaseCachingMiddleware.process_request(): API "
                        "credits used has hit API credits threshold of "
                        f"{self.credits_threshold}; spider closing down..."
                    )
            else:
                # There's no record in the database for this URL and we still
                # have credits to spare, so retrieving can happen normally
                # (which is signalled by returning None).
                logging.info(
                    f"cache miss for URL '{url}', loading resource via network "
                    "as normal"
                )
                self.credits_used += 1
                logging.info(
                    f"incrementing credits_used value to {self.credits_used} "
                    f"(out of {self.credits_threshold})"
                )
                return None
        finally:
            # Ensuring the pool's connection is deallocated no matter what
            # happens.
            db_cursor.close()
            db_conn.close()


# Downloader middleware that activates if the render=True argument is being
# passed to ScraperAPI via the HTTPS_PROXY username argument. If so it
# rate-limits requests to ≤3 per second in conformance with ScraperAPI API
# limits.
class RenderingRateLimitingMiddleware:
    hr_min: tuple[int, ...]
    reqs_this_min: int

    def __init__(self) -> None:
        self.hr_min = time.localtime()[3:5]
        self.reqs_this_min = 0

    @classmethod
    def from_crawler(cls, crawler: Crawler) -> object:
        return cls()

    def process_request(self, request: Request, spider: Spider) -> None:
        if "render=True" not in os.environ["HTTPS_PROXY"]:
            # If "render=True" isn't being sent to ScraperAPI, this method
            # should be a no-op.
            return
        time_tuple1: tuple[int, ...] = time.localtime()
        if self.hr_min != time_tuple1[3:5]:
            # This object's state is stale. Update the hour-and-minute stamp and
            # reset the requests counter.
            self.hr_min = time_tuple1[3:5]
            self.reqs_this_min = 0
        elif self.reqs_this_min >= 3:
            # The state applies to this minute, and there's been 3+ connections
            # this minute. sleep() until next minute.
            time.sleep(60 - time_tuple1[5])
            # Update the hour-and-minute stamp and reset the requests counter.
            # Since it's a new minute a new call to time.localtime() is needed.
            self.hr_min = time.localtime()[3:5]
            self.reqs_this_min = 0
        else:
            # The state isn't stale and there's been less than 3 requests this
            # minute. No need to rate-limit but increment the requests counter.
            self.reqs_this_min += 1
        return None
