#!/usr/bin/python3

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

import logging

from mysql.connector.cursor import MySQLCursor
from mysql.connector.pooling import MySQLConnectionPool, PooledMySQLConnection

from .items import SerializableItem
from .spider import UrlSetSpider


__all__ = "SerializingDatabasePipeline",


# Pipeline to save every page, in its entirety, to the database
class SerializingDatabasePipeline:
    db_conx_pool: MySQLConnectionPool

    def open_spider(self, spider: UrlSetSpider) -> None:
        # Opening the MySQL connection pool.
        pool_name = self.__class__.__name__
        logging.info(
            f"ToSqlDatabasePipeline.open_spider(): opening database connection "
            f"pool '{pool_name}'"
        )
        self.db_conx_pool = MySQLConnectionPool(
            pool_name=pool_name,
            pool_size=spider.settings.get("CONCURRENT_REQUESTS"),
            pool_reset_session=True,
            user=spider.settings.get("MYSQL_USERNAME"),
            password=spider.settings.get("MYSQL_PASSWORD"),
            host=spider.settings.get("MYSQL_HOST"),
            database=spider.settings.get("MYSQL_DATABASE"),
            charset=spider.settings.get("MYSQL_CHARSET"),
        )

    # Saves an item to the database.
    def process_item(
        self, item: SerializableItem, spider: UrlSetSpider
    ) -> None:
        # Getting a connection from the pool.
        if item["url"] in spider.urls_deserialized:
            logging.info(
                "ToSqlDatabasePipeline.process_item(): skipping bc URLs "
                f"deserialized set already contains URL {item['url']}"
            )
            return
        db_conn: PooledMySQLConnection = self.db_conx_pool.get_connection()
        db_cursor: MySQLCursor = db_conn.cursor()
        try:
            logging.info(
                "ToSqlDatabasePipeline.process_item(): storing item for URL "
                f"{item['url']} to database"
            )
            # Inserting the item into the database.
            db_cursor.execute(
                """
                INSERT INTO pages2 (
                    `url`, `status`, `encoding`, `headers`, `date`, `body`
                ) VALUES (
                    %(url)s, %(status)s, %(encoding)s, %(headers)s, %(date)s, %(body)s
                ); """,
                # This method converts item to a dict which can be used to
                # populate this row with all of its values.
                item.to_dict(),  # type: ignore[arg-type]
            )
            db_conn.commit()
        except Exception as exception:
            # Logging the exception
            logging.info(
                "ToSqlDatabasePipeline.process_item(): storing "
                f"item for url {item['url']} threw an exception "
                f"{exception.__class__.__name__}: {str(exception)}"
            )
            # Ensuring the rollback happens
            db_conn.rollback()
            # Re-raising the exception
            raise exception
        finally:
            # Ensuring the connection resource is de-allocated no matter what
            # happens.
            db_cursor.close()
            db_conn.close()

    def close_spider(self, spider: UrlSetSpider) -> None:
        # Not really sure if this is the best way to close the connection pool
        # but there don't seem to be better ones.
        del self.db_conx_pool
