#!/usr/bin/python3

import json
import logging

from email.utils import parsedate_to_datetime
from scrapy import Field, Item  # type: ignore[import-untyped]
from scrapy.http import HtmlResponse  # type: ignore[import-untyped]


__all__ = ("SerializableItem",)


class SerializableItem(Item):  # type: ignore[misc]
    # define the fields for your item here like:
    # name = scrapy.Field()
    url: Field = Field()
    status: Field = Field()
    headers: Field = Field()
    body: Field = Field()
    encoding: Field = Field()
    date: Field = Field()

    # Factory method that draws upon an HtmlResponse object to initialize values.
    @classmethod
    def from_htmlresponse(cls, hresp: HtmlResponse) -> Item:
        logging.info(
            "SerializableItem.from_htmlresponse(): instancing item from "
            f"HtmlResponse for URL '{hresp.url}'"
        )
        this = cls()
        this["url"] = hresp.url
        this["status"] = hresp.status
        this["encoding"] = hresp.encoding
        # hresp.headers is a pseudo-dict where all the data are bytes objects,
        # and the values are lists not single objects.
        this["headers"] = {
            key.decode(hresp.encoding): [v.decode(hresp.encoding) for v in value]
            for key, value in hresp.headers.items()
        }
        # Storing the server date & time of the resource. For use with
        # If-Modified-Since if that becomes a suitable download strategy.
        this["date"] = (
            parsedate_to_datetime(hresp.headers["Date"].decode(hresp.encoding))
            if "Date" in hresp.headers
            else None
        )
        this["body"] = hresp.body.decode(hresp.encoding)
        return this

    def to_dict(self) -> dict[str, str | int]:
        logging.info(
            f"SerializableItem.to_dict(): converting item for URL {self['url']} to dict"
        )
        return {
            "url": self["url"],
            "status": self["status"],
            # This dict is intended for storage to the database. So JSON is used
            # to serialize the headers dict.
            "headers": json.dumps(self["headers"]),
            "body": self["body"],
            "encoding": self["encoding"],
            "date": self["date"],
        }
