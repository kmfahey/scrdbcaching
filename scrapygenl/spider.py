#!/usr/bin/python3

import scrapy  # type: ignore[import-untyped]


__all__ = "UrlSetSpider",

class UrlSetSpider(scrapy.Spider):  # type: ignore[misc]
    urls_deserialized: set[str]

    def __init__(  # type: ignore[no-untyped-def]
        self,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.urls_deserialized = set()

