#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pymongo import MongoClient

from pymar.datasource import DataSource


class MongoDataSource(DataSource):
    """Data source based on a MongoBD collection.
    To use, inherit from this class and set CONF.

    Be careful: if you use "localhost" in configuration, your workers will access local database
    on their host, not on the host of producer! If it is not what you want, use external IP-address.
    """

    #Redefine in subclass
    CONF = {
        "ip": "localhost",
        "port": 27017,
        "db": "test_db",
        "collection": "test_collection"
    }

    def __init__(self, **kwargs):
        DataSource.__init__(self, **kwargs)
        self.data = self.read_data()

    def __iter__(self):
        return self.data.find(skip=self.offset, limit=self.limit)

    @classmethod
    def full_length(cls):
        data = cls.read_data()
        return data.count()

    @classmethod
    def read_data(cls):
        client = MongoClient(cls.CONF["ip"], cls.CONF["port"])
        db = client[cls.CONF["db"]]
        return db[cls.CONF["collection"]]