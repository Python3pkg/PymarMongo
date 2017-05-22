#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Example of using pymar with MongoDB
"""
from pymongo import MongoClient

from pymar.datasource import DataSourceFactory
from pymar.plugins.datasources.MongoDataSource import MongoDataSource
from pymar.producer import Producer


class WordsCountProducer(Producer):
    """Producer for the task word counting.
    """
    WORKERS_NUMBER = 4

    @staticmethod
    def map_fn(data_source):
        from collections import Counter
        for val in data_source:
            counter = Counter()
            for line in val["text"].splitlines():
                for word in line.split():
                    counter[word] += 1
            yield counter

    @staticmethod
    def reduce_fn(data_source):
        from collections import Counter
        final_count = Counter()
        for counter in data_source:
            final_count.update(counter)

        return final_count


class SimpleMongoSource(MongoDataSource):
    """Data source for the task of word counting.
    Illustrates work with MongoDataSource.
    You just have to provide the database configuration and name of the collection.

    Be careful: if you use "localhost" in configuration, your workers will access local database
    on their host, not on the host of producer! If it is not what you want, use external IP-address.
    """

    CONF = {
        "ip": "localhost",
        "port": 27017,
        "db": "test_db",
        "collection": "test_collection"
    }


def init_database():
    print("Initialize collection.")

    client = MongoClient(SimpleMongoSource.CONF["ip"], SimpleMongoSource.CONF["port"])
    db = client[SimpleMongoSource.CONF["db"]]
    data = db[SimpleMongoSource.CONF["collection"]]
    data.remove()

    books = [
        {
            "name": "first",
            "text": "one"
        },
        {
            "name": "second",
            "text": "one two"
        },
        {
            "name": "third",
            "text": "one two three"
        },
        {
            "name": "fourth",
            "text": "one two three four"
        },
        {
            "name": "fifth",
            "text": "one two three four five"
        },
        {
            "name": "sixth",
            "text": "one two three four five six"
        }
    ]

    data.insert(books)
    client.close()
    print("Collection initialized.")


def remove_database():
    print("Remove database")
    client = MongoClient(SimpleMongoSource.CONF["ip"], SimpleMongoSource.CONF["port"])
    db = client[SimpleMongoSource.CONF["db"]]
    db.drop_collection(SimpleMongoSource.CONF["collection"])
    print("Database removed.")

if __name__ == "__main__":
    """
    Before starting this script launch corresponding workers:
    worker.py ./pymarMongo/examples/words_count.py -s SimpleMongoSource -p WordsCountProducer -q 127.0.0.1 -w 4
    """
    init_database()

    producer = WordsCountProducer()
    factory = DataSourceFactory(SimpleMongoSource)
    value = producer.map(factory)

    print("\nAnswer: ")
    for word, count in value.most_common(6):
        print("\t%-5s : %s" % (word, count))

    remove_database()