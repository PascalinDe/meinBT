#    meinBT
#    Copyright (C) 2017  Carine Dengler
#
#    This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation, either version 3 of the License, or
#    (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses/>.


"""
:synopsis: mongoDB interface.
"""


# standard library imports
import sys
import logging
import multiprocessing

# third party imports
import pymongo

# library specific imports


def _get_logger(suffix):
    """Get multi-threaded logger.

    :param str suffix: suffix

    :returns: logger
    :rtype: Logger
    """
    try:
        logger = multiprocessing.get_logger().getChild(suffix)
        logger.setLevel(logging.INFO)
        logger.addHandler(logging.StreamHandler(stream=sys.stdout))
    except Exception:
        raise
    return logger


class MBTMongo(object):
    """mongoDB interface.

    :ivar int pid: process ID
    :ivar str host: host
    :ivar str port: port
    :ivar str db: mongoDB
    :ivar dict collections: collections
    :ivar MongoClient client: client
    """

    def __init__(self, pid, mongodb):
        """Connect to mongoDB.

        :param int pid: process ID
        :param ConfigParser mongodb: mongoDB configuration
        """
        try:
            logger = _get_logger(__name__)
            self.pid = pid
            self.host = mongodb["server"]["host"]
            self.port = mongodb["server"].getint("port")
            self.db = mongodb["server"]["db"]
            logger.info(
                "worker %d connects to mongoDB %s (%s:%d)",
                self.pid, self.db, self.host, self.port
            )
            self.client = pymongo.MongoClient(host=self.host, port=self.port)
        except pymongo.errors.PyMongoError:
            logger.exception(
                "worker %d failed to connect to mongoDB %s (%s:%d)",
                self.pid, self.db, self.host, self.port
            )
            raise
        return

    def insert(self, record, collection):
        """Insert record.

        :param dict record: record
        :param str collection: collection

        :returns: inserted ID
        :rtype: ObjectId
        """
        try:
            logger = _get_logger(__name__)
            inserted_id = self.client[self.db][collection].insert(record)
        except pymongo.errors.PyMongoError:
            logger.exception("worker %d failed to insert document", self.pid)
            raise
        return inserted_id

    def insert_many(self, records, collection):
        """Insert records.

        :param list records: records
        :param str collection: collection
        """
        try:
            logger = _get_logger(__name__)
            self.client[self.db][collection].insert_many(records)
        except pymongo.errors.PyMongoError:
            logger.exception("worker %d failed to insert documents", self.pid)
            raise
        return

    def aggregate(self, collection, pipeline):
        """Perform an aggregation.

        :param str collection: collection
        :param dict query: query

        :returns: records
        :rtype: Cursor
        """
        try:
            logger = _get_logger(__name__)
            records = self.client[self.db][collection].aggregate(pipeline)
        except pymongo.errors.PyMongoError:
            logger.exception(
                "worker %d failed to perform an aggregation", self.pid
            )
            raise
        return records

    def find(self, collection, query):
        """Find documents.

        :param str collection: collection
        :param dict query: query

        :returns: records
        :rtype: Cursor
        """
        try:
            logger = _get_logger(__name__)
            records = self.client[self.db][collection].find(query)
        except pymongo.errors.PyMongoError:
            logger.info("worker %d failed to find documents", self.pid)
            raise
        return records

    def close(self):
        """Close connection to mongoDB."""
        logger = _get_logger(__name__)
        logger.info(
            "worker %d closed connection to mongoDB %s (%s:%d)",
            self.pid, self.db, self.host, self.port
        )
        self.client.close()
        return
