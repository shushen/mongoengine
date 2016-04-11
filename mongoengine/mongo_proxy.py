# Originally from Openstack Celiometer project
#    https://git.openstack.org/openstack/ceilometer
# Modified for use with mongoengine by Shu Shen <sshen@siaras.com>
# Modifications Copyright Siaras Research Canada, Inc 2016
#
# Copyright Ericsson AB 2013. All rights reserved
#
# Authors: Ildiko Vancsa <ildiko.vancsa@ericsson.com>
#          Balazs Gibizer <balazs.gibizer@ericsson.com>
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
import functools
import time

import pymongo
import six


def safe_mongo_call(call):
    @functools.wraps(call)
    def closure(*args, **kwargs):
        max_retries = 10
        retry_interval = 10
        attempts = 0
        while True:
            try:
                return call(*args, **kwargs)
            except pymongo.errors.AutoReconnect:
                if max_retries <= attempts:
                    raise
                attempts += 1
                time.sleep(retry_interval)
    return closure


class MongoConn(object):
    def __init__(self, method):
        self.method = method

    @safe_mongo_call
    def __call__(self, *args, **kwargs):
        return self.method(*args, **kwargs)


MONGO_METHODS = set(typ for typ in dir(pymongo.collection.Collection)
                    if not typ.startswith('_'))
MONGO_METHODS.update(set(typ for typ in dir(pymongo.MongoClient)
                         if not typ.startswith('_')))
MONGO_METHODS.update(set(typ for typ in dir(pymongo)
                         if not typ.startswith('_')))


class MongoProxy(object):
    def __init__(self, conn):
        self.conn = conn

    def __getitem__(self, item):
        """Create and return proxy around the method in the connection.

        :param item: name of the connection
        """
        return MongoProxy(self.conn[item])

    def find(self, *args, **kwargs):
        # We need this modifying method to return a CursorProxy object so that
        # we can handle the Cursor next function to catch the AutoReconnect
        # exception.
        return CursorProxy(self.conn.find(*args, **kwargs))

    def __getattr__(self, item):
        """Wrap MongoDB connection.

        If item is the name of an executable method, for example find or
        insert, wrap this method in the MongoConn.
        Else wrap getting attribute with MongoProxy.
        """
        real_item = getattr(self.conn, item)
        if item in ('name', 'database'):
            return real_item
        if item == 'connection':
            return MongoProxy(real_item)
        if item in MONGO_METHODS and six.callable(real_item):
            return MongoConn(real_item)
        return real_item

    def __call__(self, *args, **kwargs):
        return self.conn(*args, **kwargs)


def safe_cursor_call(call):
    @functools.wraps(call)
    def keep_cursor(self, *args, **kwargs):
        save_cursor = self.cursor.clone()
        try:
            return call(self, *args, **kwargs)
        except pymongo.errors.AutoReconnect:
            self.cursor = save_cursor
            raise
    return keep_cursor


class CursorProxy(pymongo.cursor.Cursor):
    def __init__(self, cursor):
        self.cursor = cursor

    @safe_mongo_call
    @safe_cursor_call
    def __getitem__(self, item):
        """Wrap Cursor __getitem__ method.

        This method will be executed before each Cursor __getitem__ method
        call.
        """
        return self.cursor[item]

    @safe_mongo_call
    @safe_cursor_call
    def next(self):
        """Wrap Cursor next method.

        This method will be executed before each Cursor next method call.
        """
        return self.cursor.next()

    def clone(self):
        """Get a clone of this cursor.

        Returns a new Cursor instance with options matching those that have
        been set on the current instance. The clone will be completely
        unevaluated, even if the current instance has been partially or
        completely evaluated.
        """
        cursor_clone = self.cursor.clone()
        return CursorProxy(cursor_clone)

    def __getattr__(self, item):
        return getattr(self.cursor, item)
