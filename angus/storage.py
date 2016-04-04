# -*- coding: utf-8 -*-

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

""" Resource storage
"""

import collections
import memcache


class ResourceStorage(object):

    def __init__(self):
        raise Exception("Not implemented")

    def get(self, key, auth=None):
        raise Exception("Not implemented")

    def set(self, key, value, owner=None, mode=None, ts=None):
        raise Exception("Not implemented")


class MemoryStorage(dict):

    def __init__(self):
        super(MemoryStorage, self).__init__(self)

    def set(self, key, value, owner=None, mode=None, ts=None):
        self[key] = (value, ts, owner, mode)

    def get(self, key, auth=None):
        result = self.get(key)
        if result is not None and auth == result[2]:
            return result[0]
        else:
            return None


class LimitedMemoryStorage(ResourceStorage):
    """ Storage class for blobs
    """

    def __init__(self, max_size=10):
        self.max_size = max_size
        self.inner = collections.OrderedDict()

    def set(self, key, value, owner=None, mode=None, ts=None):
        """ Store a new blob
        """
        self.inner[key] = (value, ts, owner, mode)
        if len(self.inner) > self.max_size:
            self.inner.popitem(last=False)

    def get(self, key, auth=None):
        """ Get back a blob
        """
        result = self.inner.get(key)
        if result is not None and auth == result[2]:
            return result[0]
        else:
            return None

    def iteritems(self):
        """ Iterator over content
        """
        return self.inner.iteritems()


class MemcacheStorage(ResourceStorage):

    def __init__(self, servers=["127.0.0.1"]):
        self.client = memcache.Client(servers)

    def set(self, key, value, owner=None, mode=None, ts=None):
        self.client.set(key, (value, ts, owner, mode))

    def get(self, key, auth=None):
        result = self.client.get(key)
        if result is not None and auth == result[2]:
            return result[0]
        else:
            return None
