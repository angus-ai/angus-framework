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

import logging
import collections
import memcache
import boto3
import json
import threading
import Queue
import StringIO

__updated__ = "2016-12-15"
__author__ = "Aurélien Moreau"
__copyright__ = "Copyright 2015-2016, Angus.ai"
__credits__ = ["Aurélien Moreau", "Gwennael Gate"]
__license__ = "Apache v2.0"
__maintainer__ = "Aurélien Moreau"
__status__ = "Production"

LOGGER = logging.getLogger(__name__)

class ResourceStorage(object):

    def __init__(self):
        raise Exception("Not implemented")

    def get(self, key, auth=None):
        raise Exception("Not implemented")

    def safe_get(self, key):
        raise Exception("Not implemented")

    def set(self, key, value, owner=None, mode=None, ts=None, ttl=None):
        raise Exception("Not implemented")

    def update(self, key, value):
        raise Exception("Not implemented")

    def flush(self, key):
        raise Exception("Not implemented")

class MemoryStorage(dict):

    def __init__(self):
        super(MemoryStorage, self).__init__(self)

    def set(self, key, value, owner=None, mode=None, ts=None, ttl=None):
        self[key] = (value, ts, owner, mode, ttl)

    def update(self, key, value):
        if key in self:
            _, ts, owner, mode, ttl = self[key]
            self.set(key, value, owner, mode, ts, ttl)

    def get(self, key, auth=None):
        result = self.safe_get(key)
        if result is not None and auth == result[2]:
            return result[0]
        else:
            return None

    def safe_get(self, key):
        return super(MemoryStorage, self).get(key)

    def flush(self, key):
        return

class LimitedMemoryStorage(ResourceStorage):
    """ Storage class for blobs
    """

    def __init__(self, max_size=10):
        self.max_size = max_size
        self.inner = collections.OrderedDict()

    def set(self, key, value, owner=None, mode=None, ts=None, ttl=None):
        """ Store a new blob
        """
        self.inner[key] = (value, ts, owner, mode, ttl)
        if len(self.inner) > self.max_size:
            self.inner.popitem(last=False)

    def update(self, key, value):
        if key in self.inner:
            _, ts, owner, mode, ttl = self.inner[key]
            self.set(key, value, owner, mode, ts, ttl)

    def safe_get(self, key):
        return self.inner.get(key)

    def get(self, key, auth=None):
        """ Get back a blob
        """
        result = self.safe_get(key)
        if result is not None and auth == result[2]:
            return result[0]
        else:
            return None

    def iteritems(self):
        """ Iterator over content
        """
        return self.inner.iteritems()

    def flush(self, key):
        return

class MemcacheStorage(ResourceStorage):

    def __init__(self, servers=["127.0.0.1"]):
        self.client = memcache.Client(servers)

    def set(self, key, value, owner=None, mode=None, ts=None, ttl=None):
        self.client.set(key, (value, ts, owner, mode, ttl))

    def update(self, key, value):
        _, ts, owner, mode, ttl = self.client.get(key)
        self.set(key, value, owner, mode, ts, ttl)

    def safe_get(self, key):
        return self.client.get(key)

    def get(self, key, auth=None):
        result = self.safe_get(key)
        if result is not None and auth == result[2]:
            return result[0]
        else:
            return None

    def flush(self, key):
        return

class S3Putter(threading.Thread):
    def __init__(self, bucket_name=None,
                 access_key_id=None,
                 region_name=None,
                 secret_access_key=None,
                 input_queue=None):
        super(S3Putter, self).__init__()
        self.input_queue = input_queue
        self.access_key_id = access_key_id
        self.region_name = region_name
        self.secret_access_key= secret_access_key
        self.bucket_name = bucket_name

    def run(self):
        session = boto3.session.Session(aws_access_key_id=self.access_key_id,
                                       aws_secret_access_key=self.secret_access_key,
                                       region_name=self.region_name)
        s3 = session.resource('s3')
        bucket = s3.Bucket(self.bucket_name)
        while True:
            LOGGER.info(self.input_queue.qsize())
            key, body, metadata = self.input_queue.get()
            bucket.put_object(Key=key, Body=body, Metadata=metadata)


class S3Storage(ResourceStorage):

    def __init__(self, bucket_name=None,
                 access_key_id=None,
                 region_name=None,
                 secret_access_key=None):

        session = boto3.session.Session(aws_access_key_id=access_key_id,
                                       aws_secret_access_key=secret_access_key,
                                       region_name=region_name)
        s3 = session.resource('s3')
        self.bucket = s3.Bucket(bucket_name)
        self.queue = Queue.Queue()
        self.putter = S3Putter(bucket_name, access_key_id, region_name, secret_access_key, self.queue)
        self.putter.start()

    def set(self, key, value, owner="", mode="", ts=None, ttl=None):
        if mode is None:
            mode = ""
        metadata = {
            'owner':owner,
            'mode':mode,
            'ts': ts.isoformat(),
            'ttl': str(ttl)
        }
        self.queue.put((key, json.dumps(value), metadata))

    def update(self, key, value):
        o = self.safe_get(key)
        self.set(key, value, o.metadata.owner, o.metadata.mode, o.metadata.ts, o.metedata.ttl)

    def safe_get(self, key):
        return self.bucket.Object(key)

    def get(self, key, auth=None):
        obj = self.safe_get(self)
        if obj.metadata.auth == auth:
            buff = StringIO.StringIO()
            obj.download_fileobj(buff)
            return json.loads(buff.getvalue())
        else:
            return None

    def flush(self, key):
        return

class CompleteStorage(object):

    def __init__(self,
                 bucket_name,
                 region_name,
                 access_key_id,
                 secret_access_key,
                 max_size=10):
        self.mem = LimitedMemoryStorage(max_size=max_size)
        self.shared = MemcacheStorage()
        self.s3 = S3Storage(bucket_name=bucket_name,
                            region_name=region_name,
                            access_key_id=access_key_id,
                            secret_access_key=secret_access_key)

    def get(self, key, auth=None):
        resource = self.mem.get(key, auth=auth)
        if resource is None:
            resource = self.shared.get(key, auth=auth)

        if resource is None:
            resource = self.s3.get(key, auth=auth)

        return resource

    def set(self, *args, **kwargs):
        self.mem.set(*args, **kwargs)

    def update(self, *args, **kwargs):
        self.mem.update(*args, **kwargs)

    def flush(self, key):
        obj = self.mem.safe_get(key)
        ttl = obj[4]
        if ttl >= 0:
            self.shared.set(key, obj[0], owner=obj[2], mode=obj[3], ts=obj[1], ttl=obj[4])
        if ttl > 0:
            self.s3.set(key, obj[0], owner=obj[2], mode=obj[3], ts=obj[1], ttl=obj[4])
