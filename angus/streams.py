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

import json
import os
import urlparse
import uuid
import datetime
import pytz
import logging
import re

import tornado.gen
import tornado.httpclient
import tornado.web
from tornado.queues import Queue
from angus.analytics import report
import angus.framework
import jobs

LOGGER = logging.getLogger(__name__)

class Decoder(object):
    def __init__(self, uid, conf, queue, compute):
        self.data = ""
        self.uid = uid
        self.queue = queue
        self.index = 0
        self.compute = compute
        self.conf = conf
        self.boundary = "boundary"
        self.to_read = 0
        self.finish = False

    def _wait_header(self):
        finish = self.data.find("--{}--".format(self.boundary))
        if finish!=-1:
            self.finish = True
            self.queue.put(None)

        start = self.data.find("--{}\r\n".format(self.boundary))
        end = self.data.find("\r\n\r\n", start)
        if start!=-1 and end!=-1:
            header = self.data[start:end+4]
            content_length = re.search(r'Content-Length: (\w+)\r\n', header).group(1)
            self.data = self.data[end+6:]
            self.to_read = int(content_length)
            if len(self.data) > self.to_read:
                self.read_part()

    def _read_part(self):
        jpg = self.data[:self.to_read]
        self.data=self.data[self.to_read+1:]
        resource = dict()
        complete_data = self.conf.copy()
        complete_data['image'] = jobs.Resource(content=jpg)
        self.compute(resource, complete_data)
        self.queue.put(resource)
        self.to_read = 0

    def __call__(self, chunk):
        LOGGER.info(".")
        self.data = self.data + chunk
        if self.to_read == 0:
            self._wait_header()
        elif len(self.data) > self.to_read:
            self._read_part()

class Streams(tornado.web.RequestHandler):
    def initialize(self, *args, **kwargs):
        self.service_key = kwargs.pop('service_key')
        self.service_version = kwargs.pop('version')
        self.resource_storage = kwargs.pop('resource_storage')
        self.compute = kwargs.pop('compute')
        self.streams = kwargs.pop('streams')

    def post(self):
        stream_id = unicode(uuid.uuid1())

        public_url = "%s://%s" % (self.request.protocol, self.request.host)

        service_url = "{}/services/{}/{}".format(public_url,
                                                 self.service_key,
                                                 self.service_version)

        response = {
            'url': "{}/streams/{}".format(service_url, stream_id),
            'uuid': stream_id,
        }

        # Two possibility, application/json or multipart/mixed
        content_type = self.request.headers.get('Content-Type')
        if "application/json" in content_type:
            data = json.loads(self.request.body)
        elif content_type is None:
            raise Exception("You must specified a content-type")
        else:
            raise Exception("Unknown content-type: %s" % (content_type))

        self.streams[stream_id] = Decoder(stream_id, data, Queue(), self.compute)

        response["input"] = "{}/streams/{}/input".format(service_url, stream_id)
        response["output"] = "{}/streams/{}/output".format(service_url, stream_id)

        self.write(response)

        self.finish()

class Stream(tornado.web.RequestHandler):
    def initialize(self, *args, **kwargs):
        pass

    def get(self, uid):
        self.write("ok")

class Output(tornado.web.RequestHandler):

    def initialize(self, *args, **kwargs):
        self.streams = kwargs.pop('streams')

    @tornado.gen.coroutine
    def get(self, uid):
        decoder = self.streams.get(uid, None)
        if decoder is None:
            self.set_status(404)
            self.finish()
            return

        while True:
            response = yield decoder.queue.get()
            if response is None:
                self.finish("\r\n")
                break
            self.write(response)
            self.write("\r\n")
            yield self.flush()

@tornado.web.stream_request_body
class Input(tornado.web.RequestHandler):

    def initialize(self, *args, **kwargs):
        self.streams = kwargs.pop('streams')

    def prepare(self):
        self.uid = self.path_args[0]

        content_type = self.request.headers.get('Content-Type')
        boundary= re.search(r'boundary=(\w+)', content_type).group(1)
        self.decoder = self.streams.get(self.uid, None)
        self.decoder.boundary = boundary

    def data_received(self, data):
        if self.decoder:
            self.decoder(data)

    def post(self, uid):
        self.delete()

    def on_connection_close(self):
        self.delete()

    def delete(self):
        del self.streams[self.uid]
