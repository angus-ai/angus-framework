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

MARK_END = "\r\n"

class Decoder(object):
    def __init__(self, uid, conf, queue, compute):
        self.data = ""
        self.uid = uid
        self.queue = queue
        self.index = 0
        self.compute = compute
        self.conf = conf
        self.boundary = "boundary"

    @tornado.gen.coroutine
    def _wait_header(self):
        finish = self.data.find("--{}--".format(self.boundary))
        if finish == 0:
            yield self.queue.put(None)
            return

        start = self.data.find("--{}\r\n".format(self.boundary))
        end = self.data.find(MARK_END+MARK_END, start)
        if start!=-1 and end!=-1:
            header = self.data[start:end+len(MARK_END+MARK_END)]

            content_length = re.search(r'Content-Length: (.+)\r\n', header)
            if content_length is not None:
                content_length = int(content_length.group(1))
            else:
                content_length = 0

            field = re.search(r'X-Angus-DataField: (.+)\r\n', header)
            if field is not None:
                field = field.group(1)
            else:
                field = 'image'

            parameters = re.search(r'X-Angus-Parameters: (.+)\r\n', header)
            LOGGER.debug(header)
            if parameters is not None:
                parameters = json.loads(parameters.group(1))
            else:
                parameters = dict()

            buff = self.data[end+len(MARK_END+MARK_END):]

            if len(self.data) > content_length:
                self.data = buff
                yield self._read_part(content_length, field, parameters)
                yield self._wait_header()

    @tornado.gen.coroutine
    def _read_part(self, to_read, field, parameters):
        jpg = self.data[:to_read]
        self.data=self.data[to_read+len(MARK_END):]
        resource = dict()
        complete_data = self.conf.copy()
        complete_data.update(parameters)
        complete_data[field] = jobs.Resource(content=jpg)
        yield self.compute(resource, complete_data)
        yield self.queue.put(resource)

    @tornado.gen.coroutine
    def __call__(self, chunk):
        self.data = self.data + chunk
        yield self._wait_header()



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
        self.up = True

    @tornado.gen.coroutine
    def get(self, uid):
        decoder = self.streams.get(uid, None)
        if decoder is None:
            self.set_status(404)
            self.finish()
            return

        while self.up:
            response = yield decoder.queue.get()
            if response is None:
                self.finish("--myboundary--")
                break
            response = json.dumps(response)
            response = "\r\n".join(("--myboundary",
                                "Content-Type: application/json",
                                "Content-Length: " + str(len(response)),
                                "",
                                response,
                                ""))
            self.write(response)
            yield self.flush()

    def on_connection_close(self):
        self.up = False

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

    @tornado.gen.coroutine
    def data_received(self, data):
        if self.decoder:
            yield self.decoder(data)
