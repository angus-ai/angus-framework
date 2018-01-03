#!/usr/bin/env python
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

""" A helper to create a tornado service.
"""

import json
import logging

import concurrent.futures
import tornado.web

import angus.jobs
import angus.streams

class FakeGatewayRoot(tornado.web.RequestHandler):
    def initialize(self, *args, **kwargs):
        self.service_key = kwargs.pop('service_key')

    def get(self):
        self.write({
            "services": {
                self.service_key: {
                    "url": "/services/{}".format(self.service_key)
                }
            }
        })

class FakeGatewayService(tornado.web.RequestHandler):
    def initialize(self, *args, **kwargs):
        self.service_key = kwargs.pop('service_key')
        self.version = kwargs.pop('version')

    def get(self):
        self.write({
            "versions": {
                "1": {"url": "/services/{}/{}".format(self.service_key, self.version)}
            }
        })

class Description(tornado.web.RequestHandler):
    """ Every services have a description endpoint.
    """

    def initialize(self, *args, **kwargs):
        self.description = kwargs.pop('description')
        self.version = kwargs.pop('version')
        self.service_key = kwargs.pop('service_key')

    def get(self):
        public_url = "%s://%s" % (self.request.protocol, self.request.host)
        result = {
            "description": self.description,
            "version": self.version,
            "url": "%s/sevices/%s/%s" % (public_url,
                                         self.service_key,
                                         self.version),
        }

        self.write(json.dumps(result))

def wrap_computer(compute, threads):
    if threads == 0:
        return tornado.gen.coroutine(compute)

    executor = concurrent.futures.ThreadPoolExecutor(threads)
    @tornado.gen.coroutine
    def wrap_compute(*args, **kwargs):
        yield executor.submit(compute, *args, **kwargs)

    return wrap_compute

class Service(tornado.web.Application):
    """ Start a tornado server and configure it to run an angus
    service.
    """

    def __init__(self, service_key, version,
                 port,
                 compute,
                 resource_storage=None, threads=4,
                 description="No description"):

        self.logger = logging.getLogger(service_key)

        self.port = port

        self.queues = dict() # TODO: use celery

        conf = {
            'service_key': service_key,
            'resource_storage': resource_storage,
            'version': version,
            'compute': wrap_computer(compute, threads),
            'description': description,
            'streams': self.queues,
        }

        basename = "/services/{}/{}".format(service_key, version)

        super(Service, self).__init__([
            (r"/services", FakeGatewayRoot, conf),
            (r"/services/{}".format(service_key), FakeGatewayService, conf),
            (r"{}/jobs/(.*)".format(basename), angus.jobs.Job, conf),
            (r"{}/jobs".format(basename), angus.jobs.JobCollection, conf),
            (r"{}/streams/(.*)/input".format(basename), angus.streams.Input, conf),
            (r"{}/streams/(.*)/output".format(basename), angus.streams.Output, conf),
            (r"{}/streams/(.*)".format(basename), angus.streams.Stream, conf),
            (r"{}/streams".format(basename), angus.streams.Streams, conf),

            (r"{}".format(basename), Description, conf),

        ])

    def start(self):
        """ Run the service
        """
        self.listen(self.port, xheaders=True)
        tornado.ioloop.IOLoop.instance().start()
