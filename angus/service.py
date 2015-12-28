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

from angus.analytics import report
import angus.jobs


class Description(tornado.web.RequestHandler):
    """ Every services have a description endpoint.
    """

    def initialize(self, *args, **kwargs):
        self.description = kwargs.pop('description')
        self.version = kwargs.pop('version')
        self.service_key = kwargs.pop('service_key')

    @report
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

        @tornado.gen.coroutine
        def comp_thread(resource, data,
                        executor=concurrent.futures.ThreadPoolExecutor(threads)):
            """ This method compute job when we used many threads.
            """
            yield executor.submit(compute, resource, data)

        @tornado.gen.coroutine
        def comp_solo(resource, data):
            """ This method compute a job when multithreading is off
            """
            compute(resource, data)

        if threads == 0:
            comp = comp_solo
        else:
            comp = comp_thread

        conf = {
            'service_key': service_key,
            'resource_storage': resource_storage,
            'compute': comp,
            'version': version,
            'description': description,
        }

        super(Service, self).__init__([
            (r"/services/%s/%s/jobs" %
             (service_key, version), angus.jobs.JobCollection, conf),
            (r"/services/%s/%s/jobs/(.*)" %
             (service_key, version), angus.jobs.Job, conf),
            (r"/services/%s/%s" % (service_key,
                                   version), Description, conf),
        ])

    def start(self):
        """ Run the service
        """
        self.listen(self.port, xheaders=True)
        tornado.ioloop.IOLoop.instance().start()
