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

"""Enable use of Google Analytics for API tracking (experimental)

This module provides some tools to track tornado endpoint calls
with Google analytics.

It try to get back an environment variable GOOGLE_TID to send data
to Google.
"""

import logging
import os
from urllib import urlencode

from tornado.netutil import Resolver

import angus
import tornado.httpclient as hclient


LOGGER = logging.getLogger('analytics')

Resolver.configure('tornado.netutil.ThreadedResolver',
                   num_threads=10)

GOOGLE_TID = os.environ.get('GOOGLE_TID', None)
if GOOGLE_TID and os.path.exists(GOOGLE_TID):
    with open(GOOGLE_TID, 'r') as f:
        GOOGLE_TID = f.readline().strip()


def complexe_report(tid=GOOGLE_TID, client=hclient.AsyncHTTPClient()):
    """
    Define an annotation for tornado handler operations that send
    notification to Google Analytics.
    """
    def decorator(func):
        """
        Decorator for method annotation
        """

        def wrapper(self, *args, **kwargs):
            """
            Option for the decorator
            """
            try:
                user = angus.framework.extract_user(self)
                path = self.request.path
                host = self.request.host
                params = {
                    "v": 1,
                    "dp": path,
                    "dl": host,
                    "tid": tid,
                    "cid": user,
                    "t": "pageview",
                }
                params = urlencode(params)
                req = hclient.HTTPRequest(
                    "http://www.google-analytics.com/collect?%s" %
                    (params))
                if tid:
                    client.fetch(req)
            except IOError as io_exception:
                LOGGER.exception(io_exception)
            except Exception as other_exception:
                LOGGER.exception(other_exception)
            return func(self, *args, **kwargs)
        return wrapper
    return decorator


def report(func):
    """
    If you set GOOGLE_TID environement variable, this
    is the default annotation to tag tornado handler operations.
    """
    return complexe_report()(func)
