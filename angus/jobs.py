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

"""Main piece of the angus webservice framework.

Define Resource, Job, JobCollection to implement a RESTful
server with python tornado.
"""

import json
import os
import urlparse
import uuid

import tornado.gen
import tornado.httpclient
import tornado.web

from angus.analytics import report


class Resource(object):
    """ A could be a local path, an url, or a binary content
    """

    def __init__(self, path=None, url=None, content=None):
        self._path = path
        self.url = url
        self.content = content

    @property
    def path(self):
        """ Local path of the resource if exists.
        """
        if self._path is None:
            raise Exception("You must resolve the resource before use it")
        return self._path

    @tornado.gen.coroutine
    def resolve(self, auth):
        """ Resolve the resource
        """
        if self._path is None:
            self._path = "/tmp/%s" % (uuid.uuid4())
            if self.content is None:
                assert self.url is not None
                client = tornado.httpclient.AsyncHTTPClient()
                headers = {
                    'Authorization': auth
                }
                # TODO: Must validate cert after, quick fix
                req = yield client.fetch(self.url, headers=headers,
                                         validate_cert=False)
                self.content = req.body

            with open(self._path, 'wb') as tmp_file:
                tmp_file.write(self.content)

    def __del__(self):
        os.remove(self._path)


class JobCollection(tornado.web.RequestHandler):
    """ JobCollection is a set of job, it enables creation of
    new job and resolve resource before run it.
    """

    def initialize(self, *args, **kwargs):
        self.service_key = kwargs.pop('service_key')
        self.service_version = kwargs.pop('version')
        self.resource_storage = kwargs.pop('resource_storage')
        self.compute = kwargs.pop('compute')

    @tornado.gen.coroutine
    @report
    def post(self):
        new_job_id = unicode(uuid.uuid1())

        public_url = "%s://%s" % (self.request.protocol, self.request.host)

        response = {
            'url': "%s/services/%s/%s/jobs/%s" % (public_url,
                                                  self.service_key,
                                                  self.service_version,
                                                  new_job_id),
        }

        self.resource_storage[new_job_id] = response

        # Two possibility, application/json or multipart/mixed
        content_type = self.request.headers.get('Content-Type')
        if "application/json" in content_type:
            data = json.loads(self.request.body)
        elif content_type is None:
            raise Exception("You must specified a content-type")
        elif content_type.startswith("multipart/form-data"):
            data = json.loads(self.get_body_argument('meta'))
        else:
            raise Exception("Unknown content-type: %s" % (content_type))

        # By default a request is asynchronous

        if 'async' in data and not data['async']:
            status = 201
            yield self._compute_result(response, data)
            reason = "New job was finished."
        else:
            status = 202
            self._compute_result(response, data)
            reason = "New job was accepted, keep in touch."

        response['status'] = status
        self.set_status(status, reason)
        self.write(json.dumps(response))

    def replace(self, obj):
        """ Find resource in request message and convert it
        into a Resource object.
        """
        if isinstance(obj, dict):
            return dict([(k, self.replace(e)) for k, e in obj.iteritems()])
        elif isinstance(obj, list):
            return [self.replace(e) for e in obj]
        if isinstance(obj, unicode):
            return self.resource(obj)
        else:
            return obj

    def resource(self, resource):
        """ If a string is a resource, starts with http:// or
        file:// or attachment://, convert it, if not, just return
        the string.
        """
        file_path = None
        res_url = urlparse.urlparse(resource)
        file_content = None

        if res_url.scheme == "http" or res_url.scheme == "https":
            # This is a resource, we must download it
            res_url = res_url.geturl()
        elif res_url.scheme == "file":
            file_path = res_url.path
        elif res_url.scheme == "attachment":
            # This is a files embeded in the request
            if res_url.geturl() in self.request.files:
                file_content = self.request.files[
                    res_url.geturl()][0]['body']
            else:
                file_path = self.get_body_argument(res_url.geturl())
                file_path = urlparse.urlparse(file_path)
                assert file_path.scheme == "file"
                file_path = file_path.path
        else:
            return resource

        result = Resource(file_path, res_url, file_content)
        return result

    @tornado.gen.coroutine
    def resolve(self, obj, auth=None):
        """ Resolve resources.
        """
        if isinstance(obj, dict):
            for elem in obj.values():
                yield self.resolve(elem, auth=auth)
        elif isinstance(obj, list):
            for elem in obj:
                yield self.resolve(elem, auth=auth)
        if isinstance(obj, Resource):
            yield obj.resolve(auth=auth)

    @tornado.gen.coroutine
    def _compute_result(self, resource, data):
        """ Run the job, mark as done.
        """
        data = self.replace(data)
        auth = self.request.headers.get('Authorization', None)
        yield self.resolve(data, auth)
        yield self.compute(resource, data)
        resource['status'] = 201


class Job(tornado.web.RequestHandler):
    """ A job is a running algorithm with an unique ID.
    This handler return results
    """

    def initialize(self, *args, **kwargs):
        self.resource_storage = kwargs.pop('resource_storage')

    @report
    def get(self, uid):
        if uid in self.resource_storage:
            response = self.resource_storage[uid]
            self.write(json.dumps(response))
            self.set_status(200)
        else:
            self.set_status(404, "Unknown job")
