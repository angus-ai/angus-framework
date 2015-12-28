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

""" Some helper for angus
"""

import base64


def extract_user(handler):
    """
    Extract requester login from (Basic) Authorization token.
    """
    auth_header = handler.request.headers.get('Authorization')
    if auth_header is not None:
        auth_header = base64.decodestring(auth_header[6:])
        user = auth_header.split(':', 2)[0]
        return user
    else:
        return "anonymous"
