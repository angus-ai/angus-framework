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
"""
    A last updated comes first class based on OrderedDict
    with thread safe add an remove operations
    to handle alive Sessions
"""

import threading
import collections

class Sessions(object):
    def __init__(self, max_sessions=50):
        self.sessions = collections.OrderedDict()
        self.max_sessions = max_sessions
        self.lock = threading.Lock()

    def __contains__(self, key):
        return key in self.sessions

    def __str__(self):
        return str(self.sessions)

    def add(self, key):
        self.lock.acquire()
        if key in self.sessions:
            del self.sessions[key]
        self.sessions[key] = True
        self.lock.release()

    def remove(self):
        self.lock.acquire()
        to_rm = []
        while len(self.sessions) > self.max_sessions:
            to_rm.append(self.sessions.popitem(last=False)[0])
        self.lock.release()
        return to_rm
