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

""" Quota management
"""

from cassandra.cluster import Cluster, NoHostAvailable
import logging
import datetime
import pytz

__updated__ = "2016-12-07"
__author__ = "Aurélien Moreau"
__copyright__ = "Copyright 2015-2016, Angus.ai"
__credits__ = ["Aurélien Moreau", "Gwennael Gate"]
__license__ = "Apache v2.0"
__maintainer__ = "Aurélien Moreau"
__status__ = "Production"

LOGGER = logging.getLogger(__name__)

class CassandraRecorder(object):
    def __init__(self, *args, **kwargs):
        self.cluster = Cluster(*args, **kwargs)

        try:
            self.session = self.cluster.connect()
        except NoHostAvailable:
            self.session = None
            return

        self.session.execute("""
        CREATE KEYSPACE IF NOT EXISTS api_quota
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor':2};
        """)
        self.session.set_keyspace('api_quota')
        self.session.execute("""
        CREATE TABLE IF NOT EXISTS api_quota.quantities (
        client_id text,
        day int,
        service text,
        quantity counter,
        PRIMARY KEY ((client_id, day), service)
        );
        """)

    def inc(self, client_id, service, quantity):
        if self.session is None:
            LOGGER.warning("No quota backend")
            return
        timestamp = datetime.datetime.now(pytz.utc)
        day = timestamp.toordinal()
        self.session.execute("""
        UPDATE api_quota.quantities SET quantity = quantity + %s
        WHERE client_id = %s AND service = %s AND day = %s;
        """, (quantity, client_id, service, day))
