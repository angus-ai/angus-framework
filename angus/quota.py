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
from cassandra.policies import ConstantReconnectionPolicy
from cassandra.query import BatchStatement, BatchType

import logging
import os
import datetime
import pytz

__updated__ = "2017-01-03"
__author__ = "Aurélien Moreau"
__copyright__ = "Copyright 2015-2016, Angus.ai"
__credits__ = ["Aurélien Moreau", "Gwennael Gate"]
__license__ = "Apache v2.0"
__maintainer__ = "Aurélien Moreau"
__status__ = "Production"

LOGGER = logging.getLogger(__name__)

CASSANDRA_LOGGING = logging.getLogger("cassandra.connection")
CASSANDRA_LOGGING.setLevel(logging.INFO)

class CassandraRecorder(object):
    BATCH_SIZE = 100

    def __init__(self):
        self._reset()
        self.contact_point = os.environ.get('CASSANDRA_CONTACT_POINT', None)
        if self.contact_point is None:
            LOGGER.warning("No CASSANDRA_CONTACT_POINT environment variable set.")
        self._connect()

    def _connect(self):
        if self.session is None:
            try:
                self.cluster = Cluster(reconnection_policy=ConstantReconnectionPolicy(60, None))
                self.session = self.cluster.connect()

                self.session.execute("""
                CREATE KEYSPACE IF NOT EXISTS api_quota
                WITH replication = {'class': 'SimpleStrategy', 'replication_factor':1};
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

                self.prepared = self.session.prepare("""
                UPDATE api_quota.quantities SET quantity = quantity + ?
                WHERE client_id = ? AND service = ? AND day = ?;
                """)
            except NoHostAvailable:
                self._reset()

    def _reset(self):
        self.session = None
        self.batch = None
        self.prepared = None
        self.queue = 0

    def _delay(self, statement):
        if self.batch is None:
            self.batch = BatchStatement(batch_type=BatchType.COUNTER)

        self.batch.add(statement)
        self.queue += 1

        if self.queue >= self.BATCH_SIZE:
            try:
                LOGGER.debug("Flush quota buffer")
                self.session.execute_async(self.batch)
                self.queue = 0
                self.batch.clear()
            except NoHostAvailable:
                self.reset()

    def inc(self, client_id, service, quantity):
        self._connect()
        if self.session is None:
            LOGGER.warning("No quota backend.")
            return

        timestamp = datetime.datetime.now(pytz.utc)
        day = timestamp.toordinal()

        try:
            bound = self.prepared.bind((quantity, client_id, service, day))
            LOGGER.debug("Inc client_id={}".format(client_id))
            self._delay(bound)
        except NoHostAvailable:
            self._reset()
