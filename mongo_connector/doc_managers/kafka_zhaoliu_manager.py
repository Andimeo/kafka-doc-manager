# Copyright 2012 10gen, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# This file will be used with PyPi in order to package and distribute the final
# product.

""" Kafka doc manager
"""

from kafka.client import KafkaClient
from kafka.producer import SimpleProducer
from mongo_connector.doc_managers.doc_manager_base import DocManagerBase
from mongo_connector import constants
from mongo_connector.errors import OperationFailed
from mongo_connector.compat import u

class DocManager(DocManagerBase):
    """ Connects to a kafka instance, generates producers for a given
    database and collection
    """

    def __init__(self, url, auto_commit=True, unique_key='_id',
                 chunk_size=constants.DEFAULT_MAX_BULK, **kwargs):
        """Connect to kafka instance
        """
        url_info = url.split(":")
        if len(url_info) < 2:
            raise SystemError

        self.server = KafkaClient(url)
        self.producer_dict = {}
        self.auto_commit = auto_commit
        print url

    def generate_producer(self, namespace):
        """ Generates a producer for a given database and collection
        """
        database, coll = namespace.split('.', 1)
        topic = (('%s-%s') % (database, coll))
        if topic not in self.producer_dict:
            try:
                self.producer_dict[topic] = SimpleProducer(
                    self.server,
                    async=True)
            except:
                self.producer_dict[topic] = None
        return self.producer_dict[topic]

    def stop(self):
        print "stop"
        """ Stops the instance
        """
        self.auto_commit = False
        self.server.close()

    def upsert(self, doc, namespace, timestamp):
        print "upsert"
        """ Sends the document to kafka
        """
        if doc.has_key('isInTangoDir'):
          print doc
          database, coll = namespace.split('.', 1)
          topic = (('%s-%s') % (database, coll))
          producer = self.generate_producer(namespace)
          if producer:
              producer.send_messages(topic, str(doc))
          else:
              raise SystemError

    def remove(self, doc):
        print "remove"
        """ Not revelant in this context
        """
        pass

    def search(self, start_ts, end_ts):
        print "search"
        """ Not revelant in this context
        """
        pass

    def commit(self):
        print "commit"
        """ Not revelant in this context
        """
        pass

    def run_auto_commit(self):
        print 'run_auto_commit'
        """ Not revelant in this context
        """
        pass

    def get_last_doc(self):
        print 'get_last_doc'
        """ This is probably possible but unsure of implementation.
            Hestitant to implement this since it might be system
            specific.
        """
        pass
