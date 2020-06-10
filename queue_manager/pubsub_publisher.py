#!/usr/bin/env python
"""
.. code:: python

    publisher = PubsubPublisher('project_id', 'path/to/sa.json', 'topic_name')

    publisher.publish_message('hello')
"""
import logging
from collections import defaultdict
from collections.abc import Mapping
from time import time

from . import QueuePublisher

try:
    from google.api_core.exceptions import NotFound
    from google.cloud import pubsub_v1
    from google.oauth2.service_account import Credentials
except ImportError:
    raise Exception('You need to install google-cloud-pubsub')

logger = logging.getLogger(__name__)


class PubsubPublisher(QueuePublisher):
    _last_assertion = defaultdict(int)
    _assertion_ttl = 30
    scope = 'https://www.googleapis.com/auth/pubsub'

    def __init__(self, project_id, service_account, topic_name='ping'):
        logger.debug('Init PubsubPublisher ...')
        self.project_id = project_id
        self.service_account = service_account

        full_topic_name = 'projects/{project_id}/topics/{topic}'.format(
            project_id=self.project_id,
            topic=topic_name,
        )
        topic_name_ping = 'projects/{project_id}/topics/ping'.format(
            project_id=self.project_id,
        )

        self.full_topic_name = full_topic_name
        self.topic_name_ping = topic_name_ping

        self.client = self.setup_client()

    def assert_topic(self, topic_name):
        if time() - self._assertion_ttl < self._last_assertion[topic_name]:
            return
        self._last_assertion[topic_name] = 0
        try:
            logger.debug('Getting topic %s', topic_name)
            self.client.get_topic(topic_name)
            logger.debug('Nice, topic already exists %s', topic_name)
            self._last_assertion[topic_name] = time()
        except NotFound:
            logger.warning('Topic doesnt exist, creating a new topic %s', topic_name)
            self.client.create_topic(topic_name)

    def _get_credentials(self):
        if isinstance(self.service_account, Mapping):
            return Credentials.from_service_account_info(self.service_account, scopes=(self.scope,))
        return Credentials.from_service_account_file(self.service_account, scopes=(self.scope,))

    def setup_client(self):
        credentials = self._get_credentials()
        publisher_client = pubsub_v1.PublisherClient(credentials=credentials)
        return publisher_client

    def ping(self):
        self.assert_topic(self.topic_name_ping)
        response = self.client.publish(topic=self.topic_name_ping, data=b'OK')
        message_id = response.result(timeout=5000)
        logger.debug('Response from PubSub: %s', message_id)
        return message_id

    def publish_message(self, message, message_properties=None):
        message_properties = message_properties or {}
        if type(message) is not bytes:
            message = message.encode('utf-8')
        self.assert_topic(self.full_topic_name)
        response = self.client.publish(self.full_topic_name, message, **message_properties)
        return response.result(timeout=5000)
