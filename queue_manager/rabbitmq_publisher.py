# -*- coding: utf-8 -*-
"""Inspired on: http://pika.readthedocs.io/en/0.10.0/examples/asynchronous_publisher_example.html"""
import logging

import pika

logger = logging.getLogger(__name__)


class RabbitMqPublisher:
    connection = None

    def __init__(self, amqp_urls, exchange=None, exchange_type=None,
                 queue=None, queue_properties=None, routing_key=None,
                 declare=True):

        self._urls = (amqp_urls,) if isinstance(amqp_urls, str) else amqp_urls
        self.exchange = exchange
        self.exchange_type = exchange_type
        self.queue = queue
        self.queue_properties = queue_properties
        self.routing_key = routing_key
        self.declare = declare

    def ping(self):
        is_open = self.__connect().is_open
        self.__disconnect()
        return is_open

    def message_count(self):
        channel = self.__connect().channel()
        method_frame = channel.queue_declare(queue=self.queue, arguments=self.queue_properties)

        message_count = method_frame.method.message_count

        self.__disconnect()
        return message_count

    def __connect(self):
        logger.debug('Connecting to %s', self._urls)
        urls = tuple(map(pika.URLParameters, self._urls))

        self.connection = pika.BlockingConnection(urls)
        return self.connection

    def __get_channel(self):
        channel = self.__connect().channel()
        if not self.declare:
            return channel

        channel.queue_declare(queue=self.queue, arguments=self.queue_properties) if self.queue else None
        channel.exchange_declare(exchange=self.exchange, exchange_type=self.exchange_type) if self.exchange else None

        if self.queue and self.exchange:
            channel.queue_bind(queue=self.queue, exchange=self.exchange, routing_key=self.routing_key)

        return channel

    def get_publish_params(self, message, message_properties):
        pika_properties = pika.BasicProperties(**message_properties) if message_properties else None

        return dict(
                exchange=self.exchange or '',
                routing_key=self.routing_key if isinstance(self.routing_key, str) else self.queue or '',
                body=message,
                properties=pika_properties,
                mandatory=True
        )

    def publish_message(self, message, message_properties=None):
        channel = self.__get_channel()

        ret = channel.basic_publish(**self.get_publish_params(message, message_properties))

        logger.debug("pushed %s return(%r)", message, ret)
        self.__disconnect()
        return ret

    def __disconnect(self):
        if not self.connection:
            return

        logger.debug("disconnecting %r", self.connection.close())
        self.connection = None
