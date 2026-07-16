# -*- coding: utf-8 -*-
import logging
from unittest.mock import MagicMock

import pytest
import pytest_asyncio
import aio_pika

import cabbagok

logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)


@pytest.fixture
def connection(event_loop):
    conn = cabbagok.AmqpConnection(hosts=[(HOST, 5672)], loop=event_loop)
    conn.connection = MockConnection()
    conn.channel = MockChannel()
    return conn


@pytest_asyncio.fixture
async def rpc(connection):
    _rpc = cabbagok.AsyncAmqpRpc(connection=connection)
    await _rpc.connect()
    return _rpc


# some non-default values to use in tests


HOST = "fake_amqp_host"
TEST_EXCHANGE = "rpc_exchange"
TEST_DESTINATION = "rpc_destination"
SUBSCRIPTION_QUEUE = "rpc_subscription_queue"
RANDOM_QUEUE = "amq.gen-random_queue_name"
SUBSCRIPTION_KEY = "rpc_subscription_key"
RESPONSE_CORR_ID = "response_correlation_id"
CONSUMER_TAG = "some_consumer_tag"
DELIVERY_TAG = 10

# aio_pika classes mocked as factory functions:


def MockConnection():
    m = MagicMock(name="MockConnection")
    m.is_closed = False
    m.channel = pytest.mark.asyncio(MagicMock(return_value=MockChannel()))
    return m


def MockChannel():
    m = MagicMock(name="MockChannel")
    m.is_closed = False

    async def declare_queue(*args, **kwargs):
        q = MagicMock()
        q.name = kwargs.get("name") or RANDOM_QUEUE

        async def consume(*a, **kw):
            return CONSUMER_TAG

        q.consume = consume
        return q

    m.declare_queue = declare_queue
    return m


def MockMessage(body=b""):
    m = MagicMock()
    m.body = body
    m.reply_to = RANDOM_QUEUE
    m.correlation_id = RESPONSE_CORR_ID

    async def process(*args, **kwargs):
        class AsyncContext:
            async def __aenter__(self):
                pass

            async def __aexit__(self, exc_type, exc_val, exc_tb):
                pass

        return AsyncContext()

    m.process = process

    async def reject(*args, **kwargs):
        pass

    m.reject = reject
    return m
