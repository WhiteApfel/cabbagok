# -*- coding: utf-8 -*-
import asyncio
import inspect
import logging
import random
import uuid
from functools import partial
from itertools import cycle
from typing import Optional, Callable, Union, Awaitable, Mapping, Dict

import aio_pika
from aio_pika.exceptions import AMQPException

from .utils import FibonaccianBackoff

logger = logging.getLogger("cabbagok")


class ServiceUnavailableError(Exception):
    """External service unavailable."""


class AmqpConnection:
    def __init__(
        self,
        hosts=None,
        username="guest",
        password="guest",
        virtualhost="/",
        loop=None,
        ssl=False,
    ):
        """
        :param hosts: iterable with tuples (host, port), default localhost:5672
        :param username: AMQP login, default guest
        :param password: AMQP password, default guest
        :param virtualhost: AMQP virtual host, default /
        :param loop: asyncio event loop, default current event loop
        :param ssl: bool, uses ssl if True, default False
        """
        self.loop = loop
        self.username = username
        self.password = password
        self.virtualhost = virtualhost
        self.hosts = hosts if hosts is not None else [("localhost", 5672)]
        self._connection_cycle = self.cycle_hosts()
        self.connection: Optional[aio_pika.RobustConnection] = None
        self.channel: Optional[aio_pika.RobustChannel] = None
        self.ssl = ssl
        self._connect_lock = asyncio.Lock()

    def cycle_hosts(self, shuffle=False):
        if shuffle:
            random.shuffle(self.hosts)
        yield from cycle(self.hosts)

    async def connect(self):
        """Connect to AMQP broker. On failure this function will endlessly try reconnecting.
        Do nothing if already connected or connecting.
        """
        async with self._connect_lock:
            if self.is_connected:
                return

            delay = FibonaccianBackoff(limit=60.0)
            for host, port in self._connection_cycle:
                try:
                    kwargs = {}
                    if self.ssl:
                        kwargs["ssl"] = True

                    self.connection = await aio_pika.connect_robust(
                        host=host,
                        port=port,
                        login=self.username,
                        password=self.password,
                        virtualhost=self.virtualhost,
                        loop=self.loop or asyncio.get_running_loop(),
                        **kwargs,
                    )
                    self.channel = await self.connection.channel()
                    logger.info(f"wait connection to {host}:{port}")
                    break
                except (ConnectionError, OSError, AMQPException) as e:
                    next_delay = delay.next()
                    logger.warning(
                        f"failed to connect to {host}:{port}, error <{e.__class__.__name__}> {e}, "
                        f"retrying in {next_delay} seconds"
                    )
                    await asyncio.sleep(next_delay)
                except Exception as e:
                    logger.error(
                        f"connection failed, not retrying: <{e.__class__.__name__}> {e}"
                    )
                    raise

    async def disconnect(self):
        async with self._connect_lock:
            if self.channel and not self.channel.is_closed:
                await self.channel.close()
            if self.connection and not self.connection.is_closed:
                await self.connection.close()
            self.channel = None
            self.connection = None

    @property
    def is_connected(self):
        """Property, required for rpc to check readiness"""
        return self.connection is not None and not self.connection.is_closed


class AsyncAmqpRpc:
    def __init__(
        self,
        connection: AmqpConnection,
        exchange_params: Mapping = None,
        queue_params: Mapping = None,
        subscriptions=None,
        prefetch_count=1,
        raw=False,
        default_response_timeout=15.0,
        shutdown_timeout=60.0,
        connection_delay: float = 0.1,
        callback_exchange="",
    ):
        """
        All arguments are optional. If `request_handler` is not supplied or None, RPC works only in client mode.

        :param queue_params: options for creating queues, default durable and DLX
        :param exchange_params: options when creating exchanges, default durable and type topic
        :param subscriptions: list of tuples (handler, queue, exchange, routing_key, queue_params, exchange_params)
                Rightmost parameters are optional, you can specify only (handler, queue).
        :param raw: do not attempt decoding, use iff `request_handler` maps `bytes -> bytes`.
        :param prefetch_count: per-consumer prefetch message limit, default 1
        :param default_response_timeout: default timeout for awaiting response when sending remote calls
        :param shutdown_timeout: timeout for handlers to finish gracefully on shutdown
        """
        self.raw = raw
        self.queue_params = queue_params
        self.exchange_params = exchange_params
        self.start_subscriptions = list(subscriptions) if subscriptions else []
        self.default_response_timeout = default_response_timeout
        self.shutdown_timeout = shutdown_timeout
        self.connection = connection
        self.prefetch_count = prefetch_count
        self.keep_running = True
        self.callback_queue: Optional[aio_pika.RobustQueue] = None
        self.callback_exchange = callback_exchange
        self._responses: Dict[str, asyncio.Future] = {}
        self._tasks = set()
        self._consumers = {}  # Map of (consumer_tag) -> queue
        self.connection_delay = connection_delay
        self._connect_lock = asyncio.Lock()

    def _prepare_payload(self, data: Union[str, bytes]):
        if isinstance(data, str):
            return data.encode("utf-8"), False
        return data, True

    async def connect(self):
        async with self._connect_lock:
            if self.connection.is_connected and self.callback_queue:
                return

            await self.connection.connect()
            channel = self.connection.channel

            self.callback_queue = await channel.declare_queue(exclusive=True)

            if self.callback_exchange != "":
                exchange = await channel.declare_exchange(
                    name=self.callback_exchange,
                    type=aio_pika.ExchangeType.TOPIC,
                    durable=True,
                )
                await self.callback_queue.bind(
                    exchange=exchange,
                    routing_key=self.callback_queue.name,
                )

            await self.callback_queue.consume(self._on_response, no_ack=False)
            logger.debug(f"listening on callback queue {self.callback_queue.name}")

    async def declare(
        self,
        queue: str,
        exchange: str = "",
        routing_key: str = None,
        queue_params: Mapping = None,
        exchange_params: Mapping = None,
    ):
        """
        Set up necessary objects — exchange, queue, binding, QoS.

        :param queue: queue name
        :param exchange: exchange name, default '' (default AMQP exchange)
        :param routing_key: routing key, default same as `queue`
        :param queue_params: options for the queue, default durable and DLX
        :param exchange_params: options for the exchange, default durable and type topic
        """
        if routing_key is None:
            routing_key = queue

        exchange_params = exchange_params or dict(type_name="topic", durable=True)
        mapped_ex_params = dict(exchange_params)
        if "type_name" in mapped_ex_params:
            type_name = mapped_ex_params.pop("type_name")
            if type_name == "topic":
                mapped_ex_params["type"] = aio_pika.ExchangeType.TOPIC
            elif type_name == "direct":
                mapped_ex_params["type"] = aio_pika.ExchangeType.DIRECT
            elif type_name == "fanout":
                mapped_ex_params["type"] = aio_pika.ExchangeType.FANOUT
            else:
                mapped_ex_params["type"] = type_name

        queue_params = queue_params or dict(
            durable=True,
            arguments={
                "x-dead-letter-exchange": "DLX",
                "x-dead-letter-routing-key": "dlx_rpc",
            },
        )

        channel = self.connection.channel

        ex = None
        if exchange != "":
            ex = await channel.declare_exchange(name=exchange, **mapped_ex_params)

        q = await channel.declare_queue(name=queue, **queue_params)

        if exchange != "":
            await q.bind(exchange=ex, routing_key=routing_key)

        await channel.set_qos(prefetch_count=self.prefetch_count)
        return q, ex

    async def subscribe(
        self,
        request_handler: Union[
            Callable[[str], Optional[str]],
            Callable[[bytes], Optional[bytes]],
            Callable[[str], Awaitable[Optional[str]]],
            Callable[[bytes], Awaitable[Optional[bytes]]],
        ],
        queue: str,
        exchange: str = "",
        routing_key: str = None,
        add_to_start: bool = False,
    ) -> str:
        """
        Subscribe to a specific queue. Exchange and queue will be created if they do not exist.

        :param request_handler: request handler, can be a normal or coroutine function
                that maps either str->str or bytes->bytes. If `request_handler` returns None,
                it is taken to mean no response is needed.
        :param exchange: exchange name, default '' (default AMQP exchange)
        :param queue: queue name
        :param routing_key: routing key, default same as `queue`
        :param add_to_start: add to start_subscriptions
        :return: consumer_tag
        """
        if routing_key is None:
            routing_key = queue

        q, _ = await self.declare(
            queue=queue,
            exchange=exchange,
            routing_key=routing_key,
            queue_params=self.queue_params,
            exchange_params=self.exchange_params,
        )

        consumer_tag = await q.consume(
            partial(self._on_request, request_handler=request_handler)
        )

        self._consumers[consumer_tag] = q

        if add_to_start:
            params = (request_handler, queue, exchange, routing_key)
            if params not in self.start_subscriptions:
                self.start_subscriptions.append(params)

        logger.debug(
            f"subscribed to queue {queue}, bound to exchange {exchange} with key {routing_key} "
            f"(consumer tag {consumer_tag})"
        )
        return consumer_tag

    async def unsubscribe(self, consumer_tag: str):
        """
        Stop consuming on a queue.

        :param consumer_tag: consumer tag returned by `subscribe()`
        """
        if consumer_tag in self._consumers:
            q = self._consumers.pop(consumer_tag)
            await q.cancel(consumer_tag)
            logger.debug(f"unsubscribed from a queue (consumer tag {consumer_tag})")

    async def _on_request(self, message: aio_pika.IncomingMessage, request_handler):
        """Run handle_rpc() in background."""
        task = asyncio.ensure_future(self.handle_rpc(message, request_handler))
        self._tasks.add(task)
        task.add_done_callback(lambda fut: self._tasks.discard(fut))

    async def handle_rpc(
        self,
        message: aio_pika.IncomingMessage,
        request_handler,
    ):
        """Process request with handler and send response if needed."""
        async with message.process(requeue=True, ignore_processed=True):
            try:
                data = message.body if self.raw else message.body.decode("utf-8")
                logger.debug(
                    f"> handle_rpc: data {data}, routing_key {message.reply_to}, "
                    f"correlation_id {message.correlation_id}"
                )
                response = request_handler(data)
                if inspect.isawaitable(response):
                    response = await response
            except Exception as e:
                logger.error(
                    f"handle_rpc. error <{e.__class__.__name__}> {e}, routing_key {message.reply_to}, "
                    f"correlation_id {message.correlation_id}"
                )
                await message.reject(requeue=True)
            else:
                responding = message.reply_to is not None and response is not None
                logger.debug(
                    f'{"< " * responding}handle_rpc: responding? {responding}, routing_key {message.reply_to}, '
                    f"correlation_id {message.correlation_id}, result {response}"
                )
                if responding:
                    if isinstance(response, bytes):
                        payload = response
                    else:
                        payload = str(response).encode("utf-8")
                    reply_msg = aio_pika.Message(
                        body=payload,
                        correlation_id=message.correlation_id,
                    )

                    default_exchange = self.connection.channel.default_exchange
                    await default_exchange.publish(
                        reply_msg,
                        routing_key=message.reply_to,
                    )

    async def run_server(self):
        """Main routine for the server."""
        try:
            while self.keep_running:
                try:
                    await self.connect()
                    self._consumers.clear()
                    for params in self.start_subscriptions:
                        await self.subscribe(*params, add_to_start=False)

                    # Wait for connection to close
                    close_event = asyncio.Event()

                    def on_close(*args, **kwargs):
                        if not close_event.is_set():
                            close_event.set()

                    self.connection.connection.close_callbacks.add(on_close)
                    await close_event.wait()
                except Exception as e:
                    if not self.keep_running:
                        break
                    logger.warning(
                        f"amqp connection lost: <{e.__class__.__name__}> {e}, reconnecting"
                    )
                    await asyncio.sleep(self.connection_delay)
                    continue
        finally:
            await self.connection.disconnect()

    async def run(self, app=None):
        """aiohttp-compatible on_startup coroutine."""
        self._server_task = asyncio.ensure_future(self.run_server())
        await self.wait_connected()
        logger.info("Waiting finished. Connected successfully.")

    async def stop(self, app=None):
        """aiohttp-compatible on_shutdown coroutine."""
        for consumer_tag in list(self._consumers.keys()):
            await self.unsubscribe(consumer_tag)
        if self._tasks:
            logger.info(f"waiting for {len(self._tasks)} task(s) to finish normally")
            done, pending = await asyncio.wait(
                self._tasks, timeout=self.shutdown_timeout
            )
            if pending:
                level = logger.warning
            else:
                level = logger.info
            level(
                f"{len(done)} task(s) finished, {len(pending)} task(s) did not finish in time"
            )
        self.keep_running = False
        await self.connection.disconnect()

    # AMQP client implementation

    async def send_rpc(
        self,
        destination: str,
        data: Union[str, bytes],
        exchange: str = "",
        await_response=True,
        timeout: float = None,
        correlation_id: str = None,
    ) -> Union[str, bytes, None]:
        """
        Execute a method on remote server. Sends `data` to `destination` routing key.

        If `await_response` is True, the call blocks coroutine until the result is returned or until `timeout` seconds
        passed (class default is used if None). AMQP correlation_id is set to `correlation_id` or new UUID if None.

        Raises `ServiceUnavailableError` on response timeout.
        """
        payload, raw = self._prepare_payload(data)

        if await_response and correlation_id is None:
            correlation_id = str(uuid.uuid4())

        if not (self.connection.is_connected and self.connection.channel):
            await self.connect()

        msg = aio_pika.Message(
            body=payload,
            reply_to=self.callback_queue.name if await_response else None,
            correlation_id=correlation_id if await_response else None,
        )

        logger.debug(
            f"< send_rpc: destination {destination}, data {data}, "
            f"awaiting? {await_response}, timeout {timeout}, correlation_id {correlation_id}"
        )

        channel = self.connection.channel

        if exchange == "":
            ex = channel.default_exchange
        else:
            ex = await channel.get_exchange(exchange)

        await ex.publish(
            msg,
            routing_key=destination,
        )

        if await_response:
            if timeout is None:
                timeout = self.default_response_timeout
            response = await self._await_response(
                correlation_id=correlation_id, timeout=timeout
            )
            if not raw:
                response = response.decode("utf-8")
            logger.debug(f"> send_rpc: response {response}")
            return response

    async def _await_response(self, correlation_id, timeout):
        """Wait for a response with given correlation id. Blocks current Task."""
        if correlation_id in self._responses:
            raise ValueError(f"correlation_id {correlation_id} is already in use")

        self._responses[correlation_id] = asyncio.get_running_loop().create_future()
        try:
            await asyncio.wait_for(self._responses[correlation_id], timeout=timeout)
            return self._responses[correlation_id].result()
        except asyncio.TimeoutError:
            logger.warning(f"request {correlation_id} timed out")
            fut = self._responses.get(correlation_id)
            if fut is not None and not fut.done():
                fut.cancel()
            raise ServiceUnavailableError("Request timed out") from None
        finally:
            self._responses.pop(correlation_id, None)

    async def _on_response(self, message: aio_pika.IncomingMessage):
        """Set response result. Called by aioamqp on a message in callback queue."""
        async with message.process():
            correlation_id = message.correlation_id
            if correlation_id in self._responses:
                fut = self._responses[correlation_id]
                if not fut.cancelled() and not fut.done():
                    fut.set_result(message.body)
            else:
                logger.warning(
                    f"unexpected message with correlation_id {correlation_id}."
                )

    async def wait_connected(self):
        while not (self.connection.is_connected and self.callback_queue):
            if hasattr(self, "_server_task") and self._server_task.done():
                exc = self._server_task.exception()
                if exc:
                    raise exc
                break
            if not self.keep_running:
                break
            logger.debug(f"Waiting connection for {self.connection_delay}s...")
            await asyncio.sleep(self.connection_delay)
