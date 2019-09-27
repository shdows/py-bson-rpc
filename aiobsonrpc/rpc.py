# -*- coding: utf-8 -*-


"""
Main module providing BSONRpc and JSONRpc.
"""

import re
import six
import asyncio

from aiobsonrpc.definitions import Definitions
from aiobsonrpc.exceptions import BsonRpcError, ResponseTimeout
from aiobsonrpc.dispachter import Dispatcher
from aiobsonrpc.framing import JSONFramingRFC7464
from aiobsonrpc.options import DefaultOptionsMixin, MessageCodec
from aiobsonrpc.socket_queue import BSONCodec, JSONCodec, SocketQueue
from aiobsonrpc.util import PeerProxy

__license__ = 'http://mozilla.org/MPL/2.0/'


class ResultScope(object):

    def __init__(self, dispatcher, msg_id):
        self.dispatcher = dispatcher
        self.msg_id = msg_id

    def __enter__(self):
        return self.dispatcher.register(self.msg_id)

    def __exit__(self, tp, value, tb):
        self.dispatcher.unregister(self.msg_id)


class RpcBase(DefaultOptionsMixin):
    protocol = 'base'
    protocol_version = ''

    def __init__(self, reader: asyncio.StreamReader,
                 writer: asyncio.StreamWriter,
                 codec, services=None, **options):
        assert (hasattr(services, 'request_handlers') and
                hasattr(services, 'notification_handlers'))
        for key, value in options.items():
            setattr(self, key, value)

        if getattr(self, 'loop', None) is None:
            self.loop = asyncio.get_event_loop()

        self.definitions = Definitions(
            self.protocol, self.protocol_version,
            self.no_arguments_presentation
        )
        self.services = services
        self.socket_queue = SocketQueue(reader, writer, codec, self.loop)
        self.dispatcher = Dispatcher(self)

    @property
    def is_closed(self):
        """
        :property: bool -- Closed by peer node or with ``close()``
        """
        return self.socket_queue.is_closed

    async def invoke_request(self, method_name, *args, **kwargs):
        """
        Invoke RPC Request.

        :param method_name: Name of the request method.
        :type method_name: str
        :param args: Arguments
        :param kwargs: Keyword Arguments.
        :returns: Response value(s) from peer.
        :raises: BsonRpcError or ResponseTimeout

        A timeout for the request can be set by giving a special keyword
        argument ``timeout`` (float value of seconds) which can be prefixed
        by any number of underscores - if necessary - to differentiate it from
        the actual keyword arguments going to the peer node method call.

        e.g.
        ``invoke_request('testing', [], {'_timeout': 22, '__timeout: 10.0})``
        would call a request method ``testing(_timeout=22)`` on the RPC peer
        and wait for the response for 10 seconds.

        **NOTE:**
          Use either arguments or keyword arguments. Both can't
          be used in a single call.
          (Naturally the timeout argument does not count to the rule.)
        """
        rec = re.compile(r'^_*timeout$')
        to_keys = sorted(filter(lambda x: rec.match(x), kwargs.keys()))
        if to_keys:
            timeout = kwargs[to_keys[0]]
            del kwargs[to_keys[0]]
        else:
            timeout = None
        msg_id = six.next(self.id_generator)
        try:
            with ResultScope(self.dispatcher, msg_id) as future:
                await self.socket_queue.put(
                    self.definitions.request(msg_id, method_name, args, kwargs)
                )
                return await asyncio.wait_for(
                    future, loop=self.loop, timeout=timeout
                )
        except asyncio.TimeoutError:
            raise ResponseTimeout(u'Waiting response expired.')
        except BsonRpcError as e:
            raise e

    def invoke_notification(self, method_name, *args, **kwargs):
        """
        Send an RPC Notification.

        :param method_name: Name of the notification method.
        :type method_name: str
        :param args: Arguments
        :param kwargs: Keyword Arguments.

        **NOTE:**
          Use either arguments or keyword arguments. Both can't
          be used simultaneously in a single call.
        """
        coro = self.socket_queue.put(
            self.definitions.notification(method_name, args, kwargs)
        )
        self.loop.create_task(coro)

    def get_peer_proxy(self, requests=None, notifications=None, timeout=None):
        """
        Get a RPC peer proxy object. Method calls to this object
        are delegated and executed on the connected RPC peer.

        :param requests: A list of method names which can be called and
                         will be delegated to peer node as requests.
                         Default: None -> All arbitrary attributes are
                         handled as requests to the peer.
        :type requests: list of str | None
        :param notifications: A list of method names which can be called and
                              will be delegated to peer node as notifications.
                              Default: None -> If ``requests`` is not ``None``
                              all other attributes are handled as
                              notifications.
        :type notifications: list of str | None
        :param timeout: Timeout in seconds, maximum time to wait responses
                        to each Request.
        :type timeout: float | None
        :returns: A proxy object. Attribute method calls delegated over RPC.

        ``get_peer_proxy()`` (without arguments) will return a proxy
        where all attribute method calls are turned into Requests,
        except calls via ``.n`` which are turned into Notifications.
        Example:
        ::

          proxy = rpc.get_peer_proxy()
          proxy.n.log_this('hello')          # -> Notification
          result = proxy.swap_this('Alise')  # -> Request

        But if arguments are given then the interpretation is explicit and
        ``.n``-delegator is not used:
        ::

          proxy = rpc.get_peer_proxy(['swap_this'], ['log_this'])
          proxy.log_this('hello')            # -> Notification
          result = proxy.swap_this('esilA')  # -> Request
        """
        return PeerProxy(self, requests, notifications, timeout)

    def close(self):
        """
        Close the connection and stop the internal dispatcher.
        """
        # Closing the socket queue causes the dispatcher to close also.
        self.socket_queue.close()

    async def join(self, timeout=None):
        """
        Wait for the internal dispatcher to shut down.

        :param timeout: Timeout in seconds, max time to wait.
        :type timeout: float | None
        """
        await self.socket_queue.join(timeout=timeout)


class DefaultServices(object):
    request_handlers = {}

    notification_handlers = {}


class BSONRpc(RpcBase):
    """
    BSON RPC Connector. Follows closely `JSON-RPC 2.0`_ specification
    with only few differences:

    * Batches are not supported since BSON does not support top-level lists.
    * Keyword 'jsonrpc' has been replaced by 'bsonrpc'

    Connects via socket to RPC peer node. Provides access to the services
    provided by the peer node and makes local services available for the peer.

    To use BSONRpc you need to install ``pymongo``-package
    (see requirements.txt)

    .. _`JSON-RPC 2.0`: http://www.jsonrpc.org/specification
    """

    #: Protocol name used in messages
    protocol = 'bsonrpc'

    #: Protocol version used in messages
    protocol_version = '2.0'

    def __init__(self, reader, writer, services=None, **options):
        """
        :param socket: Socket connected to the peer. (Anything behaving like
                       a socket and implementing socket methods ``close``,
                       ``recv``, ``sendall`` and ``shutdown`` is equally
                       viable)
        :type socket: socket.socket
        :param services: Object providing request handlers and
                         notification handlers to be exposed to peer.
                         See `Providing Services`_ for details.
        :type services: ``@service_class`` Class | ``None``
        :param options: Modify behavior by overriding the library defaults.

        **Available options:**

        .. include:: options.snippet

        **custom_codec_implementation**
          Is by default ``None`` in which case this library is able
          to automatically use bson codec from either pymongo or bson
          (https://pypi.python.org/pypi/pymongo or
          https://pypi.python.org/pypi/bson) libraries depending on
          which ever is installed on the system.

          Otherwise if you provide a custom codec it must have callable
          attibutes (aka member methods) ``dumps`` and ``loads`` with
          function signatures identical to those of the bson:0.4.6 library.

        All options as well as any possible custom/extra options are
        available as attributes of the constructed class object.
        """
        self.codec = MessageCodec.BSON
        if not services:
            services = DefaultServices()
        cci = options.get('custom_codec_implementation', None)
        super(BSONRpc, self).__init__(
            reader, writer, BSONCodec(custom_codec_implementation=cci),
            services=services, **options
        )


class JSONRpc(RpcBase):
    """
    JSON RPC Connector. Implements the `JSON-RPC 2.0`_ specification.

    Connects via socket to RPC peer node. Provides access to the services
    provided by the peer node. Optional ``services`` parameter will take an
    object of which methods are accessible to the peer node.

    Various methods of JSON message framing are available for the stream
    transport.

    .. _`JSON-RPC 2.0`: http://www.jsonrpc.org/specification
    """

    #: Protocol name used in messages
    protocol = 'jsonrpc'

    #: Protocol version used in messages
    protocol_version = '2.0'

    #: Default choice for JSON Framing
    framing_cls = JSONFramingRFC7464

    def __init__(self, reader, writer, services=None, **options):
        """
        :param socket: Socket connected to the peer. (Anything behaving like
                       a socket and implementing socket methods ``close``,
                       ``recv``, ``sendall`` and ``shutdown`` is equally
                       viable)
        :type socket: socket.socket
        :param services: Object providing request handlers and
                         notification handlers to be exposed to peer.
                         See `Providing Services`_ for details.
        :type services: ``@service_class`` Class | ``None``
        :param options: Modify behavior by overriding the library defaults.

        **Available options:**

        **framing_cls**
          Selection of framing method implementation.
          Either select one of the following:

          * ``bsonrpc.JSONFramingNetstring``
          * ``bsonrpc.JSONFramingNone``
          * ``bsonrpc.JSONFramingRFC7464`` (Default)

          Or provide your own implementation class for some other framing type.
          See `bsonrpc.framing`_ for details.

        .. include:: options.snippet

        **custom_codec_implementation**
          Is by default ``None`` in which case this library uses python default
          json library. Otherwise an alternative json codec implementation can
          be provided as an object that must have callable attributes ``dumps``
          and ``loads`` with identical function signatures to those standard
          json library.

        All options as well as any possible custom/extra options are
        available as attributes of the constructed class object.
        """
        self.codec = MessageCodec.JSON
        if not services:
            services = DefaultServices()
        framing_cls = options.get('framing_cls', self.framing_cls)
        cci = options.get('custom_codec_implementation', None)
        super(JSONRpc, self).__init__(
            reader, writer,
            JSONCodec(
                framing_cls.extract_message, framing_cls.into_frame,
                custom_codec_implementation=cci
            ),
            services=services,
            **options
        )
