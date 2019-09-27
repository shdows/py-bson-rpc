# -*- coding: utf-8 -*-


"""
JSON & BSON codecs and the SocketQueue class which uses them.
"""


import abc
import asyncio
from struct import unpack

import bson
from aiobsonrpc.exceptions import (
    BsonRpcError, DecodingError, EncodingError, FramingError)


__license__ = 'http://mozilla.org/MPL/2.0/'


class Codec(abc.ABC):
    """
    Base Encode/Decode
    """
    @abc.abstractmethod
    def loads(self, b_msg):
        """may raise DecodingError"""
        pass

    @abc.abstractmethod
    def dumps(self, msg):
        """may raise EncodingError"""
        pass

    @abc.abstractmethod
    def extract_message(self, raw_bytes):
        pass

    @abc.abstractmethod
    def into_frame(self, message_bytes):
        pass


class BSONCodec(Codec):
    """
    Encode/Decode message to/from BSON format.

    Pros:
      * Explicit type for binary data
          * No string piggypacking.
          * No size penalties.
      * Explicit type for datetime.
    Cons:
      * No top-level arrays -> no batch support.
    """

    def __init__(self, custom_codec_implementation=None):
        if custom_codec_implementation is not None:
            self._loads = custom_codec_implementation.loads
            self._dumps = custom_codec_implementation.dumps
        else:
            # Use implementation from pymongo or from pybson
            if hasattr(bson, 'BSON'):
                # pymongo
                self._loads = lambda raw: bson.BSON.decode(bson.BSON(raw))
                self._dumps = lambda msg: bytes(bson.BSON.encode(msg))
            else:
                # pybson
                self._loads = bson.loads
                self._dumps = bson.dumps

    def loads(self, b_msg):
        try:
            return self._loads(b_msg)
        except Exception as e:
            raise DecodingError(e)

    def dumps(self, msg):
        try:
            return self._dumps(msg)
        except Exception as e:
            raise EncodingError(e)

    def extract_message(self, raw_bytes):
        rb_len = len(raw_bytes)
        if rb_len < 4:
            return None, raw_bytes
        try:
            msg_len = unpack('<i', raw_bytes[:4])[0]
            if msg_len < 5:
                raise FramingError('Minimum valid message length is 5.')
            if rb_len < msg_len:
                return None, raw_bytes
            else:
                return raw_bytes[:msg_len], raw_bytes[msg_len:]
        except Exception as e:
            raise FramingError(e)

    def into_frame(self, message_bytes):
        return message_bytes


class JSONCodec(Codec):
    """
    Encode/Decode messages to/from JSON format.
    """

    def __init__(self, extractor, framer, custom_codec_implementation=None):
        self._extractor = extractor
        self._framer = framer
        if custom_codec_implementation is not None:
            self._loads = custom_codec_implementation.loads
            self._dumps = custom_codec_implementation.dumps
        else:
            import json
            self._loads = json.loads
            self._dumps = json.dumps

    def loads(self, b_msg):
        try:
            return self._loads(b_msg.decode('utf-8'))
        except Exception as e:
            raise DecodingError(e)

    def dumps(self, msg):
        try:
            return self._dumps(msg,
                               separators=(',', ':'),
                               sort_keys=True).encode('utf-8')
        except Exception as e:
            raise EncodingError(e)

    def extract_message(self, raw_bytes):
        try:
            return self._extractor(raw_bytes)
        except Exception as e:
            raise FramingError(e)

    def into_frame(self, message_bytes):
        try:
            return self._framer(message_bytes)
        except Exception as e:
            raise FramingError(e)


class SocketQueue(object):
    """
    SocketQueue is a duplex Queue connected to a given socket and
    internally takes care of the conversion chain:

    python-data <-> queue-interface <-> codec <-> socket <-:net:-> peer node.
    """

    BUFSIZE = 4096

    def __init__(self, reader: asyncio.StreamReader,
                 writer: asyncio.StreamWriter,
                 codec: Codec, loop=None):
        """
        :param reader: asyncio stream reader to rpc peer node.
        :type reader: asyncio.StreamReader
        :param writer: asyncio stream writer to rpc peer node.
        :type writer: asyncio.StreamWriter
        :param codec: Codec converting python data to/from binary data
        :type codec: BSONCodec or JSONCodec
        :param loop: asyncio event loop
        :type loop: asyncio.AbstractEventLoop
        """
        self.loop = asyncio.get_event_loop() if loop is None else loop
        self._reader = reader
        self._writer = writer
        self.codec = codec
        self._queue = asyncio.Queue()
        self._receiver_task = self.loop.create_task(self._receiver())
        self._closed = False

    @property
    def is_closed(self):
        """
        :property: bool -- Closed by peer node or with ``close()``
        """
        return self._closed

    def close(self):
        """
        Close this queue and the underlying socket.
        """
        self._closed = True
        self._writer.close()

    async def put(self, item):
        """
        Put item to queue -> codec -> socket.

        :param item: Message object.
        :type item: dict, list or None
        """
        if self._closed:
            raise BsonRpcError('Attempt to put items to closed queue.')
        msg_bytes = self.codec.into_frame(self.codec.dumps(item))
        self._writer.write(msg_bytes)
        await self._writer.drain()

    async def get(self):
        """
        Get message items  <- codec <- socket.

        :returns: Normally a message object (python dict or list) but
                  if socket is closed by peer and queue is drained then
                  ``None`` is returned.
                  May also be Exception object in case of parsing or
                  framing errors.
        """
        return await self._queue.get()

    async def _to_queue(self, bbuffer):
        b_msg, bbuffer = self.codec.extract_message(bbuffer)
        while b_msg is not None:
            await self._queue.put(self.codec.loads(b_msg))
            b_msg, bbuffer = self.codec.extract_message(bbuffer)
        return bbuffer

    async def _receiver(self):
        bbuffer = b''
        while True:
            try:
                chunk = await self._reader.read(self.BUFSIZE)
                if chunk == b'':
                    break
                bbuffer = await self._to_queue(bbuffer + chunk)
            except DecodingError as e:
                await self._queue.put(e)
            except Exception as e:
                await self._queue.put(e)
                break
        self._closed = True
        await self._queue.put(None)

    async def join(self, timeout=None):
        """
        Wait for internal socket receiver thread to finish.
        """
        await asyncio.wait_for(self._receiver_task, timeout=timeout)
