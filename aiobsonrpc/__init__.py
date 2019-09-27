# -*- coding: utf-8 -*-
"""
Library for JSON RPC 2.0 and BSON RPC
"""
from aiobsonrpc.exceptions import BsonRpcError
from aiobsonrpc.framing import (
    JSONFramingNetstring, JSONFramingNone, JSONFramingRFC7464)
from aiobsonrpc.interfaces import (
    notification, request, rpc_notification, rpc_request, service_class)
from aiobsonrpc.options import NoArgumentsPresentation, ThreadingModel
from aiobsonrpc.rpc import BSONRpc, JSONRpc
from aiobsonrpc.util import BatchBuilder


__version__ = '0.2.1'

__license__ = 'http://mozilla.org/MPL/2.0/'

__all__ = [
    'BSONRpc',
    'BatchBuilder',
    'BsonRpcError',
    'JSONFramingNetstring',
    'JSONFramingNone',
    'JSONFramingRFC7464',
    'JSONRpc',
    'NoArgumentsPresentation',
    'ThreadingModel',
    'notification',
    'request',
    'rpc_notification',
    'rpc_request',
    'service_class',
]
