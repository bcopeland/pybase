# -*- coding: utf-8 -*-
from exceptions import Exception
import socket
import threading
import random
from Queue import Queue

try:
    import pkg_resources
    pkg_resources.require('Thrift')
except ImportError:
    pass
from thrift import Thrift
from thrift.transport import TTransport
from thrift.transport import TSocket
from thrift.protocol import TBinaryProtocol

from hbase import THBaseService

__all__ = ['connect', 'connect_thread_local']

DEFAULT_SERVER = 'localhost:9090'

class Connection(THBaseService.Client):
    def __init__(self, server, framed_transport=True, timeout=None):
        self.server = server
        self.framed_transport = framed_transport
        self.timeout = timeout

        server = server.split(":")
        if len(server) <= 1:
            port = 9090
        else:
            port = server[1]
        host = server[0]

        socket = TSocket.TSocket(host, int(port))
        if timeout is not None:
            socket.setTimeout(timeout*1000.0)
        if framed_transport:
            self.transport = TTransport.TFramedTransport(socket)
        else:
            self.transport = TTransport.TBufferedTransport(socket)
        protocol = TBinaryProtocol.TBinaryProtocolAccelerated(self.transport)
        super(Connection, self).__init__(protocol)
        self.transport.open()

    def close(self):
        if self.transport:
            self.transport.close()

import pool

def connect(servers=None, framed_transport=False, timeout=0.5,
            use_threadlocal=True):
    """
    Constructs a single HBase connection. Initially connects to the first
    server on the list.
    
    If the connection fails, it will attempt to connect to each server on the
    list in turn until one succeeds. If it is unable to find an active server,
    it will throw a NoServerAvailable exception.

    Parameters
    ----------
    servers : [server]
              List of HBase servers with format: "hostname:port"

              Default: ['localhost:9160']
    framed_transport: bool
              If True, use a TFramedTransport instead of a TBufferedTransport
    timeout: float
              Timeout in seconds (e.g. 0.5)

              Default: None (it will stall forever)

    Returns
    -------
    HBase client
    """

    if servers is None:
        servers = [DEFAULT_SERVER]
    return pool.ConnectionPool(server_list=servers,
                               framed_transport=framed_transport,
                               timeout=timeout,
                               use_threadlocal=use_threadlocal, prefill=False,
                               pool_size=len(servers),
                               max_overflow=len(servers),
                               max_retries=len(servers))

def connect_thread_local(*args, **kwargs):
    return connect(*args, **kwargs)

