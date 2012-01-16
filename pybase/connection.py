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

__all__ = ['connect', 'connect_thread_local', 'NoServerAvailable']

DEFAULT_SERVER = 'localhost:9090'

class NoServerAvailable(Exception):
    pass

def create_client_transport(server, framed_transport, timeout):
    host, port = server.split(":")
    socket = TSocket.TSocket(host, int(port))
    if timeout is not None:
        socket.setTimeout(timeout*1000.0)
    if framed_transport:
        transport = TTransport.TFramedTransport(socket)
    else:
        transport = TTransport.TBufferedTransport(socket)
    protocol = TBinaryProtocol.TBinaryProtocolAccelerated(transport)
    client = THBaseService.Client(protocol)
    transport.open()

    return client, transport

def connect(servers=None, framed_transport=False, timeout=None,
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
    return Connection(servers, framed_transport, timeout)

def connect_thread_local(*args, **kwargs):
    return connect(*args, **kwargs)

class Connection(object):
    def __init__(self, servers, framed_transport, timeout):
        self._servers = servers
        random.shuffle(self._servers)
        self._queue = Queue()
        for i in xrange(len(servers)):
            self._queue.put(i)
        self._local = threading.local()
        self._local.client = None
        self._framed_transport = framed_transport
        self._timeout = timeout

    def __getattr__(self, attr):
        print "tlc getattr %s" % attr
        def client_call(*args, **kwargs):
            if self._local.client is None:
                self._find_server()

            try:
                return getattr(self._local.client, attr)(*args, **kwargs)
            except (Thrift.TException, socket.timeout, socket.error), exc:
                # Connection error, try to connect to all the servers
                self._local.transport.close()
                self._local.client = None

                servers = self._rotate_servers()

                for server in servers:
                    try:
                        self._local.client, self._local.transport = create_client_transport(server, self._framed_transport, self._timeout)
                        return getattr(self._local.client, attr)(*args, **kwargs)
                    except (Thrift.TException, socket.timeout, socket.error), exc:
                        continue
                self._local.client = None
                raise

        setattr(self, attr, client_call)
        return getattr(self, attr)
    
    def close(self):
        if self._local.client:
            self._local.transport.close()
            self._local.client = None


    def _rotate_servers(self):
        servers = self._servers
        i = self._queue.get()
        self._queue.put(i)
        servers = servers[i:]+servers[:i]

        return servers

    def _find_server(self):
        servers = self._rotate_servers()

        for server in servers:
            try:
                self._local.client, self._local.transport = create_client_transport(server, self._framed_transport, self._timeout)
                return
            except (Thrift.TException, socket.timeout, socket.error), exc:
                print "cannot contact %s" % server
                continue
        self._local.client = None
        raise NoServerAvailable()

