"""
Connection pooling for HBase connections.

Based on code originally from pycassa and released under MIT license.
"""
import time, threading, random

import connection

__all__ = ['ConnectionPool',
           'ConnectionWrapper']

class ConnectionPool(object):

    def __init__(self,
        server_list=['localhost:9090'],
        timeout=0.5,
        use_threadlocal=True,
        pool_size=5,
        framed_transport=False,
        max_retries=5):

        self.set_server_list(server_list)
        self.timeout = timeout
        self._pool_threadlocal = use_threadlocal
        self._pool_size = pool_size
        self.framed_transport = framed_transport
        self._max_retries = max_retries

    def set_server_list(self, server_list):
        """
        Sets the server list that the pool will make connections to.

        `server_list` should be sequence of servers in the form 'host:port' that
        the pool will connect to.  The list will be randomly permuted before
        being used. `server_list` may also be a function that returns the
        sequence of servers.

        """

        if callable(server_list):
            self.server_list = list(server_list())
        else:
            self.server_list = list(server_list)

        assert len(self.server_list) > 0

        # Randomly permute the array (trust me, it's uniformly random)
        n = len(self.server_list)
        for i in range(0, n):
            j = random.randint(i, n-1)
            temp = self.server_list[j]
            self.server_list[j] = self.server_list[i]
            self.server_list[i] = temp

        self._list_position = 0
        # self._notify_on_server_list(self.server_list)

    def get_conn(self):
        """ Gets a connection from the pool. """
        return ConnectionWrapper(self, self._max_retries,
            self.server_list, self.framed_transport, self.timeout)

    def return_conn(self, conn):
        conn.close()

    def __getattr__(self, attr):
        """ temporarily chain pool.xyz -> connectionwrapper.xyz() ->
            connection.xyz()
        """
        def callup(*args, **kwargs):
            conn = self.get_conn()
            try:
                result = getattr(conn, attr)(*args, **kwargs)
                return result
            finally:
                conn.return_to_pool()

        setattr(self, attr, callup)
        return getattr(self, attr)

class ConnectionWrapper(connection.ThreadLocalConnection):
    """
    A wrapper class for :class:`connection.Connection`s that adds pooling
    functionality.

    """
    def __init__(self, pool, max_retries, *args, **kwargs):
        self._pool = pool
        self._retry_count = 0
        self._max_retries = max_retries
        super(ConnectionWrapper, self).__init__(*args, **kwargs)

    def return_to_pool(self):
        self._pool.return_conn(self)

    def _retry(f):
        def new_f(self, *args, **kwargs):
            try:
                conn = super(ConnectionWrapper, self)
                fn = conn.__getattr__(f.__name__)
                result = fn(*args, **kwargs)
                self._retry_count = 0
                return result

            except:
                raise

        new_f.__name__ = f.__name__
        return new_f

    @_retry
    def put(self, *args, **kwargs):
        pass

    @_retry
    def checkAndPut(self, *args, **kwargs):
        pass

    @_retry
    def get(self, *args, **kwargs):
        pass

    @_retry
    def openScanner(self, *args, **kwargs):
        pass

    @_retry
    def getScannerRows(self, *args, **kwargs):
        pass

    @_retry
    def closeScanner(self, *args, **kwargs):
        pass

    @_retry
    def deleteSingle(self, *args, **kwargs):
        pass

    @_retry
    def checkAndDelete(self, *args, **kwargs):
        pass
