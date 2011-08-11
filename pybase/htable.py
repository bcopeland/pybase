#!/usr/bin/env python
# -*- coding: utf-8 -*-

from connection import *
from hbase.ttypes import *

__all__ = ['HTable']

#TODO: strip admin/user functions of HTable

class HTable(object):
    def __init__(self, client, tableName, columnFamiliesList=[], createIfNotExist=False, overwrite=False):
        self._client = client
        self._tableName = tableName
        self._columnFamiliesList = columnFamiliesList
        self._createIfNotExist = createIfNotExist
        self._overwrite = overwrite
        if createIfNotExist:
            try:
                self._client.createTable(tableName, columnFamiliesList)
            except AlreadyExists, tx:
                if not overwrite:
                    print "Thrift exception"
                    print '%s' % (tx.message)
                    print "Overwrite not setted"
                    #raise
                else:
                    self._client.disableTable(tableName)
                    self._client.deleteTable(tableName)
                    self._client.createTable(tableName, columnFamiliesList)

    def __str__(self):
        descr = "%s (" % self._tableName + ", ".join(["%s" % cf.name for cf in self._columnFamiliesList]) + ")"
        return descr

    def __repr__(self):
        return "%s", self._client.getColumnDescriptors(self._tableName)


    def insert(self, row, mutations, timestamp=None):
        """
        Apply a series of mutations (updates/deletes) to a row in a
        single transaction.  If an exception is thrown, then the
        transaction is aborted.  Default current timestamp is used, and
        all entries will have an identical timestamp.

        Setting the value for a column to ``None`` effectively deletes
        that column.

        @param row row key
        @param mutations list of mutation commands, as a dict of column:value
            ex. mutations = {'person:name':'Antonio'}
        """
        mutations = [Mutation(column=k, value=v, isDelete=(v==None)) \
            for (k,v) in mutations.iteritems()]

        if timestamp is not None:
            self._client.mutateRowTs(self._tableName, row, mutations,
                timestamp)
        else:
            self._client.mutateRow(self._tableName, row, mutations)

    def _hrow_to_tuple(self, row, include_timestamp):
        """ Given a TRowResult, return the pair (key, {'column': value})
            If include_timestamp is True, each entry has the tuple
            (value, timestamp) instead of just value.
        """
        key = row.row
        cdict = {}
        for colname, cell in row.columns.iteritems():
            if include_timestamp:
                value = (cell.value, cell.timestamp)
            else:
                value = cell.value
            cdict[colname] = value
        return (key, cdict)

    def get(self, key, columns=None, timestamp=None, include_timestamp=False):
        """
        Fetch all or part of a row with key `key`.

        A list of columns (or regexes) may be supplied to limit
        the return result to matching columns.  A single column
        name is of the form ``column_family:name``.  If columns is
        None (default), all columns are returned.

        The return result is of the form:
        ``{column_name: column_value}``, or None if no matches.
        """
        if timestamp is not None:
            response = self._client.getRowWithColumnsTs(
                self._tableName, key, columns, timestamp)
        else:
            response = self._client.getRowWithColumns(
                self._tableName, key, columns)
        if not response:
            return None
        return self._hrow_to_tuple(response[0], include_timestamp)[1]


    def get_range(self, start='', finish='', columns=None, timestamp=None,
        include_timestamp=False):
        """
        Get a generator over rows in a specified key range.
        """
        buffer_size = 1024
        scanner = None

        if timestamp is not None:
            scanner = self._client.scannerOpenWithStopTs(self._tableName,
                start, finish, columns, timestamp)
        else:
            scanner = self._client.scannerOpenWithStop(self._tableName,
                start, finish, columns)

        while True:
            ret = self._client.scannerGetList(scanner, buffer_size)
            if not ret:
                break
            for item in ret:
                yield self._hrow_to_tuple(item, include_timestamp)
        self._client.scannerClose(scanner)

    def remove(self, key):
        self._client.deleteAllRow(self._tableName, key)

    def getTableRegions(self):
        return self._client.getTableRegions(self._tableName)

