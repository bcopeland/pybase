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
                    self.dropTable(tableName)
                    self._client.createTable(tableName, columnFamiliesList)

    def dropTable(self, tabName):
        self._client.disableTable(self._tableName)
        self._client.deleteTable(self._tableName)

    def __str__(self):
        descr = "%s (" % self._tableName + ", ".join(["%s" % cf.name for cf in self._columnFamiliesList]) + ")"
        return descr

    def __repr__(self):
        return "%s", self._client.getColumnDescriptors(self._tableName)


    def insert(self, row, mutations):
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

        self._client.mutateRow(self._tableName, row, mutations)

    def insertTs(self, row, mutations, ts): # untested
        """
        Apply a series of mutations (updates/deletes) to a row in a
        single transaction.  If an exception is thrown, then the
        transaction is aborted. A timestamp value is required, and
        all entries will have an identical timestamp.

        @param row row key
        @param mutations list of mutation commands, as a dict of column:value
            ex. mutations = {'person:name':'Antonio'}
        @param ts timestamp
        """
        mutations = [Mutation(column=k, value=v) for (k,v) in mutations.iteritems()]
        self._client.mutateRowTs(self._tableName, row, mutations, ts)

    def _hrow_to_tuple(self, row):
        """ Given a TRowResult, return the pair (key, {'column': value})
            TODO: we currently discard the timestamp, we could optionally
            use 'column' => (value, timestamp) for that use case.
        """
        key = row.row
        cdict = {}
        for colname, cell in row.columns.iteritems():
            cdict[colname] = cell.value
        return (key, cdict)

    def get(self, key, columns=None):
        """
        Fetch all or part of a row with key `key`.

        A list of columns (or regexes) may be supplied to limit
        the return result to matching columns.  A single column
        name is of the form ``column_family:name``.  If columns is
        None (default), all columns are returned.

        The return result is of the form:
        ``{column_name: column_value}``, or None if no matches.
        """
        response = self._client.getRowWithColumns(
            self._tableName, key, columns)
        if not response:
            return None
        return self._hrow_to_tuple(response[0])[1]


    def get_range(self, start='', finish='', columns=None, timestamp=None):
        """
        Get a generator over rows in a specified key range.
        """
        buffer_size = 1024
        scanner = self._client.scannerOpenWithStop(self._tableName,
            start, finish, columns)
        while True:
            ret = self._client.scannerGetList(scanner, buffer_size)
            if not ret:
                break
            for item in ret:
                yield self._hrow_to_tuple(item)
        self._client.scannerClose(scanner)

    def scanner(self, startRow="", stopRow="", columnlist="", timestamp=None):
        '''
        Get a scanner on the current table starting at the specified row and
        ending at the last row in the table.  Return the specified columns.
        Only values with the specified timestamp are returned.
        @param startRow starting row in table to scan.  send "" (empty string) to
                start at the first row.
        @param stopRow row to stop scanning on.  This row is *not* included in the
                scanner's results
        @param columnList columns to scan. If column name is a column family, all
                columns of the specified column family are returned.  Its also possible
                to pass a regex in the column qualifier.
        @param timestamp timestamp

        @return scanner iterator
        '''
        if stopRow: #untested
            if timestamp: #untested
                scanner = self._client.scannerOpenWithStopTs(self._tableName, startRow, stopRow, columnlist, timestamp)
            else: #untested
                scanner = self._client.scannerOpenWithStop(self._tableName, startRow, stopRow, columnlist)
        elif timestamp: #untested
            scanner = self._client.scannerOpenTs(self._tableName, startRow, columnlist, timestamp)
        else: #tested
            scanner = self._client.scannerOpen(self._tableName, startRow, columnlist)

        def next(n=1):
            while True:
                ret = self._client.scannerGetList(scanner, n)
                if not ret:
                    break
                yield ret
            self._client.scannerClose(scanner)
        return next()

    def getTableRegions(self):
        return self._client.getTableRegions(self._tableName)

