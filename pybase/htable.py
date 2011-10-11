#!/usr/bin/env python
# -*- coding: utf-8 -*-

from connection import *
from hbase.ttypes import *

__all__ = ['HTable']

class HTable(object):
    def __init__(self, client, tableName):
        self._client = client
        self._tableName = tableName

    def __str__(self):
        descr = "%s" % self._tableName
        return descr

    def _split_cf(self, key):
        if ':' in key:
            return key.split(':', 1)

        return key, None

    def _columns_to_tcolumn(self, columns, timestamp):
        if not columns:
            return None

        cols = []
        for key in columns:
            family, qualifier = self._split_cf(key)
            cols.append(TColumn(family=family,
                qualifier=qualifier, timestamp=timestamp))
        return cols

    def _column_dict_to_tcolumnvalues(self, colvals, timestamp=None):
        columns = []
        for k, v in colvals.iteritems():
            family, qualifier = self._split_cf(k)
            columns.append(TColumnValue(family=family,
                qualifier=qualifier, value=v, timestamp=timestamp))
        return columns

    def insert(self, key, put_values, timestamp=None):
        """
        Apply a series of mutations (inserts/updates) to a row in a
        single transaction.  If an exception is thrown, then the
        transaction is aborted.  Default current timestamp is used, and
        all entries will have an identical timestamp.

        @param key row key
        @param put_values list of puts, as a dict of column:value
            ex. put_values = {'person:name':'Antonio'}
        """

        return self._client.put(self._tableName, TPut(row=key,
            columnValues=self._column_dict_to_tcolumnvalues(put_values),
            timestamp=timestamp))

    def check_and_insert(self, check_key, check_column, check_value,
            put_values, timestamp=None):

        family, qualifier = check_column.split(':', 1)
        return self._client.checkAndPut(self._tableName, check_key, family,
            qualifier, check_value, TPut(row=check_key,
            columnValues=self._column_dict_to_tcolumnvalues(put_values),
            timestamp=timestamp))


    def _hrow_to_tuple(self, row, include_timestamp):
        """ Given a TResult, return the pair (key, {'column': value})
            If include_timestamp is True, each entry has the tuple
            (value, timestamp) instead of just value.
        """
        key = row.row
        cdict = {}
        for cell in row.columnValues:
            colname = ':'.join([cell.family, cell.qualifier])
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
        response = self._client.get(self._tableName,
            TGet(row=key, columns=self._columns_to_tcolumn(columns, timestamp),
                 timestamp=timestamp))

        if not response.row:
            return None
        return self._hrow_to_tuple(response, include_timestamp)[1]


    def get_range(self, start='', finish='', columns=None,
        include_timestamp=False):
        """
        Get a generator over rows in a specified key range.
        """
        buffer_size = 1024
        scanner = None

        tscan = TScan(startRow=start, stopRow=finish,
            columns=self._columns_to_tcolumn(columns, None))

        scanner = self._client.openScanner(self._tableName, tscan)

        while True:
            ret = self._client.getScannerRows(scanner, buffer_size)
            if not ret:
                break
            for item in ret:
                yield self._hrow_to_tuple(item, include_timestamp)
        self._client.closeScanner(scanner)

    def remove(self, key, columns=None, timestamp=None):
        """
        Apply a series of deletes to a row in a single transaction.

        Supply a list of column names to delete parts of a row.
        Otherwise, the whole row will be deleted.
        """
        self._client.deleteSingle(self._tableName,
            TDelete(row=key,
                    columns=self._columns_to_tcolumn(columns, timestamp),
                    timestamp=timestamp,
                    deleteType=TDeleteType.DELETE_COLUMNS))

