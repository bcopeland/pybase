#!/usr/bin/python
# -*- coding: utf-8 -*-
from pybase import *
from hbase.ttypes import *
import time

if __name__ == '__main__':
    import progressbar

    timestamp = int(time.time() * 1000)

    TABNAME = 'pycassahtable'
    KEYS=256
    client = connect_thread_local(['localhost:9090'])
    client.getTableNames()
    tab = HTable(client, TABNAME,
        [ColumnDescriptor(name='foo:'), ColumnDescriptor(name='foo2:')],
        createIfNotExist=True, overwrite=True)
    print "Table: ", tab

    widgets = ['Insert Values: ', progressbar.Percentage(), ' ', progressbar.Bar('>'), ' ', progressbar.ETA(), ' ', progressbar.FileTransferSpeed()]
    prog = progressbar.ProgressBar(widgets = widgets, maxval=KEYS).start()
    for i in range(KEYS):
        row_key = "%.8x" % i
        value = "%s" % i
        mods = {"foo:ciao":value, "foo:ciao2":value}
        tab.insert(row_key, mods, timestamp=timestamp)
        prog.update(i)

    print "Testing Scanner"

    start = "%.8x" % (int(KEYS / 2))
    end = "%.8x" % (int(3 * KEYS / 4))
    column_limit = ["foo:ciao"]

    print "Get single row %s" % start
    print tab.get(start, timestamp=timestamp)
    print

    print "Get single row %s, columns %s" % (start, column_limit)
    print tab.get(start, column_limit)
    print

    print "Scan all rows"
    for i in tab.get_range():
        print i
    print

    print "Scan starting from %s" % start
    for i in tab.get_range(start):
        print i
    print

    print "Scan starting from %s to %s" % (start, end)
    for i in tab.get_range(start, end):
        print i
    print

    print "Scan starting from %s to %s columns=%s" % (start, end, column_limit)
    for i in tab.get_range(start, end, columns=column_limit):
        print i
    print

    print "Scan starting from %s to %s columns=%s, ts=%s" % (start, end, column_limit, timestamp)
    for i in tab.get_range(start, end, columns=column_limit,
        timestamp=timestamp, include_timestamp=True):
        print i
    print

    print "TABLE REGIONS:"
    print tab.getTableRegions()

