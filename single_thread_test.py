# -*- coding: utf-8 -*-
from pybase import *
from hbase.ttypes import *

if __name__ == '__main__':
    import progressbar

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
        tab.insert(row_key, mods)
        prog.update(i)

    print "Testing Scanner"

    start = "%.8x" % (int(KEYS / 2))
    end = "%.8x" % (int(3 * KEYS / 4))

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

    print "Scan starting from %s to %s column='foo:ciao'" % (start, end)
    for i in tab.get_range(start, end, columns=['foo:ciao']):
        print i
    print

    print "TABLE REGIONS:"
    print tab.getTableRegions()

