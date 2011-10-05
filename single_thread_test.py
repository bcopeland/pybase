#!/usr/bin/python
# -*- coding: utf-8 -*-
from pybase import *
from hbase.ttypes import *
from nose.tools import *
import time

"""
Before running this test, create a suitable table in hbase, like so:

$ hbase shell
hbase> create 'pybase-test', 'person'

"""

if __name__ == '__main__':

    timestamp = int(time.time() * 1000)

    keys=256
    first_key = 10

    client = connect_thread_local(['localhost:9190'])

    person = HTable(client, 'pybase-test')

    # some basic tests
    for i in range(0, first_key-1):
        person.remove('%5d' % i)

    # insert
    d = {'person:name': 'Joe User'}
    person.insert('00001', d)
    x = person.get('00001')
    ok_(x, d)

    # update a row with new column
    d2 = {'person:dob': '01/10/1970'}
    person.insert('00001', d2)
    x = person.get('00001')

    d = (d.items() + d2.items())
    ok_(x, d)

    # get a range
    person.insert('00002', {'person:name': 'A. Guy'})
    person.insert('00004', {'person:name': 'Frank Franklestein'})
    for i in person.get_range('00001', '00003'):
        print i

    print "Inserting %d keys..." % keys
    for i in range(first_key, keys + first_key):
        row_key = "%.5d" % i
        value = "%s" % i
        mods = {"person:x1": value, "person:x2": value}
        person.insert(row_key, mods, timestamp=timestamp)

    print "Testing Scanner"

    start = "%.5d" % (first_key + int(keys / 2))
    end = "%.5d" % (int(3 * keys / 4))
    column_limit = ["person:x1"]

    print "Get single row %s" % start
    print person.get(start, timestamp=timestamp)
    print

    print "Get single row %s, columns %s" % (start, column_limit)
    print person.get(start, column_limit)
    print

    print "Scan all rows"
    for i in person.get_range():
        print i
    print

    print "Scan starting from %s" % start
    for i in person.get_range(start):
        print i
    print

    print "Scan starting from %s to %s" % (start, end)
    for i in person.get_range(start, end):
        print i
    print

    print "Scan starting from %s to %s columns=%s" % (start, end, column_limit)
    for i in person.get_range(start, end, columns=column_limit):
        print i
    print

    print "Scan starting from %s to %s columns=%s, ts=%s" % (start, end, column_limit, timestamp)
    for i in person.get_range(start, end, columns=column_limit,
        include_timestamp=True):
        print i
    print

