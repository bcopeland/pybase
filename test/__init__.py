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

hbase_conn = None
person = None
test_keys=16


def setup():
    global hbase_conn, person

    hbase_conn = connect_thread_local(['localhost:9090'])
    person = HTable(hbase_conn, 'pybase-test')

    for i in range(0, test_keys):
        person.remove('%.5d' % i)

def teardown():
    hbase_conn.close()

def test_insert():
    d = {'person:name': 'Joe User'}
    person.insert('00001', d)
    x = person.get('00001')
    eq_(x, d)
    person.remove('00001') 

def test_update():
    d = {'person:name': 'Joe User'}
    person.insert('00001', d)

    d2 = {'person:dob': '01/10/1970'}
    person.insert('00001', d2)
    x = person.get('00001')

    d = dict(d.items() + d2.items())
    eq_(x, d)
    person.remove('00001')

def test_remove():
    d = {'person:name': 'Joe User'}
    person.insert('00001', d)
    person.remove('00001')
    eq_(person.get('00001'), None)

def test_check_and_put():
    d = {'person:name': 'Joe User', 'person:age': '1'}
    x = person.check_and_insert('00001', 'person:name', None, d)
    eq_(x, True)

    x = person.check_and_insert('00001', 'person:name', None,
        {'person:dob': '1990-01-01'})
    eq_(x, False)
    person.remove('00001')

def test_check_and_remove():
    d = {'person:name': 'Joe User', 'person:age': '1'}
    person.insert('00001', d)
    x = person.check_and_remove('00001', 'person:name', 'Joe User',
        ['person:age'])
    eq_(x, True)

    eq_(person.get('00001').get('person:name'), 'Joe User')
    eq_(person.get('00001').get('person:age'), None)
    x = person.check_and_remove('00001', 'person:age', '1', ['person:age'])
    eq_(x, False)
    person.remove('00001')

def test_get_range():
    d = {'person:name': 'Joe User'}
    d2 = {'person:name': 'A. Guy'}
    d4 = {'person:name': 'Frank Franklestein'}

    person.insert('00001', d)
    person.insert('00002', d2)
    person.insert('00004', d4)

    expected = [ ('00001', d), ('00002', d2) ]
    x = person.get_range('00001', '00003')
    eq_(list(x), expected)
    person.remove('00001')
    person.remove('00002')
    person.remove('00004')
    
