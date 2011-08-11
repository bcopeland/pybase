pybase
=======

pybase is a python library for HBase, based on Pycassa
(http://github.com/pycassa/pycassa), a python library for Cassandra.

This is a fork from the original project by Antonio Barbuzzi, available at:
https://github.com/antoniobarbuzzi/pybase

Requirements
------------

    thrift: http://incubator.apache.org/thrift/
    HBase: http://hbase.apache.org

To install thrift's python bindings:

    easy_install thrift

pybase comes with the HBase python files for convenience, but you can replace
them with your own.

Installation
------------

Just copy the directory in your program, or in your $PYTHONPATH.

Basic Usage
----------

To get a connection:

    >>> import pybase
    >>> server_list = ['localhost:9090']
    >>> client = pybase.connect_thread_local(server_list)

Create an instance of the table you wish to modify, and use
insert() to add new rows and get() to retrieve rows:

    >>> person = pybase.HTable(client, 'person')
    >>> person.insert('00001', {'person:name': 'Joe User'})
    >>> person.get('00001')
    {'person:name': 'Joe User'}

insert() will also update existing rows:

    >>> person.insert('00001', {'person:dob': '01/10/1970'})
    >>> person.get('00001')
    {'person:name': 'Joe User', 'person:dob': '01/10/1970'}

Use get_range() for range queries:

    >>> person.insert('00002', {'person:name': 'A. Guy'})
    >>> person.insert('00004', {'person:name': 'Frank Franklestein'})
    >>> for i in person.get_range('00001','00003'):
    ...     print i
    ('00001', {'person:name': 'Joe User', 'person:dob': '01/10/1970'})
    ('00002', {'person:name': 'A. Guy'})


Implementation Notes
--------------------

The current thrift interface is the original HBase thrift interface.
However, a new thrift interface (thrift2) is currently being designed
in HBase.  I will likely shift to that when generally available.

HBase thrift has an interesting interpretation of timestamp parameters
on reads: they are used as the upper limit of range searches, but this
limit is applied exclusively in HBase so you will get all rows less
than the supplied timestamp.

I feel that this implementation is useless for my purposes, so I modified
the HBase thrift server to use setTimestamp() instead of setTimeRange() in
all the cases I care about.  Do note that you may get something unexpected
if you use this parameter; my intended interpretation is the result with
my modifications.

