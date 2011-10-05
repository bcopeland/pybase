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

This has been updated to use the thrift2 interface, which is still
in.  The thrift2 interface is much better than the original and closer
to the API that Java provides.  As such, pybase really only provides
some syntatic sugar over using the thrift API directly (for example,
thrift classes are hidden from the user).  When in doubt, I have chosen
to name functions the same or similar to corresponding features in
pycassa.

Earlier versions of pybase would treat updates to 'None' as deletes.
This was because the Mutations interface supported sending these at
the same time.  Unfortunately, HBase does not really support atomic
inserts and deletes of a row in the same update, and the newer API
does not provide a way to pretend to do so.  Thus, I have reinstated
'None' inserts and added a columns parameter to remove to enable
column removal.  If you want atomic deletes, you should nominate some
value as a sentinel and treat columns with that value as dead until
your real delete can run.
