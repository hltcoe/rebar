#!/usr/bin/python

#
# Copyright 2012-2013 Johns Hopkins University HLTCOE. All rights reserved.
# This software is released under the 2-clause BSD license.
# See LICENSE in the project root directory.
#

"""
This is a small script that demonstrates how to use a Python client to ingest Communications into rebar.

To use this script:
1) Generate thrift python files, e.g.,
thrift -gen py <path-to-communication.thrift>

2) Move the 'gen-py' folder one level above this script (or, alter the appended path appropriately).

3) Ensure the RebarIngesterService is running on port 9990 on your machine (or, alter the code below).

4) Run the script:
python sample-ingest.py
"""

import sys
sys.path.append("../gen-py")

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

from concrete.communication import *
from concrete.communication.ttypes import *

try:
    transport = TSocket.TSocket('localhost', 9990)
    transport = TTransport.TBufferedTransport(transport)
    protocol = TBinaryProtocol.TBinaryProtocol(transport)

    cli = Ingester.Client(protocol)
    transport.open()
    comm = Communication()
    comm.id = "foo"
    comm.type = DocType.OTHER
    comm.text = "hello world!"

    cli.ingest(comm)

except Thrift.TException, tx:
    print "Got an error: %s" % (tx.message)
