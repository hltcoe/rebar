#!/usr/bin/python

#
# Copyright 2012-2013 Johns Hopkins University HLTCOE. All rights reserved.
# This software is released under the 2-clause BSD license.
# See LICENSE in the project root directory.
#

"""
This is a small script that demonstrates how to use a Python client to create a Rebar Corpus and get Rebar Corpora.

To use this script:
1) Generate thrift python files, e.g.,
thrift -gen py <path-to-communication.thrift>

2) Move the 'gen-py' folder one level above this script (or, alter the appended path appropriately).

3) Ensure the RebarCorpusServer is running on port 30001 on your machine (or, alter the code below).
Additionally, ensure that the RebarIngesterServer is running on port 30000.

4) Run the script:
python sample-corpus.py
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
    # set up corpus client
    transport = TSocket.TSocket('localhost', 30001)
    transport = TTransport.TBufferedTransport(transport)
    protocol = TBinaryProtocol.TBinaryProtocol(transport)

    cli = CorpusHandler.Client(protocol)
    transport.open()

    # NOTE: all corpora names must begin with "corpus_".
    print "Is corpus 'corpus_hello' available?"
    print cli.corpusExists("hello")

    cn = 'corpus_quxbarqux'
    print "Let's create a corpus: '{}'. First we need some document IDs; let's ingest them.".format(cn)
    # set up ingester client
    ing_xp = TSocket.TSocket('localhost', 30000)
    ing_xp = TTransport.TBufferedTransport(ing_xp)
    ing_proto = TBinaryProtocol.TBinaryProtocol(ing_xp)
    ing_cli = Ingester.Client(ing_proto)
    ing_xp.open()

    doc_ids = []
    for x in range(50, 60):
        comm = Communication()
        comm.id = "foo_{}".format(str(x))
        print "Ingesting {}...".format(comm.id)
        comm.type = DocType.OTHER
        comm.text = "hello world!"
        ing_cli.ingest(comm)
        doc_ids.append(comm)

    if cli.corpusExists(cn):
        print "Oops, it exists already: we will delete it."
        cli.deleteCorpus(cn)

    cli.createCorpus(cn, doc_ids)

    print "New corpus list:"
    for c in cli.listCorpora():
        print c

    print "Let's get the comms from this set."
    for c in list(cli.getCorpusCommunicationSet(cn)):
        print c

    print "Now, we'll delete it."
    cli.deleteCorpus(cn)

except Thrift.TException, tx:
    print "Got an error: %s : perhaps the server isn't running there?" % (tx.message)
