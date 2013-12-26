/**
  *  Copyright 2012-2013 Johns Hopkins University HLTCOE. All rights reserved.
  *  This software is released under the 2-clause BSD license.
  *  See LICENSE in the project root directory.
  */
package edu.jhu.rebar {
  package object scarbre {    
    type TBinaryProtocol = org.apache.thrift.protocol.TBinaryProtocol
    type TSerializer = org.apache.thrift.TSerializer
    type TDeserializer = org.apache.thrift.TDeserializer

    // def DefaultSerializer = new TSerializer(new TBinaryProtocol.Factory())
    // def DefaultDeserializer = new TDeserializer(new TBinaryProtocol.Factory())
  }
}


