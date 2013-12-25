/**
  *  Copyright 2012-2013 Johns Hopkins University HLTCOE. All rights reserved.
  *  This software is released under the 2-clause BSD license.
  *  See LICENSE in the project root directory.
  */
package edu.jhu.rebar.scarbre.accumulo

import org.apache.accumulo.core.client._
import org.apache.accumulo.core.cli.BatchWriterOpts

import org.apache.thrift.{TDeserializer, TSerializer}
import org.apache.thrift.protocol.TBinaryProtocol

import edu.jhu.rebar.config.Configuration

abstract class AccumuloClient(conn: Connector) {
  implicit val cfg = new Configuration

  protected val tableOps = new TableOps(conn)
  protected val bwOpts = new BatchWriterOpts
  bwOpts.batchLatency = 1024L * 1024 * 5
  bwOpts.batchMemory = 5L
  bwOpts.batchThreads = 4
  bwOpts.batchTimeout = 3000L;
  protected val bwOptsCfg = bwOpts.getBatchWriterConfig

  protected val serializer = new TSerializer(new TBinaryProtocol.Factory())
  protected val deserializer = new TDeserializer(new TBinaryProtocol.Factory())

  protected val bw = conn.createBatchWriter(cfg.documentTableName, bwOptsCfg)
}
