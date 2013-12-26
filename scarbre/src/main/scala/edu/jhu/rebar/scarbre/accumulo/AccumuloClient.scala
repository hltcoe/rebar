/**
  *  Copyright 2012-2013 Johns Hopkins University HLTCOE. All rights reserved.
  *  This software is released under the 2-clause BSD license.
  *  See LICENSE in the project root directory.
  */
package edu.jhu.rebar.scarbre
package accumulo

import edu.jhu.rebar.config.Configuration

abstract class AccumuloClient(conn: Connector) {
  protected val tableOps = new TableOps(conn)
  protected val bwOpts = new BatchWriterOpts

  bwOpts.batchLatency = Configuration.BWLatency
  bwOpts.batchMemory = Configuration.BWMemory
  bwOpts.batchThreads = Configuration.BWThreads
  bwOpts.batchTimeout = Configuration.BWTimeout
  protected val bwOptsCfg = bwOpts.getBatchWriterConfig

  protected val bw = conn.createBatchWriter(Configuration.DocumentTableName, bwOptsCfg)

}
