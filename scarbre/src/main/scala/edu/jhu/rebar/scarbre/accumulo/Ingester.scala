/**
  *  Copyright 2012-2013 Johns Hopkins University HLTCOE. All rights reserved.
  *  This software is released under the 2-clause BSD license.
  *  See LICENSE in the project root directory.
  */
package edu.jhu.rebar.scarbre
package accumulo

import edu.jhu.hlt.concrete._
import edu.jhu.rebar.config.Configuration

class BasicIngester(conn: Connector) extends AccumuloClient(conn) {
  private val bw = this.conn.createBatchWriter(Configuration.DocumentTableName, bwOptsCfg)

  def ingest(comm: Communication) = {
    val m = new Mutation(comm.id)
    val v = new Value(this.serializer.serialize(comm))
    m.put(Configuration.DocumentCF, "", v)
    this.bw.addMutation(m)
  }
}
