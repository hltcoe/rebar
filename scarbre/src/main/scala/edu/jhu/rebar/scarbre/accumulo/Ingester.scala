/**
  *  Copyright 2012-2013 Johns Hopkins University HLTCOE. All rights reserved.
  *  This software is released under the 2-clause BSD license.
  *  See LICENSE in the project root directory.
  */
package edu.jhu.rebar.scarbre
package accumulo

import edu.jhu.hlt.concrete._
import edu.jhu.rebar.config.Configuration
import com.twitter.scrooge.BinaryThriftStructSerializer

/**
  * A class that represents a basic Rebar "ingester", e.g., an
  * interface to ingest `Communication` objects without annotations.
  *
  * @param conn The `Connector` object to use to connect to Accumulo.
  */
class BasicIngester(implicit conn: Connector) extends AccumuloClient(conn) {
  /**
    * Ingests a `Communication` object into Rebar.
    *
    * @param comm The `Communication` to ingest.
    */
  def ingest(comm: Communication) = {
    val m = new Mutation(comm.id)
    val v = new Value(BinaryThriftStructSerializer(Communication).toBytes(comm))
    m.put(Configuration.DocumentCF, "", v)
    this.bw.addMutation(m)
  }
}
