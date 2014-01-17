/**
  *  Copyright 2012-2014 Johns Hopkins University HLTCOE. All rights reserved.
  *  This software is released under the 2-clause BSD license.
  *  See LICENSE in the project root directory.
  */
package edu.jhu.hlt.rebar
package accumulo

import edu.jhu.hlt.concrete._
import edu.jhu.hlt.rebar.Configuration

/**
  * A class that represents a basic Rebar "ingester", e.g., an
  * interface to ingest `Communication` objects without annotations.
  *
  * @param conn The `Connector` object to use to connect to Accumulo.
  */
class BasicIngester(implicit conn: Connector) {
  import scala.util.{Try, Success, Failure}

  if (!TableOps.exists(Configuration.DocumentTableName).getOrElse(false))
    TableOps.create(Configuration.DocumentTableName)

  lazy val bw = this.conn.createBatchWriter(Configuration.DocumentTableName, AccumuloClient.DefaultBWConfig)

  /**
    * Process a `Communication` via Accumulo.
    */
  private def processCommunication(comm : Communication) : Unit = {
    val m = new Mutation(comm.id)
    val v = new Value(CommunicationSerializer.toBytes(comm))

    m.put(Configuration.DocumentCF, "", v)
    this.bw.addMutation(m)
  }

  /**
    * Return a new `Communication` object with annotations removed.
    */
  private def trimCommunication (comm : Communication) : Communication = {
    val trimComm = new Communication(comm)

    trimComm.lid = null
    trimComm.sectionSegmentation = null
    trimComm.entityMentionSet = null
    trimComm.entitySet = null
    trimComm.situationMentionSet = null
    trimComm.situationSet = null

    trimComm
  }

  /**
    * Ingests a `Communication` object into Rebar.
    *
    * @param comm The `Communication` to ingest.
    */
  def ingest(comm: Communication) : Try[Unit] = {
    if (!comm.valid)
      Failure(new IllegalArgumentException("Your communication is invalid; check that all required fields are present."))
    else
      Try(processCommunication(trimCommunication(comm)))
    }

  /**
    * Close the underlying `BatchWriter` associated with this [[BasicIngester]].
    */
  def close = this.bw.close
}
