/**
  *  Copyright 2012-2013 Johns Hopkins University HLTCOE. All rights reserved.
  *  This software is released under the 2-clause BSD license.
  *  See LICENSE in the project root directory.
  */
package edu.jhu.rebar.scarbre.accumulo

import edu.jhu.hlt.concrete.Communication

import org.apache.accumulo.core.data.Mutation
import org.apache.accumulo.core.data.Value
import org.apache.accumulo.core.client.Connector

class BasicIngester(conn: Connector) extends AccumuloClient(conn) {
  def ingest(comm: Communication) = {

  }
}
