/**
  *  Copyright 2012-2013 Johns Hopkins University HLTCOE. All rights reserved.
  *  This software is released under the 2-clause BSD license.
  *  See LICENSE in the project root directory.
  */
package edu.jhu.rebar.scarbre
package accumulo

import edu.jhu.hlt.concrete._

class BasicIngester(conn: Connector) extends AccumuloClient(conn) {
  def ingest(comm: Communication) = {

  }
}
