/**
  *  Copyright 2012-2014 Johns Hopkins University HLTCOE. All rights reserved.
  *  This software is released under the 2-clause BSD license.
  *  See LICENSE in the project root directory.
  */
package edu.jhu.rebar.scarbre
package accumulo

import edu.jhu.hlt.concrete._
import edu.jhu.rebar.config.Configuration

import com.twitter.scrooge.BinaryThriftStructSerializer

object Reader {
  private def getRoot (row: Map[Key, Value]) : Communication = {
    val rootEntry = row.par.find(entry =>
      entry._1.compareColumnFamily(new Text(Configuration.DocumentCF)) == 0)
    BinaryThriftStructSerializer(Communication).fromBytes(rootEntry.get._2.get)
  }

}
