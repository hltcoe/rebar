/**
  *  Copyright 2012-2013 Johns Hopkins University HLTCOE. All rights reserved.
  *  This software is released under the 2-clause BSD license.
  *  See LICENSE in the project root directory.
  */
package edu.jhu.rebar.scarbre
package accumulo

import edu.jhu.hlt.concrete._
import edu.jhu.rebar.config.Configuration

class CorpusHandler(conn: Connector) extends AccumuloClient(conn) {
  /**
    * Returns `True` if the corpus exists in Rebar (specifically, in Accumulo backing Rebar), otherwise `False`.
    * 
    * @param corpusName The name of the corpus to check. 
    */
  def corpusExists (corpusName : String) : Boolean = {
    val scan = conn.createScanner(Configuration.CorpusTableName, AccumuloClient.DefaultAuths)
    val range = new Range(corpusName)
    scan.setRange(range)
    scan.iterator().hasNext()
  }

  /**
    * Returns `True` if the corpus is a valid corpus name, e.g., starts with the prefix specified by `Configuration`.`CorpusPrefix`.
    * 
    * @param corpusName The name of the corpus to check. 
    */
  def validCorpusName(corpusName: String) : Boolean = {
    corpusName.startsWith(Configuration.CorpusPrefix)
  }
}
