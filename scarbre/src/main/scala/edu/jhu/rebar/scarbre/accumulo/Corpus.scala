/**
  *  Copyright 2012-2013 Johns Hopkins University HLTCOE. All rights reserved.
  *  This software is released under the 2-clause BSD license.
  *  See LICENSE in the project root directory.
  */
package edu.jhu.rebar.scarbre
package accumulo

import edu.jhu.hlt.concrete._
import edu.jhu.rebar.config.Configuration

/**
  * A class that represents a collection of `Communication` objects ingested into Rebar. These are user defined: e.g., for training and testing, development, etc. 
  * 
  * @constructor pass in the Corpus name. 
  */
class Corpus(name: String) {
  private val internalName = "corpus_".concat(name)
  private val conn = AccumuloClient.DefaultConnector

  /**
    * Returns `True` if the corpus exists in Rebar (specifically, in Accumulo backing Rebar), otherwise `False`.
    * 
    * @param corpusName The name of the corpus to check. 
    */
  def exists : Boolean = {
    val scan = conn.scanner(Configuration.CorpusTableName)
    val range = new Range(internalName)
    scan.setRange(range)
    scan.iterator().hasNext()
  }
}

/**
  * Factory / utility object for [[Corpus]].
  */
object Corpus {
  /**
    * Returns `True` if the corpus is a valid corpus name, e.g., starts with the prefix specified by `Configuration`.`CorpusPrefix`.
    * 
    * @param corpusName The name of the corpus to check. 
    */
  def validCorpusName(corpusName: String) : Boolean = {
    corpusName.startsWith(Configuration.CorpusPrefix)
  }
}
