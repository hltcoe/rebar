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

/**
  * A class that represents a collection of `Communication` objects ingested
  * into Rebar. These are user defined: e.g., for training and testing,
  * development, etc.
  *
  * @constructor pass in the Corpus name and the internal name.
  */
class Corpus(name: String, internalName: String) {
  import scala.collection.JavaConverters._
  import scala.collection.mutable.ArrayBuffer

  /**
    * An overloaded constructor that automatically creates the internalName field.
    *
    * @param name The name of the [[Corpus]].
    */
  def this(name:String) = this(name, Corpus.getInternalName(name))

  /**
    * Return a `Seq` of `Communication` objects that this [[Corpus]] object represents.
    */
  def getCommunicationSeq : Seq[Communication] = {
    // Create a buffer of `Range` objects to use for the main scanner.
    val rangeBuffer = new ArrayBuffer[Range]

    // Create scanner to access the corpus table.
    val scan = AccumuloClient.DefaultConnector.scanner(internalName)
    scan.setRange(new Range())
    val iter = scan.iterator
    iter.asScala.foreach { entry =>
      rangeBuffer += new Range(entry.getKey.getRow.toString)
    }

    // Create a buffer for the results (iterative).
    val resultBuffer = new ArrayBuffer[Communication]

    // Create a batch scanner to scan the communications table.
    AccumuloClient.DefaultConnector.withBatchScanner(Configuration DocumentTableName) { bsc =>
      bsc.setRanges(rangeBuffer.toSet.asJava)
      bsc.iterator.asScala.foreach { entry =>
        resultBuffer += BinaryThriftStructSerializer(Communication).fromBytes(entry.getValue.get)
      }
    }

    resultBuffer.toSeq
  }
}

/**
  * Factory and utility object for [[Corpus]].
  */
object Corpus {
  import scala.util.matching.Regex.Match
  import scala.util.{Try, Success, Failure}

  private val conn = AccumuloClient.DefaultConnector

  /**
    * Returns `True` if the corpus is a valid corpus name. This is determined by
    * the prefix specified by `Configuration`.`CorpusPrefix`.
    *
    * @param corpusName The name of the corpus to check.
    */
  def validCorpusName(corpusName: String) : Boolean = {
    AccumuloClient.AccumuloTableRE findAllMatchIn corpusName isEmpty
  }

  /**
    * Create a [[Corpus]] object in Rebar.
    *
    * @param name The name of the [[Corpus]] object to create.
    * @param commIDSet a `Set` of `Communication`s that this corpus will point to.
    */
  def create (name: String, commIDSet : Set[String]) : Try[Corpus] = {
    if (commIDSet.isEmpty)
      Failure(new IllegalArgumentException("Your commIDSet had zero document IDs."))

    if (exists(name))
      Failure(new IllegalArgumentException("This corpus already exists."))

    Try(createInternal(name, commIDSet))
  }

  private def createInternal(name: String, commIDSet: Set[String]) : Corpus = {
    val iName = getInternalName(name)

    // Add table to Corpus list table.
    conn.withBatchWriter(Configuration CorpusTableName) { bw =>
      val m = new Mutation(iName)
      m.putEmpty
      bw.addMutation(m)
    }

    // Create the corpus table itself.
    TableOps create iName

    // Add each comm ID to the corpus table.
    conn.withBatchWriter(iName) { bw =>
      commIDSet.foreach { id =>
        val m = new Mutation(id)
        m.putEmpty
        bw.addMutation(m)
      }
    }

    new Corpus(name, iName)
  }

  /**
    * Returns `True` if the corpus exists in Rebar.
    *
    * @param name The name of the [[Corpus]] to check.
    */
  def exists(name: String) : Boolean = {
    val internalName = getInternalName(name)
    val scan = AccumuloClient.DefaultConnector.scanner(Configuration CorpusTableName)
    val range = new Range(internalName)
    scan.setRange(range)
    scan.iterator().hasNext()
  }

  /**
    * Return the internal representation of this [[Corpus]] object's name.
    *
    * @param name The normal name of the [[Corpus]].
    */
  private def getInternalName(name: String) : String = {
    Configuration.CorpusPrefix.concat(name)
  }
}
