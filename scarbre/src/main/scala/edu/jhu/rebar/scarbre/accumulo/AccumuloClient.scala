/**
  *  Copyright 2012-2013 Johns Hopkins University HLTCOE. All rights reserved.
  *  This software is released under the 2-clause BSD license.
  *  See LICENSE in the project root directory.
  */
package edu.jhu.rebar.scarbre
package accumulo

import edu.jhu.rebar.config.Configuration

abstract class AccumuloClient(conn: Connector) {
  protected lazy val bw = this.conn.createBatchWriter(Configuration.DocumentTableName, AccumuloClient.DefaultBWConfig)

  protected lazy val corporaTableBW = conn.createBatchWriter(Configuration.CorpusTableName, AccumuloClient.DefaultBWConfig)
}

/**
  * Small wrapper around `Connector` to simplify usage.
  */
class PowerConnector(conn: Connector) {
  def scanner(tableName: String)(implicit auths: Authorizations) : Scanner = {
    conn.createScanner(tableName, auths)
  }

  private def batchWriter(tableName: String)(implicit cfg: BatchWriterConfig) : BatchWriter = {
    conn.createBatchWriter(tableName, cfg)
  }

  private def batchScanner(tableName: String, threads: Int)(implicit auths: Authorizations) : BatchScanner = {
    conn.createBatchScanner(tableName, auths, threads)
  }

  def withBatchWriter(tableName: String)(fx : BatchWriter => Unit) : Unit = {
    val bw = batchWriter(tableName)
    try {
      fx(bw)
    } finally {
      bw.close()
    }
  }

  def withBatchScanner(tableName: String)(fx: BatchScanner => Unit) : Unit = {
    val bs = batchScanner(tableName, 8)
    try {
      fx(bs)
    } finally {
      bs.close()
    }
  }
}

/**
  * A mutable wrapper around Scanner that provides scala friendly results.
  */
class PowerScanner(scan: Scanner) {
  import scala.collection.JavaConverters._

  def withRange(range: Range) = scan.setRange(range)

  def getResults : Seq[Entry[Key, Value]] = scan.iterator().asScala.toSeq
}

class PowerMutation(m: Mutation) {
  def putEmpty = m.put("", "", new Value(new Array[Byte](0)))
}

/**
  * A "pimped" `String` class that facilitates naming of Accumulo tables.
  */
class AccumuloTableNameString(orig: String) {
  /**
    * Returns `True` if the `String` is a valid Accumulo table name.
    */
  def isValidTableName : Boolean = AccumuloClient.AccumuloTableRE findAllMatchIn orig isEmpty
}

/**
  * Contains Accumulo-specific constants and default values.
  */
object AccumuloClient {
  import scala.util.matching.Regex
  import scala.util.{Try, Success, Failure}

  /**
    * The default `Authorizations`.
    */
  val DefaultAuths = org.apache.accumulo.core.Constants.NO_AUTHS

  /**
    * The `PasswordToken` for this user.
    */
  val DefaultPasswordToken = new PasswordToken(Configuration.AccumuloPass)

  /**
    * The default system `Connector` object.
    */
  val DefaultConnector = getConnector

  /**
    * The `Regex` to use for Accumulo table names.
    */
  val AccumuloTableRE = """[^a-zA-Z0-9_]+""".r

  private val bwOpts = new BatchWriterOpts

  bwOpts.batchLatency = Configuration.BWLatency
  bwOpts.batchMemory = Configuration.BWMemory
  bwOpts.batchThreads = Configuration.BWThreads
  bwOpts.batchTimeout = Configuration.BWTimeout

  /**
    * The default `BatchWriterConfig` for `BatchWriter` objects.
    */
  val DefaultBWConfig = bwOpts.getBatchWriterConfig

  /**
    * An empty Accumulo `Value` object, backed by a byte array of length 0.
    */
  val EmptyValue = new Value(new Array[Byte](0))

  /**
    * Return a `Connector` object for use in rebar.
    *
    * @return a `Connector` to either an in-memory or configured Accumulo cluster, depending on the configuration.
    */
  private def getConnector() : Connector = {
    if (Configuration UseMock)
      new MockInstance().getConnector("max", new PasswordToken(""))
    else
      new ZooKeeperInstance(Configuration.AccumuloInstance, Configuration.Zookeepers).getConnector(Configuration.AccumuloUser, DefaultPasswordToken)
  }

  /**
    * A small wrapping function that ensures a valid Accumulo table name. This function should
    * only be called with Accumulo-table functions, but no way (currently) to enforce this.
    */
  def withAccumuloTableName(tableName: String)(fx: String => Any) : Try[Any] = {
    if (!tableName.isValidTableName)
      Failure(new IllegalArgumentException("Your table name contained an invalid character."))

    Try(fx(tableName))
  }
}
