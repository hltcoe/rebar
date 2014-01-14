/**
  *
  */
package edu.jhu.hlt.rebar

import com.typesafe.config.{Config, ConfigFactory}

/**
  * A utility object that represents the configuration of the Rebar system.
  *
  */
object Configuration {
  private val config = ConfigFactory.load()
  config.checkValid(ConfigFactory.defaultReference(), "rebar")

  //////////////////////////////////
  // Accumulo Configuration
  //////////////////////////////////
  private val AccumuloConfig = config getConfig "rebar.accumulo"

  /**
    * A boolean constant - whether or not to use a mock instance of Accumulo.
    */
  val UseMock = AccumuloConfig getBoolean "useMock"

  /**
    * A string that represents the Accumulo instance that Rebar will use.
    */
  val AccumuloInstance = AccumuloConfig getString "instance"

  /**
    * A string that represents the Zookeeper servers that Rebar will use.
    */
  val Zookeepers = AccumuloConfig getString "zookeepers"

  /**
    * A string that represents the Accumulo user that Rebar will use.
    */
  val AccumuloUser = AccumuloConfig getString "user"

  /**
    * A string that represents the password for the Accumulo user.
    */
  val AccumuloPass = AccumuloConfig getString "password"

  private val BatchConfig = AccumuloConfig getConfig "batchwriter"

  /**
    * The batchwriter latency, a `Long`.
    */
  val BWLatency = BatchConfig getLong "latency"

  /**
    * An `Int` that represents the amount of memory the batchwriter should use.
    */
  val BWMemory = BatchConfig getInt "memory"

  /**
    * An `Int` that represents the number of writer threads to use on
    * the Accumulo server by the batchwriter object.
    */
  val BWThreads = BatchConfig getInt "threads"

  /**
    * An `Int` that represents the timeout the batchwriter should use
    * when communicating with the server.
    */
  val BWTimeout = BatchConfig getInt "timeout"

  /////////////////////////
  // Redis Configuration
  /////////////////////////
  private val RedisConfig = config getConfig "rebar.redis"

  /**
    * A string that represents the Redis server that Rebar will use.
    */
  val RedisServer = RedisConfig getString "server"

  /**
    * A string that represents the key in Redis that Rebar will use
    * for ingested document IDs.
    */
  val IngestedIdKey = RedisConfig getString "ingested-id-key"

  //////////////////////////////////
  // General Rebar Configuration
  //////////////////////////////////
  private val RebarConfig = config getConfig "rebar"

  /**
    * A `String` that indicates the name of the table that Rebar will
    * store raw documents in.
    */
  val DocumentTableName = RebarConfig getString "document-table-name"

  /**
    * A `String` that Rebar will use as a column family to indicate a
    * value is a raw document.
    */
  val DocumentCF = RebarConfig getString "document-cf"

  /**
    * A `String` that Rebar will use as a column family for Rebar annotations.
    */
  val DocumentAnnotationCF = RebarConfig getString "document-annotation-cf"

  /**
    * A `String` that Rebar will use as a table name for corpora.
    */
  val CorpusTableName = RebarConfig getString "corpus-table-name"

  /**
    * A `String` that Rebar will use as a prefix for created corpora.
    */

  val CorpusPrefix = RebarConfig getString "corpus-prefix"

  /**
    * A `String` that Rebar will use as a table name for `Stage`s.
    */
  val StagesTableName = RebarConfig getString "stages-table-name"

  /**
    * A `String` that Rebar will use as a prefix for created `Stage`s.
    */
  val StagesPrefix = RebarConfig getString "stages-prefix"

  /**
    * A `String` that Rebar will use as a column family to represent
    * actual stages in the stages table.
    */
  val StagesObjectCF = RebarConfig getString "stages-object-cf"

  /**
    * A `String` that Rebar will use as a column family for stages in
    * annotated documents.
    */
  val StagesDocumentsCF = RebarConfig getString "stages-docs-cf"

  /**
    * A `String` that Rebar will use as a column family for document
    * IDs that have been annotated in the stages table.
    */
  val StagesDocumentsAnnotationIdCF = RebarConfig getString "stages-docs-annotation-ids-cf"
}
