/**
 * 
 */
package edu.jhu.rebar.config

import com.typesafe.config.{Config, ConfigFactory}

/**
  * A utility object that represents the configuration of the Rebar system. 
  *
  * @constructor Load a non-default `Config` object.
  * @param config The `Config` object to use to construct this [[Configuration]] object.
  */
class Configuration (config: Config) {
  config.checkValid(ConfigFactory.defaultReference(), "rebar")
  
  def this() = this(ConfigFactory.load())
  
  val accumuloConfig = config getConfig "rebar.accumulo"

  /**
    * Return a boolean - whether or not to use a mock instance of Accumulo.
    */
  def useMock = accumuloConfig getBoolean "useMock"

  /**
    * Return a string that represents the Accumulo instance that Rebar will use.
    */
  def accumuloInstance = accumuloConfig getString "instance"

  /**
    * Return a string that represents the Zookeeper servers that Rebar will use.
    */
  def zookeepers = accumuloConfig getString "zookeepers"

  /**
    * Return a string that represents the Accumulo user that Rebar will use.
    */
  def accumuloUser = accumuloConfig getString "user"

  /**
    * Return a string that represents the password for the Accumulo user.
    */
  def accumuloPass = accumuloConfig getString "password"
  
  val redisConfig = config getConfig "rebar.redis"

  /**
    * Return a string that represents the Redis server that Rebar will use.
    */
  def redisServer = redisConfig getString "server"

  /**
    * Return a string that represents the key in Redis that Rebar will use
    * for ingested document IDs.
    */
  def ingestedIdKey = redisConfig getString "ingested-id-key"

  val rebarConfig = config getConfig "rebar"

  /**
    * Return a `String` that indicates the name of the table that Rebar will store raw documents in.
    */
  def documentTableName = rebarConfig getString "document-table-name"

  /**
    * Return a `String` that Rebar will use as a column family to indicate a value is a raw document.
    */
  def documentCF = rebarConfig getString "document-cf"

  /**
    * Return a `String` that Rebar will use as a column family for Rebar annotations.
    */
  def documentAnnotationCF = rebarConfig getString "document-annotation-cf"

  /**
    * Return a `String` that Rebar will use as a table name for corpora.
    */
  def corpusTableName = rebarConfig getString "corpus-table-name"

  /**
    * Return a `String` that Rebar will use as a prefix for created corpora.
    */

  def corpusPrefix = rebarConfig getString "corpus-prefix"

  /**
    * Return a `String` that Rebar will use as a table name for `Stage`s.
    */
def stagesTableName = rebarConfig getString "stages-table-name"

  /**
    * Return a `String` that Rebar will use as a prefix for created `Stage`s.
    */
  def stagesPrefix = rebarConfig getString "stages-prefix"

  /**
    * Return a `String` that Rebar will use as a column family to represent actual stages in the stages table.
    */
  def stagesObjectCF = rebarConfig getString "stages-object-cf"

  /**
    * Return a `String` that Rebar will use as a column family for stages in annotated documents.
    */
  def stagesDocumentsCF = rebarConfig getString "stages-docs-cf"

  /**
    * Return a `String` that Rebar will use as a column family for document IDs that have been annotated in the stages table.
    */
  def stagesDocumentsAnnotationIdCF = rebarConfig getString "stages-docs-annotation-ids-cf"
}



