/**
  *  Copyright 2012-2013 Johns Hopkins University HLTCOE. All rights reserved.
  *  This software is released under the 2-clause BSD license.
  *  See LICENSE in the project root directory.
  */
package edu.jhu.rebar.scarbre
package accumulo

import edu.jhu.rebar.config.Configuration
import org.apache.thrift.protocol.TBinaryProtocol

abstract class AccumuloClient(conn: Connector) {
  protected lazy val serializer = new TSerializer(new TBinaryProtocol.Factory())
  protected lazy val deserializer = new TDeserializer(new TBinaryProtocol.Factory())

  protected val tableOps = new TableOps(conn)
  protected val bwOpts = new BatchWriterOpts

  bwOpts.batchLatency = Configuration.BWLatency
  bwOpts.batchMemory = Configuration.BWMemory
  bwOpts.batchThreads = Configuration.BWThreads
  bwOpts.batchTimeout = Configuration.BWTimeout

  protected val bwOptsCfg = bwOpts.getBatchWriterConfig

  protected lazy val bw = this.conn.createBatchWriter(Configuration.DocumentTableName, bwOptsCfg)

  protected lazy val corporaTableBW = conn.createBatchWriter(Configuration.CorpusTableName, bwOptsCfg)
}

object AccumuloClient {
  /**
    * The default `Authorizations`.
    */
  val DefaultAuths = org.apache.accumulo.core.Constants.NO_AUTHS

  /**
    * The `PasswordToken` for this user. 
    */
  val DefaultPasswordToken = new PasswordToken(Configuration.AccumuloPass)

  /**
    * Return a `Connector` object for use in rebar.
    * 
    * @return a `Connector` to either an in-memory or configured Accumulo cluster, depending on the configuration. 
    */
  def getConnector() : Connector = {
    Configuration.UseMock match {
      case true => new MockInstance().getConnector("max", new PasswordToken(""))
      case false => new ZooKeeperInstance(Configuration.AccumuloInstance, Configuration.Zookeepers).getConnector(Configuration.AccumuloUser, DefaultPasswordToken)
    }
  }
}
