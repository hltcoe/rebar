/**
  *  Copyright 2012-2014 Johns Hopkins University HLTCOE. All rights reserved.
  *  This software is released under the 2-clause BSD license.
  *  See LICENSE in the project root directory.
  */
package edu.jhu.hlt.rebar
package accumulo

import edu.jhu.hlt.concrete._
import edu.jhu.hlt.rebar._
import edu.jhu.hlt.rebar.Configuration

/**
  * A class that represents a basic Rebar "ingester", e.g., an
  * interface to ingest `Communication` objects without annotations.
  *
  * @param conn The `Connector` object to use to connect to Accumulo.
  */
class StageManager(implicit conn: Connector) {
  import scala.util.{Try, Success, Failure}

  TableOps.checkExistsAndCreate(Configuration.StagesTableName)

  lazy val bw = this.conn.createBatchWriter(Configuration.StagesTableName, AccumuloClient.DefaultBWConfig)

  def exists(name: String) : Try[Boolean] = Try(conn.stageScanner.withRange(name).iterator().hasNext)
  def exists(s: Stage) : Try[Boolean] = exists(s.name)

  def create(s: Stage) : Try[Unit] = {
    val name = s.name
    if (!exists(name))
      Failure(new IllegalArgumentException(s"Corpus '$name' already exists."))

    if (!validStageName(name))
      Failure(new IllegalArgumentException(s"'$name' is not a valid stage name. Stage names must begin with '$Configuration.StagesPrefix'."))

    val nonExistent = s.dependencies filter (dep => !exists(dep))
    if (!nonExistent.isEmpty)
      Failure(new IllegalArgumentException("One or more dependencies did not exist."))

    Try(createInternal(s))
  }

  def getStages : Set[Stage] = {
    val scan = conn.stageScanner.withEmptyRange
    scan.fetchColumnFamily(new Text(Configuration.StagesObjectCF))
    scan.iterator().asScala.foreach { entry =>
      buf += BinaryThriftStructSerializer(Stage).fromBytes(entry.getValue.get)
    }

    buf.toSet
  }

  private def createInternal(s: Stage) : Unit = {
    conn.withBatchWriter(Configuration.StagesTableName) {bw =>
      val m = new Mutation(s.name)
      m.put(Configuration.StagesObjectCF, "", new Value(BinaryThriftStructSerializer(Stage).toBytes(s)))
      bw.addMutation(m)
    }
  }

  /**
    * Close the underlying `BatchWriter` associated with this [[StageManager]].
    */
  def close = this.bw.close
}

class PowerStage(s: Stage) {
  lazy val validName = s.name.startsWith(Configuration.StagesPrefix)

}
