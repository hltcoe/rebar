/**
  *  Copyright 2012-2014 Johns Hopkins University HLTCOE. All rights reserved.
  *  This software is released under the 2-clause BSD license.
  *  See LICENSE in the project root directory.
  */
package edu.jhu.hlt.rebar
package accumulo

import edu.jhu.hlt.miser._
import edu.jhu.hlt.rebar._

/**
  * A class that represents a basic Rebar "ingester", e.g., an
  * interface to ingest `Communication` objects without annotations.
  *
  * @param conn The `Connector` object to use to connect to Accumulo.
  */

class RebarStage(s: Stage) extends Connected {
  import scala.util.{Try, Success, Failure}

  if (!s.name.startsWith(Configuration.StagesPrefix))
    throw new IllegalArgumentException(s"'$s.name' is not a valid stage name. Stage names must begin with '$Configuration.StagesPrefix'.")

  if (!dependenciesExist)
    throw new IllegalArgumentException(s"One or more dependencies for stage '$s.name' did not exist.")

  def create : Try[Unit] = {
    if (exists) Failure(new IllegalArgumentException(s"'$s.name already exists.'"))
    Try(createInternal)
  }

  private def createInternal : Unit = {
    conn.withBatchWriter(Configuration.StagesTableName) {bw =>
      val m = new Mutation(s.name)
      m.put(Configuration.StagesObjectCF, "", new Value(BinaryThriftStructSerializer(Stage).toBytes(s)))
      bw.addMutation(m)
    }
  }

  def exists : Boolean = StageManager.exists(s.name)

  private def dependenciesExist : Boolean = {
    s.dependencies.map(StageManager.exists).forall(x => true)
  }
}

object StageManager {
  import scala.collection.JavaConverters._

  implicit val conn = AccumuloClient.DefaultConnector

  TableOps.checkExistsAndCreate(Configuration.StagesTableName)

  def exists(name: String) : Boolean = {
    conn.stageScanner.withRange(name).iterator().hasNext
  }

  def getStages : Set[Stage] = {
    val scan = conn.stageScanner.withEmptyRange
    scan.fetchColumnFamily(new Text(Configuration.StagesObjectCF))
    scan
      .iterator()
      .asScala
      .map(e => BinaryThriftStructSerializer(Stage).fromBytes(e.getValue.get))
      .toSet
  }
}
