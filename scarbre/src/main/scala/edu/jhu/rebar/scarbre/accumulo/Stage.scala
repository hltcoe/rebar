/**
  *  Copyright 2012-2014 Johns Hopkins University HLTCOE. All rights reserved.
  *  This software is released under the 2-clause BSD license.
  *  See LICENSE in the project root directory.
  */
package edu.jhu.rebar.scarbre
package accumulo

import edu.jhu.hlt.concrete._
import edu.jhu.rebar.config.Configuration

/**
  * An object that represents Stages in Rebar. Stages are like dependencies for
  * analytics. For example, analytic A can output stage A. If tools require
  * the output of analytic A, subsequent stages can require stage A
  * in their dependency list.
  */
object Stage {
  import scala.util.{Try, Success, Failure}
  import scala.collection.JavaConverters._
  import scala.collection.mutable.ArrayBuffer

  private val conn = AccumuloClient.DefaultConnector

  /**
    * Return `True` if the stage exists.
    *
    * @param name The name of the [[Stage]].
    */
  def exists(name: String) : Boolean = {
    val scan = conn.scanner(Configuration.StagesTableName)
    scan.setRange(new Range(name))
    scan.iterator().hasNext()
  }

  def exists(s: Stage) : Boolean = exists(s.name)

  def create(s: Stage) : Try[Unit] = {
    val name = s.name
    if (!exists(name))
      Failure(new IllegalArgumentException(s"Corpus '$name' already exists."))

    if (!validStageName(name))
      Failure(new IllegalArgumentException(s"'$name' is not a valid stage name. Stage names must begin with '$Configuration.StagesPrefix'."))

    val deps = s.dependencies.asScala
    val nonExistent = deps filter (dep => !exists(dep))
    if (!nonExistent.isEmpty)
      Failure(new IllegalArgumentException("One or more dependencies did not exist."))

    Try(createInternal(s))
  }

  def getStages : Set[Stage] = {
    val buf = new ArrayBuffer[Stage]
    val scan = conn.scanner(Configuration.StagesTableName)
    scan.setRange(new Range())
    scan.fetchColumnFamily(new Text(Configuration.StagesObjectCF))
    scan.iterator().asScala.foreach { entry =>
      val stage = new Stage()
      Util.DefaultDeserializer.deserialize(stage, entry.getValue.get)
      buf += stage
    }

    buf.toSet
  }

  private def createInternal(s: Stage) : Unit = {
    conn.withBatchWriter(Configuration.StagesTableName) {bw =>
      val m = new Mutation(s.name)
      m.put(Configuration.StagesObjectCF, "", new Value(Util.DefaultSerializer.serialize(s)))
      bw.addMutation(m)
    }
  }

  private def validStageName(name: String) : Boolean = {
    name.startsWith(Configuration.StagesPrefix)
  }
}
