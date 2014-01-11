/**
  *  Copyright 2012-2014 Johns Hopkins University HLTCOE. All rights reserved.
  *  This software is released under the 2-clause BSD license.
  *  See LICENSE in the project root directory.
  */
package edu.jhu.rebar.scarbre
package accumulo

import edu.jhu.hlt.concrete._
import edu.jhu.rebar.config.Configuration

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import com.twitter.scrooge.BinaryThriftStructSerializer

/**
  * An object that represents Stages in Rebar. Stages are like dependencies for
  * analytics. For example, analytic A can output stage A. If tools require
  * the output of analytic A, subsequent stages can require stage A
  * in their dependency list.
  */
object StageHandler {
  import scala.util.{Try, Success, Failure}

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

    val deps = s.dependencies
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

  private def validStageName(name: String) : Boolean = {
    name.startsWith(Configuration.StagesPrefix)
  }
}

class RebarStage (stage: Stage) {
  def addAnnotatedDocument (comm: Communication) : Unit = {
    val m = new Mutation(stage.name)
    m.put(Configuration.StagesDocumentsAnnotationIdCF, comm.id, AccumuloClient.EmptyValue)
    AccumuloClient.DefaultConnector.withBatchWriter(Configuration.StagesTableName) { bw =>
      bw.addMutation(m)
    }
  }

  def getAnnotatedIds() : Set[String] = {
    val scan = AccumuloClient.DefaultConnector.scanner(Configuration.StagesTableName)
    scan.setRange(new Range(stage.name))
    scan.fetchColumnFamily(new Text(Configuration.StagesDocumentsAnnotationIdCF))
    val buf = new ArrayBuffer[String]
    scan.iterator().asScala.foreach { entry =>
      buf += entry.getKey.getColumnQualifier.toString
    }

    buf.toSet
  }
}
