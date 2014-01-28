/**
  *  Copyright 2012-2014 Johns Hopkins University HLTCOE. All rights reserved.
  *  This software is released under the 2-clause BSD license.
  *  See LICENSE in the project root directory.
  */
package edu.jhu.hlt.rebar
package accumulo

import edu.jhu.hlt.miser._
import edu.jhu.hlt.rebar._
import com.twitter.scrooge.{ThriftStruct, ThriftStructCodec}
import scala.util.{Try, Success, Failure}

trait StageTableBacked extends TableBacked {
  override val tableName = Configuration.StagesTableName
  val stagePrefix = Configuration.StagesPrefix
}

/**
  * A class that represents a basic Rebar "ingester", e.g., an
  * interface to ingest `Communication` objects without annotations.
  *
  * @param conn The `Connector` object to use to connect to Accumulo.
  */

class RebarStage[+T <: ThriftStruct](s: Stage) 
    extends StageTableBacked {

  val name = s.name
  if (!name.startsWith(stagePrefix))
    throw new IllegalArgumentException(s"'$name' is not a valid stage name. Stage names must begin with '$stagePrefix'.")

  if (!dependenciesExist)
    throw new IllegalArgumentException(s"One or more dependencies for stage '$s.name' did not exist.")

  def create : Try[IngestedStage[T]] = {
    if (exists) Failure(new IllegalArgumentException(s"'$name already exists.'"))
    Try(createInternal)
  }

  private def createInternal : IngestedStage[T] = {
    conn.withBatchWriter(Configuration.StagesTableName) { bw =>
      val m = new Mutation(name)
      m.put(Configuration.StagesObjectCF, "", new Value(BinaryThriftStructSerializer(Stage).toBytes(s)))
      bw.addMutation(m)
    }

    new IngestedStage[T](s)
  }

  def exists : Boolean = StageManager.exists(name)

  private def dependenciesExist : Boolean = {
    s.dependencies.map(StageManager.exists).forall(x => true)
  }
}

/**
  * A (more) immutable wrapper around a [[RebarStage]]. 
  * Contains annotation methods. 
  */
class IngestedStage[+T <: ThriftStruct](s: Stage) extends StageTableBacked {
  import scala.collection.JavaConverters._

  val name = s.name
  val dependencies = s.dependencies
  val `type` = s.`type`
  val description = s.description
  val createTime = s.createTime

  private def addAnnotatedDocumentId(comm: Communication) : Unit =
    addAnnotatedDocumentId(comm.id)

  private def addAnnotatedDocumentId(id: String) : Unit = {
    conn.withBatchWriter(tableName) { bw =>
      val m = new Mutation(name)
      m.put(Configuration.StagesDocumentsAnnotationIdCF, id, AccumuloClient.EmptyValue)
      bw.addMutation(m)
    }
  }

  def getAnnotatedDocumentIds : Set[String] = {
    val scan = conn.scanner(tableName).withRange(name)
    scan.fetchColumnFamily(new Text(Configuration.StagesDocumentsAnnotationIdCF))
    scan.iterator().asScala.map { e =>
      e.getKey.getColumnQualifier.toString
    }.toSet
  }

  def isAnnotated(c: Communication) : Boolean = {
    conn.scanner(tableName).withRange(c.id).iterator().hasNext()
  }

  def annotate[K <: Annotation[T]](a: K) : Try[Unit] = {
    if (isAnnotated(a.comm)) 
      Failure(new IllegalArgumentException("TODO"))

    Success()
  }
}

object StageManager {
  import scala.collection.JavaConverters._

  implicit val conn = AccumuloClient.DefaultConnector

  /**
    * Needed as we may not necessarily have seen objects that will
    * have created the table.
    */
  TableOps.checkExistsAndCreate(Configuration.StagesTableName)

  def exists(name: String) : Boolean = {
    conn.stageScanner.withRange(name).iterator().hasNext
  }

  // def getStages[T <: ThriftStruct] : Set[IngestedStage[T]] = {
  //   val scan = conn.stageScanner.withEmptyRange
  //   scan.fetchColumnFamily(new Text(Configuration.StagesObjectCF))
  //   scan
  //     .iterator()
  //     .asScala
  //     .map(e => stageToRebarStage(BinaryThriftStructSerializer(Stage)
  //       .fromBytes(e.getValue.get)))
  //     .toSet
  // }

  def stageToRebarStage(s: Stage) : RebarStage[_ <: ThriftStruct] = {
    s.`type` match {
      case StageType.Section      => new RebarStage[SectionSegmentation](s)
      case StageType.Sentence     => new RebarStage[SentenceSegmentation](s)
      case StageType.Tokenization => new RebarStage[Tokenization](s)
      case _ => throw new IllegalArgumentException("wtf")
    }
  }
}
