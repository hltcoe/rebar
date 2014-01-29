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

  def getStages : Set[Stage] = {
    val scan = conn.stageScanner.withEmptyRange
    scan.fetchColumnFamily(new Text(Configuration.StagesObjectCF))
    scan
      .iterator()
      .asScala
      .map(e => BinaryThriftStructSerializer(Stage)
        .fromBytes(e.getValue.get))
      .toSet
  }
}

sealed trait TypedStage[T <: ThriftStruct] extends StageTableBacked {
  import scala.collection.JavaConverters._

  val stage : Stage
  val name = stage.name

  if (!name.startsWith(stagePrefix))
    throw new IllegalArgumentException(s"'$name' is not a valid stage name. Stage names must begin with '$stagePrefix'.")

  def annotate (a: Annotation[T]) = { 

  }

  def isAnnotated(c: Communication) : Boolean = {
    conn.scanner(tableName).withRange(c.id).iterator().hasNext()
  }

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

  def create : Try[Unit] = {
    if (exists)
      Failure(new IllegalArgumentException(s"Stage '$name' already exists."))

    if (!dependenciesExist)
      Failure(new IllegalArgumentException(s"Stage '$name' has dependencies that have not been created."))

    Try(createInternal)
  }

  protected def createInternal : Unit = {
    conn.withBatchWriter(Configuration.StagesTableName) { bw =>
      val m = new Mutation(name)
      m.put(Configuration.StagesObjectCF, "", new Value(BinaryThriftStructSerializer(Stage).toBytes(stage)))
      bw.addMutation(m)
    }
  }

  def exists : Boolean = StageManager.exists(name)
  private def dependenciesExist : Boolean = {
    stage.dependencies.map(StageManager.exists).forall(x => true)
  }
}

case class SectionStage (stage: Stage) extends TypedStage[SectionSegmentation]
case class SentenceStage (stage: Stage) extends TypedStage[SentenceSegmentation]
case class TokenizationStage (stage: Stage) extends TypedStage[Tokenization]
case class LanguageIdStage(stage: Stage) extends TypedStage[LanguageIdentification]

