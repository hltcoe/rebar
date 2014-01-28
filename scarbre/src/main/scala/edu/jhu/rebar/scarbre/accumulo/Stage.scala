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
  * A useful trait wrapping around a `Stage`.
  * Contains annotation methods. 
  */
class RebarStage(stage: Stage) extends StageTableBacked {
  val name = stage.name

  def create : Try[Unit] = {
    if (exists) 
      Failure(new IllegalArgumentException(s"Stage '$name' already exists."))
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
  // def addAnnotation (a: T, c: Communication) : Try[Unit] = {
  //   Failure(new IllegalArgumentException("TODO"))
  // }

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

}

// class RebarStage[+T <: ThriftStruct](s: Stage) 
//     extends StageTableBacked {

//   val name = s.name
//   if (!name.startsWith(stagePrefix))
//     throw new IllegalArgumentException(s"'$name' is not a valid stage name. Stage names must begin with '$stagePrefix'.")

//   if (!dependenciesExist)
//     throw new IllegalArgumentException(s"One or more dependencies for stage '$s.name' did not exist.")

//   def create : Try[IngestedStage[T]] = {
//     
//     Try(createInternal)
//   }

//   private def createInternal : IngestedStage[T] = {


//     new IngestedStage[T](s)
//   }

//   

//   private def dependenciesExist : Boolean = {
//     s.dependencies.map(StageManager.exists).forall(x => true)
//   }
// }

// class IngestedStage[+T <: ThriftStruct](s: Stage) extends StageTableBacked {
//   import scala.collection.JavaConverters._

//   val name = s.name
//   val dependencies = s.dependencies
//   val `type` = s.`type`
//   val description = s.description
//   val createTime = s.createTime

//   private def addAnnotatedDocumentId(comm: Communication) : Unit =
//     addAnnotatedDocumentId(comm.id)

//   private def addAnnotatedDocumentId(id: String) : Unit = {
//     conn.withBatchWriter(tableName) { bw =>
//       val m = new Mutation(name)
//       m.put(Configuration.StagesDocumentsAnnotationIdCF, id, AccumuloClient.EmptyValue)
//       bw.addMutation(m)
//     }
//   }

//   def getAnnotatedDocumentIds : Set[String] = {
//     val scan = conn.scanner(tableName).withRange(name)
//     scan.fetchColumnFamily(new Text(Configuration.StagesDocumentsAnnotationIdCF))
//     scan.iterator().asScala.map { e =>
//       e.getKey.getColumnQualifier.toString
//     }.toSet
//   }

//   def isAnnotated(c: Communication) : Boolean = {
//     conn.scanner(tableName).withRange(c.id).iterator().hasNext()
//   }

//   def annotate[K <: Annotation[T]](a: K) : Try[Unit] = {
//     if (isAnnotated(a.comm)) 
//       Failure(new IllegalArgumentException("TODO"))

//     Success()
//   }
// }

