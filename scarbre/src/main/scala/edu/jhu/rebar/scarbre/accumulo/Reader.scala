/**
  *  Copyright 2012-2014 Johns Hopkins University HLTCOE. All rights reserved.
  *  This software is released under the 2-clause BSD license.
  *  See LICENSE in the project root directory.
  */
package edu.jhu.hlt.rebar
package accumulo

import edu.jhu.hlt.miser._
import edu.jhu.hlt.rebar.Configuration

import scala.util.{Try, Success, Failure}
import scala.concurrent.{ future, promise, Future, Promise}
import scala.collection.JavaConverters._

class AsyncReader extends TableBacked {
  override val tableName = Configuration.DocumentTableName

  import java.util.concurrent.Executors
  import concurrent.ExecutionContext
  private val executorService = Executors.newFixedThreadPool(4)
  private val executionContext = ExecutionContext.fromExecutorService(executorService)

  def query(id: String) : Future[Option[Communication]] = {
    promise[Option[Communication]]
      .complete(Try(getRawCommunicationFromAccumulo(id)))
      .future
  }

  private def getRawCommunicationFromAccumulo(id : String) :
      Option[Communication] = {
    val scan = conn
      .scanner(Configuration.DocumentTableName)
      .withRange(new Range(id))
    val iter = scan.iterator
    if (iter.hasNext)
      Option(BinaryThriftStructSerializer(Communication).fromBytes(iter.next._2.get))
    else
      None

    // Option[(Key, Value)](res) match {
    //   case Some((k, v)) => Option(BinaryThriftStructSerializer(Communication).fromBytes(v.get))
    //   case None => None
    // }
  }

  def query(ids : Iterable[String])
      : Future[Seq[Communication]] = {
    promise[Seq[Communication]]
      .complete(Try(getRawCommunicationsFromAccumulo(ids)))
      .future
  }

  private def getRawCommunicationsFromAccumulo(ids : Iterable[String])
      : Seq[Communication] = {
    val ranges = ids.par.map (id => new Range(id))
    conn.withBatchScanner[Communication](Configuration.DocumentTableName) { bsc =>
      bsc.setRanges(ranges.toList.asJava)
      bsc.iterator().asScala.map { entry =>
        BinaryThriftStructSerializer(Communication).fromBytes(entry.getValue.get)
      }
    }.toVector
  }
}
