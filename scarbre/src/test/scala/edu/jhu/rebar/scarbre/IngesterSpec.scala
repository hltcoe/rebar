/**
  *  Copyright 2012-2014 Johns Hopkins University HLTCOE. All rights reserved.
  *  This software is released under the 2-clause BSD license.
  *  See LICENSE in the project root directory.
  */
package edu.jhu.hlt.rebar

import org.specs2.mutable._
import org.specs2.specification.{AllExpectations, Scope}
import edu.jhu.hlt.miser._
import edu.jhu.hlt.rebar.accumulo._
import scala.util.{Success, Failure, Try}
import scala.collection.JavaConverters._

trait IngesterScope extends Scope {
  implicit val conn = AccumuloClient.getConnector
  val ing = new BasicIngester
  val nDocs = 1000
  val commSeq = (for (i <- 1 to nDocs) yield Util.generateCommunication)
}

class IngesterSpec extends Specification with AllExpectations {
  "BasicIngester" should {
    "Allow valid Communication ingests" in new IngesterScope {
      val ingestRes = commSeq.take(10).map { comm => ing.ingest(comm) }
        .foreach { res => res must beSuccessfulTry }

      val scanner = conn.scanner(Configuration.DocumentTableName).withEmptyRange
      scanner.scalaIterator.length must beEqualTo(10)
    }

    "Allow bulk ingest via par mapping" in new IngesterScope {
      commSeq.par.map { comm => ing.ingest(comm) }
        .foreach { res => res must beSuccessfulTry }

      val scanner = conn
        .scanner(Configuration.DocumentTableName)
        .withEmptyRange
      scanner.scalaIterator.length must beEqualTo(nDocs)
    }

    // "Not allow invalid Communications" in new IngesterScope {
    //   val comm = Util.generateCommunication
    //   comm.text = None

    //   ing.ingest(comm) must beFailedTry.withThrowable[IllegalArgumentException]
    // }

    "Not show duplicate Communication IDs in retrieval" in new IngesterScope {
      val comm = Util.generateCommunication
      ing.ingest(comm) must beSuccessfulTry
      ing.ingest(comm) must beSuccessfulTry

      val scanner = conn
        .scanner(Configuration.DocumentTableName)
        .withRange(new Range(comm.id))
      scanner.scalaIterator.length must beEqualTo(1)
    }

    "Strip off extra fields from annotated Communications" in new IngesterScope {
      val comm = Util.generateCommunication
      md.tool = "Rebar Unit Tests"
      val md = AnnotationMetadata(md.tool)
      val lid = LanguageIdentification
      val uuid = Util.randomUuid
      val metadata = md
      val languageToProbabilityMap = Map("eng" -> new java.lang.Double(.99)).asJava
      comm.lid = lid

      ing.ingest(comm) must beSuccessfulTry
      val scanner = conn
        .scanner(Configuration.DocumentTableName)
        .withRange(new Range(comm.id))
      scanner.scalaIterator.length must beEqualTo(1)

      val results = scanner.scalaIterator.next()
      val bytez = results.getValue.get
      val deserComm = BinaryThriftStructSerializer(Communication).fromBytes(bytez)
      deserComm.lid must beNull
    }
  }
}
