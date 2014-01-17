/**
  *  Copyright 2012-2014 Johns Hopkins University HLTCOE. All rights reserved.
  *  This software is released under the 2-clause BSD license.
  *  See LICENSE in the project root directory.
  */
package edu.jhu.hlt.rebar

import org.specs2.mutable._
import edu.jhu.hlt.concrete._
import edu.jhu.hlt.rebar.accumulo._
import scala.util.{Success, Failure, Try}
import scala.collection.JavaConverters._

class IngesterSpec extends Specification {
  "BasicIngester" should {
    "Allow valid Communication ingests" in {
      implicit val conn : Connector = AccumuloClient.getConnector
      val ing = new BasicIngester
      val commSeq = for (i <- 1 to 10) yield Util.generateCommunication
      commSeq.map { comm => ing.ingest(comm) }
        .foreach { res => res must beSuccessfulTry }

      val scanner = conn.scanner(Configuration.DocumentTableName)
      scanner.withRange(new Range())
      scanner.iterator().asScala.length must beEqualTo(10)
    }

    "Allow bulk ingest via par mapping" in {
      val nDocs = 1000
      implicit val conn : Connector = AccumuloClient.getConnector
      val ing = new BasicIngester
      val commSeq = (for (i <- 1 to nDocs) yield Util.generateCommunication).par
      commSeq.map { comm => ing.ingest(comm) }
        .foreach { res => res must beSuccessfulTry }

      val scanner = conn.scanner(Configuration.DocumentTableName)
      scanner.withRange(new Range())
      scanner.iterator().asScala.length must beEqualTo(nDocs)
    }

    "Not allow invalid Communications" in {
      implicit val conn : Connector = AccumuloClient.getConnector
      val ing = new BasicIngester
      val comm = Util.generateCommunication
      comm.text = null

      ing.ingest(comm) must beFailedTry.withThrowable[IllegalArgumentException]
    }

    "Not show duplicate Communication IDs in retrieval" in {
      implicit val conn : Connector = AccumuloClient.getConnector
      val ing = new BasicIngester
      val comm = Util.generateCommunication
      val commDupe = new Communication(comm)
      ing.ingest(comm) must beSuccessfulTry
      ing.ingest(commDupe) must beSuccessfulTry

      val scanner = conn.scanner(Configuration.DocumentTableName)
      scanner.withRange(new Range())
      scanner.iterator().asScala.length must beEqualTo(1)
    }

    "Strip off extra fields from annotated Communications" in {
      implicit val conn : Connector = AccumuloClient.getConnector
      val ing = new BasicIngester
      val comm = Util.generateCommunication
      val md = new AnnotationMetadata()
      md.tool = "Rebar Unit Tests"
      val lid = new LanguageIdentification
      lid.uuid = Util.randomUuid
      lid.metadata = md
      lid.languageToProbabilityMap = Map("eng" -> new java.lang.Double(.99)).asJava
      comm.lid = lid

      ing.ingest(comm) must beSuccessfulTry
      val scanner = conn.scanner(Configuration.DocumentTableName)
      scanner.withRange(new Range())
      scanner.iterator().asScala.length must beEqualTo(1)

      val results = scanner.iterator().asScala.next()
      val bytez = results.getValue.get
      val deserComm = CommunicationSerializer.fromBytes(bytez)
      deserComm.lid must beNull
    }
  }
}
