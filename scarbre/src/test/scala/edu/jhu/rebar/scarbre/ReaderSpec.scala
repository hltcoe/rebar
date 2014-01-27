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

import concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Await
import scala.concurrent.duration._

trait ReaderScope extends IngesterScope {
  val reader = new AsyncReader
  commSeq.map { c => ing.ingest(c) }
  val sample = commSeq.sortWith { (s1, s2) => s1.id < s2.id }.head
  val sampleId = sample.id
  val duration = Duration(100, "millis")
}

class ReaderSpec extends Specification {
  "AsyncReader" should {
    "Find None for communications that don't exist" in new ReaderScope {
      Await.result(reader.query("fooblat"), duration) must beNone
    }

    "Query single communications by ID" in new ReaderScope {
      Await.result(reader.query(sampleId), duration) must beSome.which { res => res == sample }
    }

    "Query multiple communications by ID" in new ReaderScope {
      val res = reader.query(commSeq.map { c => c.id })
      Await.result(res, duration) must beEqualTo(commSeq)
    }
  }
}
