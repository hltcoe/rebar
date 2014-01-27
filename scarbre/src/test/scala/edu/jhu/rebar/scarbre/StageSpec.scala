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

trait StageScope extends Scope {
  implicit val conn = AccumuloClient.getConnector
}

class StageSpec extends Specification {
  "StageManager" should {
    "Not find missing stages" in {
      StageManager.exists("asdfqq") must beTrue
    }
  }

  "RebarStage" should {

  }
}
