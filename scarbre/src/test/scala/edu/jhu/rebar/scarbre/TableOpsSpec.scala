/**
  *  Copyright 2012-2014 Johns Hopkins University HLTCOE. All rights reserved.
  *  This software is released under the 2-clause BSD license.
  *  See LICENSE in the project root directory.
  */
package edu.jhu.rebar.scarbre

import org.specs2.mutable._
import edu.jhu.rebar.scarbre.accumulo.TableOps
import scala.util.{Success, Failure, Try}

class TableOpsSpec extends Specification {
  "TableOps" should {
    "Allow valid table names to be created" in {
      TableOps.create("foo") must beSuccessfulTry
    }

    "Not allow invalid table names to be created" in {
      TableOps.create(".") must beFailedTry
    }

    "Handle duplicate tables correctly" in {
      TableOps.create("hello")
      TableOps.create("hello") must beFailedTry
    }

    "Correctly find existing tables" in {
      TableOps.create("baz")
      TableOps.exists("baz") must beSuccessfulTry.withValue(true)
    }

    "Correctly not-find non-existent tables" in {
      TableOps.exists ("qux") must beSuccessfulTry.withValue(false)
    }

    "Handle deleting existing tables" in {
      TableOps.create("boo")
      TableOps.delete("boo") must beSuccessfulTry
    }

    "Handle deleting non-existent tables" in {
      TableOps.delete("asdf") must beFailedTry
    }
  }
}
