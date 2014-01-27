/**
  *  Copyright 2012-2014 Johns Hopkins University HLTCOE. All rights reserved.
  *  This software is released under the 2-clause BSD license.
  *  See LICENSE in the project root directory.
  */
package edu.jhu.hlt.rebar

import edu.jhu.hlt.miser._

/**
  * Utility code useful in Rebar.
  */
object Util {

  import scala.util.Random
  private val r = new Random

  /**
    * A function that generates a mock `Communication`,
    * suitable for testing.
    * @return a `Communication` with a random doc ID, uuid,
    a type of `CommunicationType.OTHER`, and some sample text.
    */
  def generateCommunication : Communication = {
    val rInt = r.nextInt
    val docIdStr = s"Communication_$rInt"
    val uuidStr = randomUuid
    val text = "Lorem Ipsum. This is some sample text!"
    Communication(docIdStr, uuidStr, CommunicationType.Other, text)
  }

  def randomUuid : String = java.util.UUID.randomUUID.toString
}

class PowerCommunication (comm: Communication) {
  // private val populated : Boolean =
  //   comm.id != null &
  //     comm.uuid != null &
  //     comm.`type` != null &
  //     comm.text != null

  lazy val valid : Boolean = validUuid

  private def validUuid : Boolean = try {
    java.util.UUID.fromString(comm.uuid)
    true
  } catch {
    case _ : Exception => false
  }
}
