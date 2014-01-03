/**
  *  Copyright 2012-2014 Johns Hopkins University HLTCOE. All rights reserved.
  *  This software is released under the 2-clause BSD license.
  *  See LICENSE in the project root directory.
  */
package edu.jhu.rebar.scarbre

/**
  * Utility methods useful in Rebar.
  */
object Util {
  // /**
  //   * Returns the current unix time as an `Int`.
  //   */
  // def getCurrentUnixTime : Int = {
  //   (DateTime.now.millis / 1000).toInt
  // }

  val DefaultSerializer =
    new TSerializer(new org.apache.thrift.protocol.TBinaryProtocol.Factory())

  val DefaultDeserializer =
    new TDeserializer(new org.apache.thrift.protocol.TBinaryProtocol.Factory())
}
