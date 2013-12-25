/**
  *  Copyright 2012-2013 Johns Hopkins University HLTCOE. All rights reserved.
  *  This software is released under the 2-clause BSD license.
  *  See LICENSE in the project root directory.
  */
package edu.jhu.rebar.scarbre

import com.github.nscala_time.time.Imports._

/**
  * Utility methods useful in Rebar.
  */
object Util {
  /**
    * Returns the current unix time as an `Int`.
    */
  def getCurrentUnixTime : Int = {
    (DateTime.now.millis / 1000).toInt
  }

  
}

