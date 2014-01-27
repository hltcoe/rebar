/**
  *  Copyright 2012-2013 Johns Hopkins University HLTCOE. All rights reserved.
  *  This software is released under the 2-clause BSD license.
  *  See LICENSE in the project root directory.
  */
package edu.jhu.hlt.rebar

import edu.jhu.hlt.miser.{Communication}

object `package` {
  implicit def c2pc(orig: Communication) = new PowerCommunication(orig)
}
