/**
  *  Copyright 2012-2014 Johns Hopkins University HLTCOE. All rights reserved.
  *  This software is released under the 2-clause BSD license.
  *  See LICENSE in the project root directory.
  */
package edu.jhu.hlt.rebar
package accumulo

import edu.jhu.hlt.miser._
import edu.jhu.hlt.rebar._

trait Annotation[T] {
  def comm : Communication
  def toBytes(x: T) : Array[Byte] = BinaryThriftStructSerializer[T].toBytes(x)
}

class SectionAnnotation (_comm: Communication, _section: SectionSegmentation) 
    extends Annotation[SectionSegmentation] with Connected {
  import scala.util.{Try, Success, Failure}

  override def comm = _comm

  def annotate (s: Stage) : Try[Unit] = {
    Failure(new IllegalArgumentException("TODO"))
  }
}
