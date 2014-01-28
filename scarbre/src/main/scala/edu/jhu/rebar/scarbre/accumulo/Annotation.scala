/**
  *  Copyright 2012-2014 Johns Hopkins University HLTCOE. All rights reserved.
  *  This software is released under the 2-clause BSD license.
  *  See LICENSE in the project root directory.
  */
package edu.jhu.hlt.rebar
package accumulo

import edu.jhu.hlt.miser._
import edu.jhu.hlt.rebar._
import com.twitter.scrooge.{ThriftStruct, ThriftStructCodec}
import scala.util.{Try, Success, Failure}

// trait DocumentTableBacked extends TableBacked {
//   override val tableName = Configuration.DocumentTableName
// }

// sealed trait Annotation extends DocumentTableBacked {
//   val anno[T <: ThriftStruct] : T
//   def comm : Communication
// }

// case class SectionAnnotation (comm: Communication, anno: SectionSegmentation)
//     extends Annotation[SectionSegmentation]

// case class SentenceAnnotation(comm: Communication, anno: SentenceSegmentation)
//     extends Annotation[SentenceSegmentation]

// case class TokenizationAnnotation(comm: Communication, anno: Tokenization)
//     extends Annotation[Tokenization]

sealed trait Annotation[T <: ThriftStruct] {
  val anno : T
  val comm : Communication

  def toBytes[T <: ThriftStruct](x: ThriftStructCodec[T], y: T) : Array[Byte] =
    BinaryThriftStructSerializer(x).toBytes(y)
}
case class SectionAnnotation(anno: SectionSegmentation, comm: Communication) extends Annotation[SectionSegmentation]

case class SentenceAnnotation(anno: SentenceSegmentation, comm: Communication) extends Annotation[SentenceSegmentation]

case class TokenizationAnnotation(anno: Tokenization, comm: Communication) extends Annotation[Tokenization]

case class LangIdAnnotation(anno: LanguageIdentification, comm: Communication) extends Annotation[LanguageIdentification]
