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

trait DocumentTableBacked extends TableBacked {
  override val tableName = Configuration.DocumentTableName
}

sealed trait Annotation[T <: ThriftStruct] extends DocumentTableBacked {
  val anno : T
  val comm : Communication

  def toBytes : Array[Byte]

  def annotate(s: TypedStage[T]) : Try[Unit] = {
    Failure(new RuntimeException("TBD"))
  }
}

case class SectionAnnotation(anno: SectionSegmentation, comm: Communication) 
    extends Annotation[SectionSegmentation] {
  def toBytes = BinaryThriftStructSerializer(SectionSegmentation).toBytes(anno)
}

case class SentenceAnnotation(anno: SentenceSegmentation, comm: Communication) 
    extends Annotation[SentenceSegmentation] {
  def toBytes = BinaryThriftStructSerializer(SentenceSegmentation).toBytes(anno)
}

case class TokenizationAnnotation(anno: Tokenization, comm: Communication) 
    extends Annotation[Tokenization] {
  def toBytes = BinaryThriftStructSerializer(Tokenization).toBytes(anno)
}

case class LanguageIdAnnotation(anno: LanguageIdentification, comm: Communication) 
    extends Annotation[LanguageIdentification] {
  def toBytes = BinaryThriftStructSerializer(LanguageIdentification).toBytes(anno)
}
