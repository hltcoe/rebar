/*
 * Copyright 2012-2014 Johns Hopkins University HLTCOE. All rights reserved.
 * This software is released under the 2-clause BSD license.
 * See LICENSE in the project root directory.
 */
package edu.jhu.hlt.rebar.ballast.tools;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.junit.Test;

import edu.jhu.hlt.concrete.Sentence;
import edu.jhu.hlt.concrete.TextSpan;

public class SillySentenceSegmenterTest {

  @Test
  public void exampleSentenceSplit() {
    String text = "hello. This is a sample sentence, is it? Very useful!";
    
    SillySentenceSegmenter sss = new SillySentenceSegmenter();
    List<Sentence> sentList = sss.generateSentencesFromText(text);
    assertEquals(new TextSpan(0, 6), sentList.get(0).getTextSpan());
    assertEquals(new TextSpan(6, 40), sentList.get(1).getTextSpan());
    assertEquals(new TextSpan(40, 53), sentList.get(2).getTextSpan());
  }
}
