/*
 * Copyright 2012-2014 Johns Hopkins University HLTCOE. All rights reserved.
 * See LICENSE in the project root directory.
 */
package edu.jhu.hlt.rebar.ballast.tools;

import edu.jhu.hlt.concrete.Communication;
import edu.jhu.hlt.concrete.Section;
import edu.jhu.hlt.concrete.Sentence;
import edu.jhu.hlt.concrete.Tokenization;
import edu.jhu.hlt.concrete.TokenizationCollection;
import edu.jhu.hlt.concrete.util.SuperTextSpan;
import edu.jhu.hlt.rebar.ballast.AnnotationException;
import edu.jhu.hlt.tift.Tokenizer;

/**
 * A wrapper around Tift that provides {@link TokenizationCollection} objects
 * for each {@link Sentence} in each {@link Section}.
 * 
 * @author max
 *
 */
public class TiftTokenizer extends BallastAnnotationTool<TokenizationCollection> {

  private final Tokenizer tokenizer;
  
  /**
   * 
   */
  public TiftTokenizer(Tokenizer tokenizer) {
    this.tokenizer = tokenizer;
  }

  /*
   * (non-Javadoc)
   * @see edu.jhu.hlt.rebar.ballast.AnnotationTool#annotate(edu.jhu.hlt.concrete.Communication)
   */
  @Override
  public TokenizationCollection annotate(Communication c) throws AnnotationException {
    Communication copy = new Communication(c);
    TokenizationCollection tokColl = new TokenizationCollection();

    for (Section s : copy.getSectionSegmentations().get(0).getSectionList()) {
      for (Sentence st : s.getSentenceSegmentation().get(0).getSentenceList()) {
        SuperTextSpan sts = new SuperTextSpan(st.getTextSpan(), copy);
        String sentenceText = sts.getText();
        Tokenization t = this.tokenizer.tokenizeSentence(sentenceText, 0, st.getUuid());
        tokColl.addToTokenizationList(t);
      }
    }
    
    return tokColl;
  }

}
