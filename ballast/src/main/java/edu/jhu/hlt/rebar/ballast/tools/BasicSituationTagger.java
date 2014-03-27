/*
 * Copyright 2012-2014 Johns Hopkins University HLTCOE. All rights reserved.
 * See LICENSE in the project root directory.
 */
package edu.jhu.hlt.rebar.ballast.tools;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.jhu.hlt.concrete.Communication;
import edu.jhu.hlt.concrete.Sentence;
import edu.jhu.hlt.concrete.SituationMention;
import edu.jhu.hlt.concrete.SituationMentionSet;
import edu.jhu.hlt.concrete.SituationType;
import edu.jhu.hlt.concrete.Token;
import edu.jhu.hlt.concrete.TokenRefSequence;
import edu.jhu.hlt.concrete.Tokenization;
import edu.jhu.hlt.concrete.util.ConcreteException;
import edu.jhu.hlt.concrete.util.SuperCommunication;
import edu.jhu.hlt.rebar.ballast.AnnotationException;

/**
 * A silly example of a tool that produces {@link SituationMentionSet}s by tagging any
 * some basic actions.
 * 
 * @author max
 */
public class BasicSituationTagger extends BallastAnnotationTool<SituationMentionSet>{

  private static final Logger logger = LoggerFactory.getLogger(BasicSituationTagger.class);
  
  private final Set<String> basicActionSet;
  
  /**
   * 
   */
  public BasicSituationTagger() {
    this.basicActionSet = new HashSet<>();
    basicActionSet.add("fled");
    basicActionSet.add("returned");
  }

  /*
   * (non-Javadoc)
   * @see edu.jhu.hlt.rebar.ballast.AnnotationTool#annotate(edu.jhu.hlt.concrete.Communication)
   */
  @Override
  public SituationMentionSet annotate(Communication c) throws AnnotationException {
    try {
      SuperCommunication sc = new SuperCommunication(c);
      SituationMentionSet sms = new SituationMentionSet();
      sms.setMetadata(this.getMetadata());
      sms.uuid = UUID.randomUUID().toString();

      for (Sentence st : sc.firstSentenceSegmentation().getSentenceList()) {
        for (Tokenization t : st.getTokenizationList()) {
          for (Token tk : t.getTokenList()) {
            String tokenText = tk.getText();
            logger.debug("Working with token text: {}", tokenText);
            String lc = tokenText.toLowerCase();
            if (this.basicActionSet.contains(lc)) {
              TokenRefSequence trs = new TokenRefSequence();
              trs.tokenizationId = t.getUuid();
              trs.textSpan = tk.getTextSpan();
              trs.addToTokenIndexList(tk.getTokenIndex());
              
              SituationMention sm = new SituationMention();
              sm.setUuid(UUID.randomUUID().toString());
              sm.confidence = 1.0d;
              sm.situationType = SituationType.FACT;
              
              sm.text = tokenText;
              sm.tokens = trs;
              
              sms.addToMentionList(sm);
            }
          }
        }
      }
      
      if (sms.getMentionListSize() < 1)
        sms.setMentionList(new ArrayList<SituationMention>());
      return sms;
    } catch (ConcreteException e) {
      throw new AnnotationException(e);
    }
  }

}
