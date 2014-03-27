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
import edu.jhu.hlt.concrete.EntityMention;
import edu.jhu.hlt.concrete.EntityMentionSet;
import edu.jhu.hlt.concrete.EntityType;
import edu.jhu.hlt.concrete.PhraseType;
import edu.jhu.hlt.concrete.Sentence;
import edu.jhu.hlt.concrete.Token;
import edu.jhu.hlt.concrete.TokenRefSequence;
import edu.jhu.hlt.concrete.Tokenization;
import edu.jhu.hlt.concrete.util.ConcreteException;
import edu.jhu.hlt.concrete.util.SuperCommunication;
import edu.jhu.hlt.rebar.ballast.AnnotationException;

/**
 * A silly example of a tool that produces {@link EntityMentionSet}s by tagging any
 * "Latin-esq" "entities".
 * 
 * @author max
 */
public class LatinEntityTagger extends BallastAnnotationTool<EntityMentionSet>{

  private static final Logger logger = LoggerFactory.getLogger(LatinEntityTagger.class);
  
  private final Set<String> latinWordSet;
  
  /**
   * 
   */
  public LatinEntityTagger() {
    this.latinWordSet = new HashSet<>();
    latinWordSet.add("capua");
    latinWordSet.add("roman");
  }

  /*
   * (non-Javadoc)
   * @see edu.jhu.hlt.rebar.ballast.AnnotationTool#annotate(edu.jhu.hlt.concrete.Communication)
   */
  @Override
  public EntityMentionSet annotate(Communication c) throws AnnotationException {
    try {
      SuperCommunication sc = new SuperCommunication(c);
      EntityMentionSet ems = new EntityMentionSet();
      ems.setMetadata(this.getMetadata());
      ems.uuid = UUID.randomUUID().toString();

      for (Sentence st : sc.firstSentenceSegmentation().getSentenceList()) {
        for (Tokenization t : st.getTokenizationList()) {
          for (Token tk : t.getTokenList()) {
            String tokenText = tk.getText();
            logger.debug("Working with token text: {}", tokenText);
            String lc = tokenText.toLowerCase();
            if (this.latinWordSet.contains(lc)) {
              TokenRefSequence trs = new TokenRefSequence();
              trs.tokenizationId = t.getUuid();
              trs.textSpan = tk.getTextSpan();
              trs.addToTokenIndexList(tk.getTokenIndex());
              
              EntityMention em = new EntityMention();
              em.setUuid(UUID.randomUUID().toString());
              em.confidence = 1.0d;
              em.entityType = EntityType.LOCATION;
              em.phraseType = PhraseType.NAME;
              em.text = tokenText;
              em.tokens = trs;
              
              ems.addToMentionSet(em);
            }
          }
        }
      }
      
      if (ems.getMentionSetSize() < 1)
        ems.setMentionSet(new ArrayList<EntityMention>());
      return ems;
    } catch (ConcreteException e) {
      throw new AnnotationException(e);
    }
  }

}
