/*
 * Copyright 2012-2014 Johns Hopkins University HLTCOE. All rights reserved.
 * See LICENSE in the project root directory.
 */
package edu.jhu.hlt.rebar.ballast.tools;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import edu.jhu.hlt.concrete.Communication;
import edu.jhu.hlt.concrete.Section;
import edu.jhu.hlt.concrete.SectionSegmentation;
import edu.jhu.hlt.concrete.Sentence;
import edu.jhu.hlt.concrete.SentenceSegmentation;
import edu.jhu.hlt.concrete.SentenceSegmentationCollection;
import edu.jhu.hlt.concrete.TextSpan;
import edu.jhu.hlt.concrete.util.SuperTextSpan;
import edu.jhu.hlt.concrete.util.Util;
import edu.jhu.hlt.rebar.ballast.AnnotationException;

/**
 * A silly example of how to generate {@link SentenceSegmentationCollection}s. Probably
 * only useful as an example.  
 * 
 * @author max
 */
public class SillySentenceSegmenter extends BallastAnnotationTool<SentenceSegmentationCollection> {

  public static final Pattern DEFAULT_SENTENCE_PATTERN = Pattern.compile("[a-zA-Z0-9 ,']+[.?!]+");
  private final Pattern splitPattern;
  
  public SillySentenceSegmenter() {
    this.splitPattern = DEFAULT_SENTENCE_PATTERN;
  }
  
  /**
   * Generate a {@link SentenceSegmentationCollection}. Will not be useful
   * for the majority of use cases, but provides a decent example of how to code up such a tool.
   * 
   * @param c - A {@link Communication} to generate a {@link SentenceSegmentationCollection} from. Note that it
   * must have at least one {@link SectionSegmentation} with at least one {@link Section}.
   * @throws AnnotationException - if the {@link Communication} is not properly formatted (see above note).
   */
  /*
   * (non-Javadoc)
   * @see edu.jhu.hlt.rebar.ballast.AnnotationTool#annotate(edu.jhu.hlt.concrete.Communication)
   */
  @Override
  public SentenceSegmentationCollection annotate(Communication c) throws AnnotationException {
    final Communication copiedComm = new Communication(c);
    SentenceSegmentationCollection col = new SentenceSegmentationCollection();
    col.metadata = Util.getMetadata();
    
    List<SectionSegmentation> ssList = copiedComm.getSectionSegmentations();
    if (ssList == null || ssList.isEmpty())
      throw new AnnotationException("Communication does not have at least one SectionSegmentation; "
          + "cannot generate a SentenceSegmentationCollection from it.");
    // by default, we'll only run on the first SectionSegmentation.
    SectionSegmentation sectSeg = ssList.get(0);
    List<Section> sectionList = sectSeg.getSectionList();
    if (sectionList == null || sectionList.isEmpty()) {
      throw new AnnotationException("Communication SectionSegmentation does not have at least one Section; "
          + "cannot generate a SentenceSegmentationCollection from it.");
    }
    
    // we want to generate one SentenceSegmentation object per Section.
    // we will then add this to the SentenceSegmentationCollection.
    for (Section s : sectionList) {
      TextSpan ts = s.getTextSpan();
      SuperTextSpan sts = new SuperTextSpan(ts, copiedComm);
      String sectionText = sts.getText();
      List<Sentence> sentList = this.generateSentencesFromText(sectionText);
      
      SentenceSegmentation ss = new SentenceSegmentation();
      ss.setUuid(UUID.randomUUID().toString());
      ss.metadata = Util.getMetadata();
      ss.setSentenceList(sentList);
      ss.sectionId = s.uuid;
      
      col.addToSentSegList(ss);
    }
    
    return col;
  }
  
  /**
   * Given some text, generate a {@link List} of {@link Sentence} objects given the {@link Pattern}
   * for this class, which is:
   * 
   * <pre>
   * [a-zA-Z0-9 ']+[.?!]+
   * </pre>
   * 
   * @param s - The {@link String} from which to generate {@link Sentence}s
   * @return a {@link List} of {@link Sentence} objects
   */
  public List<Sentence> generateSentencesFromText(String s) {
    List<Sentence> sentList = new ArrayList<Sentence>();
    Matcher m = this.splitPattern.matcher(s);
    while(m.find()) {
      int start = m.start();
      int end = m.end();

      TextSpan ts = new TextSpan(start, end);
      Sentence sent = new Sentence(UUID.randomUUID().toString());
      sent.setTextSpan(ts);
      sentList.add(sent);
    }
    
    return sentList;
  }
}
