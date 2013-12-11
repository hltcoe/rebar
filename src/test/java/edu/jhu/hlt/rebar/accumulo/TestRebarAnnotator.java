/**
 * 
 */
package edu.jhu.hlt.rebar.accumulo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import concrete.examples.RebarTokenizer;
import concrete.examples.SingleSectionSegmentator;
import concrete.examples.scala.SillySentenceSplitter;
import concrete.examples.scala.SingleSectionSegmenter;
import edu.jhu.hlt.concrete.Communication;
import edu.jhu.hlt.concrete.LangId;
import edu.jhu.hlt.concrete.RebarThriftException;
import edu.jhu.hlt.concrete.Section;
import edu.jhu.hlt.concrete.SectionSegmentation;
import edu.jhu.hlt.concrete.SentenceSegmentationCollection;
import edu.jhu.hlt.concrete.Stage;
import edu.jhu.hlt.concrete.StageType;
import edu.jhu.hlt.concrete.TokenizationCollection;
import edu.jhu.hlt.rebar.Configuration;
import edu.jhu.hlt.rebar.Constants;
import edu.jhu.hlt.rebar.RebarException;
import edu.jhu.hlt.rebar.Util;

/**
 * @author max
 * 
 */
public class TestRebarAnnotator extends AbstractAccumuloTest {

  private Set<Communication> docSet;
  private RebarAnnotator ra;

  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {
    this.initialize();
    this.ra = new RebarAnnotator(conn);

    docSet = new HashSet<>();
    RebarIngester ri = new RebarIngester(this.conn);
    for (int i = 0; i < 10; i++) {
      Communication d = TestRebarIngester.generateMockDocument();
      ri.ingest(d);
      docSet.add(d);
    }

    ri.close();
  }

  /**
   * @throws java.lang.Exception
   */
  @After
  public void tearDown() throws Exception {
    this.ra.close();
  }

  @Test
  public void testAnnotateDocumentNoStage() throws Exception {
    // Stage newStage = new Stage("stage_max_lid_test", "Testing stage for LID", Util.getCurrentUnixTime(), new HashSet<String>());
    Stage newStage = TestRebarStageHandler.generateTestStage();

    List<LangId> lidList = new ArrayList<>();
    for (Communication d : this.docSet) {
      LangId lid = generateLangId(d);
      lidList.add(lid);
      this.ra.addLanguageId(d, newStage, lid);
    }

    Iterator<Entry<Key, Value>> iter = TestRebarIngester.generateIterator(conn, Constants.DOCUMENT_TABLE_NAME, new Range());
    assertEquals("Should get 20 total rows.", 20, Util.countIteratorResults(iter));
    try (RebarStageHandler ashy = new RebarStageHandler(this.conn);) {
      Set<Stage> stageSet = ashy.getStages();
      assertTrue("Should get a stage added.", stageSet.size() > 0);
    }
  }

  @Test(expected = RebarThriftException.class)
  public void testAnnotateDocumentTwice() throws Exception {
    Stage newStage = TestRebarStageHandler.generateTestStage();

    List<LangId> lidList = new ArrayList<>();
    for (Communication d : this.docSet) {
      LangId lid = generateLangId(d);
      lidList.add(lid);
      this.ra.addLanguageId(d, newStage, lid);
      this.ra.addLanguageId(d, newStage, lid);
    }
  }

  @Test(expected = RebarThriftException.class)
  public void testAnnotateWrongType() throws Exception {
    Stage newStage = TestRebarStageHandler.generateTestStage();
    newStage.type = StageType.LANG_PRED;

    List<LangId> lidList = new ArrayList<>();
    for (Communication d : this.docSet) {
      LangId lid = generateLangId(d);
      lidList.add(lid);
      this.ra.addLanguageId(d, newStage, lid);
    }
  }

  @Test
  public void testAnnotateDocumentWithLID() throws Exception {
    // Stage newStage = new Stage("stage_max_lid_test", "Testing stage for LID", Util.getCurrentUnixTime(), new HashSet<String>());
    Stage newStage = TestRebarStageHandler.generateTestStage();

    try (RebarStageHandler ash = new RebarStageHandler(conn);) {
      ash.createStage(newStage);
    }

    List<LangId> lidList = new ArrayList<>();
    for (Communication d : this.docSet) {
      LangId lid = generateLangId(d);
      lidList.add(lid);
      this.ra.addLanguageId(d, newStage, lid);
    }

    Stage stageTwo = generateTestStage();
    stageTwo.name = "stage_two_test";
    List<LangId> lidListTwo = new ArrayList<>();
    for (Communication d : this.docSet) {
      LangId lid = generateLangId(d);
      lidListTwo.add(lid);
      this.ra.addLanguageId(d, stageTwo, lid);
    }

    Iterator<Entry<Key, Value>> iter = TestRebarIngester.generateIterator(conn, Constants.DOCUMENT_TABLE_NAME, new Range());
    assertEquals("Should get 30 total rows.", 30, Util.countIteratorResults(iter));

    Scanner sc = this.conn.createScanner(Constants.DOCUMENT_TABLE_NAME, Configuration.getAuths());
    sc.setRange(new Range());
    sc.fetchColumn(new Text(Constants.DOCUMENT_ANNOTATION_COLF), new Text(newStage.name));
    iter = sc.iterator();
    assertEquals("Should get 10 annotations: ", 10, Util.countIteratorResults(iter));

    List<Communication> docList = new ArrayList<>(docSet);
    for (int i = 0; i < 10; i++) {
      Communication d = docList.get(i);
      LangId lid = lidList.get(i);
      LangId lidTwo = lidListTwo.get(i);
      String id = d.id;
      Range r = new Range(id);
      iter = TestRebarIngester.generateIterator(conn, Constants.DOCUMENT_TABLE_NAME, r);
      while (iter.hasNext()) {
        Entry<Key, Value> e = iter.next();
        Key k = e.getKey();
        if (k.compareColumnFamily(new Text(Constants.DOCUMENT_COLF)) == 0) {
          Communication dser = new Communication();
          this.deserializer.deserialize(dser, e.getValue().get());
          assertEquals("Should get a document from document colf.", d, dser);
        } else if (k.compareColumnQualifier(new Text(newStage.name)) == 0) {
          LangId dser = new LangId();
          this.deserializer.deserialize(dser, e.getValue().get());
          assertEquals("Should get a LID from annotation colf.", lid, dser);
        } else if (k.compareColumnQualifier(new Text(stageTwo.name)) == 0) {
          LangId dser = new LangId();
          this.deserializer.deserialize(dser, e.getValue().get());
          assertEquals("Should get a LID from annotation colf.", lidTwo, dser);
        } else {
          fail("Column family was bad: " + k.getColumnFamily().toString());
        }
      }
    }
  }

  @Test
  public void addSectionsSentencesTokenizationsMentionsEntities() throws RebarException, Exception {
    String secStageName = "stage_sections_v1";
    Stage secStage = TestRebarStageHandler.generateTestStage(secStageName, "Sections stage", new HashSet<String>(), StageType.SECTION);

    for (Communication c : this.docSet) {
      SectionSegmentation ss = SingleSectionSegmenter.sectionCommunication(c);
      this.ra.addSectionSegmentation(c, secStage, ss);
    }

    Set<String> sentDeps = new HashSet<>();
    sentDeps.add(secStageName);
    String sentStageName = "stage_sentences_v1";
    Stage sentStage = TestRebarStageHandler.generateTestStage(sentStageName, "Sentences stage", sentDeps, StageType.SENTENCE);

    SillySentenceSplitter splitta = new SillySentenceSplitter();
    List<Communication> commsWithSections;
    try (RebarReader rr = new RebarReader(conn);) {
      commsWithSections = new ArrayList<>(rr.getAnnotatedCommunications(secStage));
    }

    for (Communication c : commsWithSections) {
      // SectionSegmentation ss = sss.generateSectionSegmentation(c);
      SentenceSegmentationCollection sentSegs = splitta.splitSentences(c);
      this.ra.addSentenceSegmentations(c, sentStage, sentSegs);
    }

    Iterator<Entry<Key, Value>> iter = TestRebarIngester.generateIterator(conn, Constants.DOCUMENT_TABLE_NAME, new Range());
    assertEquals("Should get 30 total rows.", 30, Util.countIteratorResults(iter));

    List<Communication> commsWithSents;
    try (RebarReader rr = new RebarReader(conn);) {
      commsWithSents = rr.getAnnotatedCommunications(sentStage);
    }

    assertEquals("Should get n comms with sentences: ", this.docSet.size(), commsWithSents.size());
    for (Communication c : commsWithSents) {
      SectionSegmentation ss = c.getSectionSegmentation();
      List<Section> sList = ss.getSectionList();
      assertEquals(1, sList.size());

      for (Section s : sList) {
        String id = s.getUuid().getId();
        assertEquals(id, s.getSentenceSegmentation().getSectionId().getId());
      }
    }
    
    Set<String> tokDeps = new HashSet<>();
    tokDeps.add(sentStageName);
    tokDeps.add(secStageName);
    String tokStageName = "stage_tokens_v1";
    Stage tokStage = TestRebarStageHandler.generateTestStage(tokStageName, "Tokens stage", tokDeps, StageType.TOKENIZATION);

    RebarTokenizer rt = new RebarTokenizer();
    for (Communication c : commsWithSents) {
      // SectionSegmentation ss = sss.generateSectionSegmentation(c);
      TokenizationCollection tc = rt.tokenize(c);
      this.ra.addTokenizations(c, tokStage, tc);
    }
    
    iter = TestRebarIngester.generateIterator(conn, Constants.DOCUMENT_TABLE_NAME, new Range());
    assertEquals("Should get 40 total rows.", 40, Util.countIteratorResults(iter));
    
    List<Communication> commsWithToks;
    try (RebarReader rr = new RebarReader(conn);) {
      commsWithToks = rr.getAnnotatedCommunications(tokStage);
    }
    
    
  }
}
