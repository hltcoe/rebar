/**
 * 
 */
package edu.jhu.hlt.rebar.itest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.jhu.hlt.ballast.tools.BasicSituationTagger;
import edu.jhu.hlt.ballast.tools.LatinEntityTagger;
import edu.jhu.hlt.ballast.tools.SillySentenceSegmenter;
import edu.jhu.hlt.ballast.tools.SingleSectionSegmenter;
import edu.jhu.hlt.ballast.tools.TiftTokenizer;
import edu.jhu.hlt.concrete.Communication;
import edu.jhu.hlt.concrete.Section;
import edu.jhu.hlt.concrete.SectionSegmentation;
import edu.jhu.hlt.concrete.Sentence;
import edu.jhu.hlt.concrete.SentenceSegmentation;
import edu.jhu.hlt.concrete.SentenceSegmentationCollection;
import edu.jhu.hlt.concrete.TokenizationCollection;
import edu.jhu.hlt.rebar.Util;
import edu.jhu.hlt.rebar.accumulo.AbstractMiniClusterTest;
import edu.jhu.hlt.rebar.accumulo.CommunicationReader;
import edu.jhu.hlt.rebar.client.iterators.AbstractThriftIterator;
import edu.jhu.hlt.rebar.stage.AbstractStageReader;
import edu.jhu.hlt.rebar.stage.AbstractStageWriter;
import edu.jhu.hlt.rebar.stage.Stage;
import edu.jhu.hlt.rebar.stage.StageCreator;
import edu.jhu.hlt.rebar.stage.StageReader;
import edu.jhu.hlt.rebar.stage.StageType;
import edu.jhu.hlt.rebar.stage.writer.SentenceStageWriter;
import edu.jhu.hlt.rebar.stage.writer.TokenizationStageWriter;
import edu.jhu.hlt.tift.Tokenizer;


/**
 * @author max
 *
 */
public class ITestMockIntegration extends AbstractMiniClusterTest {
  
  private static final Logger logger = LoggerFactory.getLogger(ITestMockIntegration.class);
  
  SingleSectionSegmenter sss;
  SillySentenceSegmenter sentSegmenter;
  TiftTokenizer tokenizer;
  LatinEntityTagger et;
  BasicSituationTagger st;
  
  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  @Before
  public void setUp() throws Exception {
    File tmpDir = Files.createTempDirectory(Paths.get("target"), "itest").toFile();
    tmpDir.deleteOnExit();
    logger.info("Checking if configuration is appropriate.");
    this.initialize(tmpDir, "password");
    logger.info("Initialized.");
    this.sss = new SingleSectionSegmenter();
    this.sentSegmenter = new SillySentenceSegmenter();
    this.tokenizer = new TiftTokenizer(Tokenizer.WHITESPACE);
    this.et = new LatinEntityTagger();
    this.st = new BasicSituationTagger();
  }
  
  @After
  public void shutDown() throws Exception {
    this.close();
  }
  
  @Test
  public void miniClusterTest() throws Exception {
    logger.info("Beginning mini cluster test.");
    int nDocs = 5;
    Map<String, Communication> idToCommMap = this.ingestDocs(nDocs);
    
    CommunicationReader cr = new CommunicationReader(this.conn);
    try (AbstractThriftIterator<Communication> commIter = cr.getCommunications("Tweet");) {
      assertEquals("Should get " + nDocs + "ingested docs.", nDocs, Util.countIteratorResults(commIter));
      assertEquals("Shouldn't get any non-Tweets.", 0, 
          Util.countIteratorResults(cr.getCommunications("News")));
    }
    
    try (AbstractThriftIterator<Communication> commIter = cr.getCommunications("Tweet");) {
      while(commIter.hasNext()) {
        Communication c = commIter.next();
        assertTrue(idToCommMap.containsKey(c.id));
        assertTrue(idToCommMap.containsValue(c));
      }
    }
    
    Stage st = generateTestStage(StageType.SECTION);
    try (StageCreator sc = new StageCreator(this.conn);) {
      sc.create(st);
    }
    
    StageReader sr = new StageReader(this.conn);
    AbstractThriftIterator<Stage> sIter = sr.getStages();
    assertEquals("Should find the ingested stage.", st, sIter.next());
    assertTrue("Should find the ingested stage via exists method.", sr.exists(st.getName()));
    assertEquals("Should find the ingested stage via get method.", st, sr.get(st.getName()));
    sIter.close();
    
    Map<String, SectionSegmentation> idToSSMap = new HashMap<>(11);
    try (AbstractStageWriter<SectionSegmentation> retStage = sr.getSectionStageWriter(st.getName());) {
      try (AbstractThriftIterator<Communication> commIter = cr.getCommunications("Tweet");) {
        while(commIter.hasNext()) {
          Communication c = commIter.next();
          SectionSegmentation empty = sss.annotateDiff(c);
          idToSSMap.put(empty.uuid, empty);
          retStage.annotate(empty, c.id);
        }
      }
    }
    
    AbstractStageReader reader = sr.getSectionStageReader(st.getName());
    try (AbstractThriftIterator<Communication> retComms = reader.getAll();) {
      while(retComms.hasNext()) {
        Communication c = retComms.next();
        assertTrue(idToCommMap.containsKey(c.id));
        assertEquals(1, c.getSectionSegmentationsSize());
        SectionSegmentation retrieved = c.getSectionSegmentations().get(0);
        assertTrue(idToSSMap.containsKey(retrieved.uuid));
      }
    }
    
    Stage stTwo = generateTestStage("stage_another", StageType.SECTION);
    try (StageCreator sc = new StageCreator(this.conn);) {
      sc.create(stTwo);
    }
    
    assertEquals("Should find the second ingested stage via get method.", stTwo, sr.get(stTwo.getName()));
    assertTrue("Should find the ingested stage via exists method.", sr.exists(stTwo.getName()));
    assertEquals("Should find the ingested stage via get method.", stTwo, sr.get(stTwo.getName()));
    
    try (AbstractStageWriter<SectionSegmentation> retStage = sr.getSectionStageWriter(stTwo.getName());) {
      try (AbstractThriftIterator<Communication> commIter = cr.getCommunications("Tweet");) {
        for (int i = 0; i < 2; i++)
          if (commIter.hasNext())
            commIter.next();
        
        while(commIter.hasNext()) {
          Communication c = commIter.next();
          SectionSegmentation empty = sss.annotateDiff(c);
          retStage.annotate(empty, c.id);
        }
      }
    }
    
    reader = sr.getSectionStageReader(stTwo.getName());
    AbstractThriftIterator<Communication> ptr = reader.getAll();
    assertEquals("Should only get " + (nDocs - 2) + " docs annotated in S2.", 
        nDocs - 2, Util.countIteratorResults(ptr));
    ptr.close();
    
    try (AbstractThriftIterator<Communication> retComms = reader.getAll();) {
      while(retComms.hasNext()) {
        Communication c = retComms.next();
        assertTrue(idToCommMap.containsKey(c.id));
        assertEquals(1, c.getSectionSegmentationsSize());
      }
    } 
    
    Set<String> deps = new HashSet<>();
    deps.add(st.getName());
    Stage sentStage = generateTestStage("stage_sent", StageType.SENTENCE, deps);

    try (StageCreator sc = new StageCreator(this.conn);) {
      sc.create(sentStage);
    }
    
    assertEquals("Should find the third ingested stage via get method.", sentStage, sr.get(sentStage.getName()));
    assertTrue("Should find the ingested stage via exists method.", sr.exists(sentStage.getName()));
    assertEquals("Should find the ingested stage via get method.", sentStage, sr.get(sentStage.getName()));
    assertEquals("SentenceStage should have 1 dependency.", 1, sr.get(sentStage.getName()).getDependencies().size());
    
    List<String> depList = new ArrayList<>(sentStage.getDependencies());
    try(AbstractStageWriter<SentenceSegmentationCollection> writer = new SentenceStageWriter(this.conn, sentStage);) {
      reader = sr.getSectionStageReader(depList.get(0));
      try (AbstractThriftIterator<Communication> retComms = reader.getAll();) {
        while (retComms.hasNext()) {
          Communication c = retComms.next();
          assertTrue(c.isSetSectionSegmentations() && !c.getSectionSegmentations().isEmpty());
          SentenceSegmentationCollection coll = this.sentSegmenter.annotateDiff(c);
          writer.annotate(coll, c.getId());
        }
      }
    }
    
    reader = sr.getSentenceStageReader(sentStage.getName());
    try (AbstractThriftIterator<Communication> retComms = reader.getAll();) {
      assertEquals("Should get " + nDocs + " docs annotated in sent stage.", 
          nDocs, Util.countIteratorResults(retComms));
    }

    try (AbstractThriftIterator<Communication> retComms = reader.getAll();) {
      while(retComms.hasNext()) {
        Communication c = retComms.next();
        assertTrue(idToCommMap.containsKey(c.id));
        assertEquals(1, c.getSectionSegmentationsSize());
        SectionSegmentation ss = c.getSectionSegmentations().get(0);
        assertTrue(ss.getSectionList() != null);
        for (Section sect : ss.getSectionList()) {
          assertTrue(sect.isSetSentenceSegmentation());
          assertEquals(1, sect.getSentenceSegmentation().size());
        }
      }
    }
    
    Set<String> tokDeps = new HashSet<>();
    tokDeps.add(st.getName());
    tokDeps.add(sentStage.getName());
    Stage tokStage = generateTestStage("stage_tok", StageType.TOKENIZATION, tokDeps);

    try (StageCreator sc = new StageCreator(this.conn);) {
      sc.create(tokStage);
    }
    
    assertEquals("Should find the third ingested stage via get method.", tokStage, sr.get(tokStage.getName()));
    assertTrue("Should find the ingested stage via exists method.", sr.exists(tokStage.getName()));
    assertEquals("Should find the ingested stage via get method.", tokStage, sr.get(tokStage.getName()));
    assertEquals("TokenizationStage should have 2 dependencies.", 2, sr.get(tokStage.getName()).getDependencies().size());
    
    depList = new ArrayList<>(tokStage.getDependencies());
    try(AbstractStageWriter<TokenizationCollection> writer = new TokenizationStageWriter(this.conn, tokStage);) {
      reader = sr.getSentenceStageReader(depList.get(1));
      
      try (AbstractThriftIterator<Communication> retComms = reader.getAll();) {
        while (retComms.hasNext()) {
          Communication c = retComms.next();
          assertTrue(c.isSetSectionSegmentations() && !c.getSectionSegmentations().isEmpty());
          for (SectionSegmentation ss : c.getSectionSegmentations()) {
            assertTrue(ss.isSetSectionList());
            assertTrue(ss.getSectionListSize()> 0);
            for (Section sect : ss.getSectionList()) {
              assertTrue(sect.isSetSentenceSegmentation());
              assertTrue(sect.getSentenceSegmentationSize() > 0);
              for (SentenceSegmentation sentSeg : sect.getSentenceSegmentation()) {
                assertTrue(sentSeg.isSetSentenceList());
                assertTrue(sentSeg.getSentenceListSize() > 0);
              }
            }
          }
          
          TokenizationCollection coll = this.tokenizer.annotateDiff(c);
          writer.annotate(coll, c.getId());
        }
      }
    }
    
    reader = sr.getTokenizationStageReader(tokStage.getName());
    try(AbstractThriftIterator<Communication> retrComms = reader.getAll();) {
      assertEquals("Should get " + nDocs + " docs annotated in tok stage.", 
          nDocs, Util.countIteratorResults(retrComms));
    }
    
    try(AbstractThriftIterator<Communication> retrComms = reader.getAll();) {
      while(retrComms.hasNext()) {
        Communication c = retrComms.next();
        assertTrue(idToCommMap.containsKey(c.id));
        assertEquals(1, c.getSectionSegmentationsSize());
        SectionSegmentation ss = c.getSectionSegmentations().get(0);
        assertTrue(ss.getSectionList() != null);
        for (Section sect : ss.getSectionList()) {
          assertTrue(sect.isSetSentenceSegmentation());
          assertEquals(1, sect.getSentenceSegmentation().size());
          for (SentenceSegmentation sentSeg : sect.getSentenceSegmentation()) {
            assertTrue(sentSeg.isSetSentenceList());
            assertTrue(sentSeg.getSentenceListSize() > 0);
            for (Sentence sent : sentSeg.getSentenceList()) {
              assertTrue(sent.isSetTokenizationList());
              assertTrue(sent.getTokenizationListSize() > 0);
            }
          }
        }
      }
    }
  }
}
