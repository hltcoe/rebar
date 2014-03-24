package edu.jhu.hlt.rebar.accumulo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.jhu.hlt.asphalt.Stage;
import edu.jhu.hlt.asphalt.StageType;
import edu.jhu.hlt.concrete.Communication;
import edu.jhu.hlt.concrete.CommunicationType;
import edu.jhu.hlt.concrete.Section;
import edu.jhu.hlt.concrete.SectionSegmentation;
import edu.jhu.hlt.concrete.Sentence;
import edu.jhu.hlt.concrete.SentenceSegmentation;
import edu.jhu.hlt.concrete.SentenceSegmentationCollection;
import edu.jhu.hlt.concrete.util.SuperTextSpan;
import edu.jhu.hlt.rebar.Util;
import edu.jhu.hlt.rebar.ballast.tools.SillySentenceSegmenter;
import edu.jhu.hlt.rebar.ballast.tools.SingleSectionSegmenter;
import edu.jhu.hlt.rebar.stage.AbstractStageReader;
import edu.jhu.hlt.rebar.stage.AbstractStageWriter;
import edu.jhu.hlt.rebar.stage.StageCreator;
import edu.jhu.hlt.rebar.stage.StageReader;
import edu.jhu.hlt.rebar.stage.writer.SentenceStageWriter;

public class IMemoryIntegrationTest extends AbstractAccumuloTest {

  private static final Logger logger = LoggerFactory.getLogger(IMemoryIntegrationTest.class);
  SingleSectionSegmenter sss;
  SillySentenceSegmenter sentSegmenter;
  
  @Before
  public void setUp() throws Exception {
    this.initialize();
    this.sss = new SingleSectionSegmenter();
    this.sentSegmenter = new SillySentenceSegmenter();
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void bigIntegrationTest() throws Exception {
    int nDocs = 5;
    Set<Communication> commSet = generateMockDocumentSet(nDocs);
    Map<String, Communication> idToCommMap = new HashMap<>(nDocs + 1);
    List<Communication> commList = new ArrayList<>(commSet);
    for (Communication c : commList)
      idToCommMap.put(c.id, c);
    
    try (CleanIngester ci = new CleanIngester(this.conn);) {
      for (Communication c : commList) {
        ci.ingest(c);
      }
    }
    
    CommunicationReader cr = new CommunicationReader(this.conn);
    Iterator<Communication> commIter = cr.getCommunications(CommunicationType.TWEET);
    assertEquals("Should get " + nDocs + "ingested docs.", nDocs, Util.countIteratorResults(commIter));
    
    commIter = cr.getCommunications(CommunicationType.TWEET);
    while(commIter.hasNext()) {
      Communication c = commIter.next();
      assertTrue(idToCommMap.containsKey(c.id));
      assertTrue(idToCommMap.containsValue(c));
    }
    
    Stage st = generateTestStage().setType(StageType.SECTION);
    try (StageCreator sc = new StageCreator(this.conn);) {
      sc.create(st);
    }
    
    StageReader sr = new StageReader(this.conn);
    assertEquals("Should find the ingested stage.", st, sr.getStages().next());
    assertTrue("Should find the ingested stage via exists method.", sr.exists(st.name));
    assertEquals("Should find the ingested stage via get method.", st, sr.get(st.name));
    
    Map<String, SectionSegmentation> idToSSMap = new HashMap<>(11);
    try (AbstractStageWriter<SectionSegmentation> retStage = sr.getSectionStageWriter(st.name);) {
      commIter = cr.getCommunications(CommunicationType.TWEET);
      while(commIter.hasNext()) {
        Communication c = commIter.next();
        SectionSegmentation empty = sss.annotate(c);
        idToSSMap.put(empty.uuid, empty);
        retStage.annotate(empty, c.id);
      }
    }
    
    AbstractStageReader reader = sr.getSectionStageReader(st.name);
    Iterator<Communication> retComms = reader.getAll();
    while(retComms.hasNext()) {
      Communication c = retComms.next();
      assertTrue(idToCommMap.containsKey(c.id));
      assertEquals(1, c.getSectionSegmentationsSize());
      SectionSegmentation retrieved = c.getSectionSegmentations().get(0);
      assertTrue(idToSSMap.containsKey(retrieved.uuid));
    }

    Stage stTwo = generateTestStage().setType(StageType.SECTION).setName("another_stage");
    try (StageCreator sc = new StageCreator(this.conn);) {
      sc.create(stTwo);
    }
    
    assertEquals("Should find the second ingested stage via get method.", stTwo, sr.get(stTwo.name));
    assertTrue("Should find the ingested stage via exists method.", sr.exists(stTwo.name));
    assertEquals("Should find the ingested stage via get method.", stTwo, sr.get(stTwo.name));
    
    try (AbstractStageWriter<SectionSegmentation> retStage = sr.getSectionStageWriter(stTwo.name);) {
      commIter = cr.getCommunications(CommunicationType.TWEET);
      for (int i = 0; i < 2; i++)
        if (commIter.hasNext())
          commIter.next();
      
      while(commIter.hasNext()) {
        Communication c = commIter.next();
        SectionSegmentation empty = sss.annotate(c);
        retStage.annotate(empty, c.id);
      }
    }
    
    reader = sr.getSectionStageReader(stTwo.name);
    assertEquals("Should only get " + (nDocs - 2) + " docs annotated in S2.", 
        nDocs - 2, Util.countIteratorResults(reader.getAll()));
    retComms = reader.getAll();
    while(retComms.hasNext()) {
      Communication c = retComms.next();
      assertTrue(idToCommMap.containsKey(c.id));
      assertEquals(1, c.getSectionSegmentationsSize());
    }
    
    Stage sentStage = generateTestStage().setType(StageType.SENTENCE).setName("sent_stage");
    Set<String> deps = new HashSet<>();
    deps.add(st.name);
    sentStage.dependencies = deps;
    try (StageCreator sc = new StageCreator(this.conn);) {
      sc.create(sentStage);
    }
    
    assertEquals("Should find the third ingested stage via get method.", sentStage, sr.get(sentStage.name));
    assertTrue("Should find the ingested stage via exists method.", sr.exists(sentStage.name));
    assertEquals("Should find the ingested stage via get method.", sentStage, sr.get(sentStage.name));
    assertEquals("SentenceStage should have 1 dependency.", 1, sr.get(sentStage.name).dependencies.size());
    
    List<String> depList = new ArrayList<>(sentStage.getDependencies());
    try(AbstractStageWriter<SentenceSegmentationCollection> writer = new SentenceStageWriter(this.conn, sentStage);) {
      reader = sr.getSectionStageReader(depList.get(0));
      retComms = reader.getAll();
      while (retComms.hasNext()) {
        Communication c = retComms.next();
        assertTrue(c.isSetSectionSegmentations() && !c.getSectionSegmentations().isEmpty());
        SentenceSegmentationCollection coll = this.sentSegmenter.annotate(c);
        writer.annotate(coll, c.getId());
      }
    }
    
    reader = sr.getSentenceStageReader(sentStage.name);
    assertEquals("Should get " + nDocs + " docs annotated in sent stage.", 
        nDocs, Util.countIteratorResults(reader.getAll()));
    retComms = reader.getAll();
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
    
//    
//    Stage sectionSegmentationStage = generateTestStage("sect_seg_stage", "Section segmentation stage.", new HashSet<String>(), StageType.SECTION);
//    try (RebarAnnotator ra = new RebarAnnotator(this.conn);) {
//      for (Communication c : commList) {
//        SectionSegm
//        ra.addSectionSegmentation(c, sectionSegmentationStage, sectionSegmentation);
//      }
//    }
  }
}
