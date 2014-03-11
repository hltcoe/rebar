package edu.jhu.hlt.rebar.accumulo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
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
import edu.jhu.hlt.concrete.SectionSegmentation;
import edu.jhu.hlt.rebar.Util;
import edu.jhu.hlt.rebar.annotations.RebarSectionSegmentation;
import edu.jhu.hlt.rebar.annotations.SingleSectionSegmenter;
import edu.jhu.hlt.rebar.stages.AbstractStage;
import edu.jhu.hlt.rebar.stages.StageCreator;
import edu.jhu.hlt.rebar.stages.StageReader;

public class IMemoryIntegrationTest extends AbstractAccumuloTest {

  private static final Logger logger = LoggerFactory.getLogger(IMemoryIntegrationTest.class);
  SingleSectionSegmenter sss;
  
  @Before
  public void setUp() throws Exception {
    this.initialize();
    this.sss = new SingleSectionSegmenter();
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
    assertEquals("Should find the ingested stage via get method.", st, sr.get(st.name));
    
    Map<String, SectionSegmentation> idToSSMap = new HashMap<>(11);
    try (AbstractStage<SectionSegmentation> retStage = sr.retrieveSectionStage(st.name);) {
      commIter = cr.getCommunications(CommunicationType.TWEET);
      while(commIter.hasNext()) {
        Communication c = commIter.next();
        SectionSegmentation empty = sss.section(c);
        idToSSMap.put(empty.uuid, empty);
        retStage.annotate(empty, c.id);
      }
      
      Iterator<Communication> retComms = retStage.getDocuments();
      while(retComms.hasNext()) {
        Communication c = retComms.next();
        assertTrue(idToCommMap.containsKey(c.id));
        assertEquals(1, c.getSectionSegmentationsSize());
        SectionSegmentation retrieved = c.getSectionSegmentations().get(0);
        assertTrue(idToSSMap.containsKey(retrieved.uuid));
      }
    }
//    Util.printTable(Constants.DOCUMENT_TABLE_NAME, this.conn, logger);
//    
//    
//    
    Stage stTwo = generateTestStage().setType(StageType.SECTION).setName("another_stage");
    try (StageCreator sc = new StageCreator(this.conn);) {
      sc.create(stTwo);
    }
    
    assertEquals("Should find the second ingested stage via get method.", stTwo, sr.get(stTwo.name));
    
    try (AbstractStage<SectionSegmentation> retStage = sr.retrieveSectionStage(stTwo.name);) {
      commIter = cr.getCommunications(CommunicationType.TWEET);
      for (int i = 0; i < 2; i++)
        if (commIter.hasNext())
          commIter.next();
      
      while(commIter.hasNext()) {
        Communication c = commIter.next();
        SectionSegmentation empty = sss.section(c);
        retStage.annotate(empty, c.id);
      }
      
      assertEquals("Should only get " + (nDocs - 2) + " docs annotated in S2.", 
          nDocs - 2, Util.countIteratorResults(retStage.getDocuments()));
      Iterator<Communication> retComms = retStage.getDocuments();
      while(retComms.hasNext()) {
        Communication c = retComms.next();
        assertTrue(idToCommMap.containsKey(c.id));
        assertEquals(1, c.getSectionSegmentationsSize());
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
