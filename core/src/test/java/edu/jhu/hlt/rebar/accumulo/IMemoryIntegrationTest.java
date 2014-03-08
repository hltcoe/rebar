package edu.jhu.hlt.rebar.accumulo;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.jhu.hlt.asphalt.Stage;
import edu.jhu.hlt.concrete.Communication;
import edu.jhu.hlt.concrete.CommunicationType;
import edu.jhu.hlt.rebar.Util;
import edu.jhu.hlt.rebar.stages.StageCreator;
import edu.jhu.hlt.rebar.stages.StageReader;

public class IMemoryIntegrationTest extends AbstractAccumuloTest {

  @Before
  public void setUp() throws Exception {
    this.initialize();
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void bigIntegrationTest() throws Exception {
    int nDocs = 25;
    Set<Communication> commSet = generateMockDocumentSet(nDocs);
    List<Communication> commList = new ArrayList<>(commSet);
    
    try (CleanIngester ci = new CleanIngester(this.conn);) {
      for (Communication c : commList) {
        ci.ingest(c);
      }
    }
    
    CommunicationReader cr = new CommunicationReader(this.conn);
    Iterator<Communication> commIter = cr.getCommunications(CommunicationType.TWEET);

    assertEquals("Should get " + nDocs + "ingested docs.", nDocs, Util.countIteratorResults(commIter));
    
    Stage st = generateTestStage();
    try (StageCreator sc = new StageCreator(this.conn);) {
      sc.create(st);
    }
    
    StageReader sr = new StageReader(this.conn);
    assertEquals("Should find the ingested stage.", st, sr.getStages().next());
    
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
