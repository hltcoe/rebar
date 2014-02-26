package edu.jhu.hlt.rebar.accumulo;

import static org.junit.Assert.fail;

import java.util.HashSet;
import java.util.List;

import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.jhu.hlt.concrete.Communication;
import edu.jhu.hlt.concrete.Stage;
import edu.jhu.hlt.concrete.StageType;
import edu.jhu.hlt.rebar.RebarException;

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
//    fail("Not yet implemented");
//    int nDocs = 25;
//    List<Communication> commList = this.ingestDocuments(nDocs);
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
