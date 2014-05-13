/**
 * 
 */
package edu.jhu.hlt.rebar.stage.writer;

import org.apache.accumulo.core.client.Connector;

import edu.jhu.hlt.concrete.SectionSegmentation;
import edu.jhu.hlt.grommet.Stage;
import edu.jhu.hlt.rebar.AnnotationException;
import edu.jhu.hlt.rebar.Constants;
import edu.jhu.hlt.rebar.RebarException;
import edu.jhu.hlt.rebar.annotations.AbstractRebarAnnotation;
import edu.jhu.hlt.rebar.annotations.RebarSectionSegmentation;
import edu.jhu.hlt.rebar.stage.AbstractStageWriter;

/**
 * @author max
 *
 */
public class SectionStageWriter extends AbstractStageWriter<SectionSegmentation> {
  
  public SectionStageWriter(Connector conn, Stage stage) throws RebarException {
    super(conn, stage);
  }

  public SectionStageWriter(Stage stage) throws RebarException {
    this(Constants.getConnector(), stage);
  }
  
  public void annotate(SectionSegmentation ss, String docId) throws RebarException, AnnotationException {
    AbstractRebarAnnotation<SectionSegmentation> rss = new RebarSectionSegmentation(ss);
    this.annotate(rss, docId);
  }
}
