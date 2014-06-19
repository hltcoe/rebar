/*
 * 
 */
package edu.jhu.hlt.rebar.stage.writer;

import org.apache.accumulo.core.client.Connector;

import edu.jhu.hlt.concrete.SectionSegmentation;
import edu.jhu.hlt.concrete.validation.AbstractAnnotation;
import edu.jhu.hlt.concrete.validation.ValidatableSectionSegmentation;
import edu.jhu.hlt.rebar.AnnotationException;
import edu.jhu.hlt.rebar.Constants;
import edu.jhu.hlt.rebar.RebarException;
import edu.jhu.hlt.rebar.stage.AbstractStageWriter;
import edu.jhu.hlt.rebar.stage.Stage;

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
    AbstractAnnotation<SectionSegmentation> rss = new ValidatableSectionSegmentation(ss);
    this.annotate(rss, docId);
  }
}
