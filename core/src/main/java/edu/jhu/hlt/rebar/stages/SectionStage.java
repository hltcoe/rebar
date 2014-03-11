/**
 * 
 */
package edu.jhu.hlt.rebar.stages;

import java.util.Iterator;

import org.apache.accumulo.core.client.Connector;

import edu.jhu.hlt.asphalt.Stage;
import edu.jhu.hlt.concrete.Communication;
import edu.jhu.hlt.concrete.SectionSegmentation;
import edu.jhu.hlt.rebar.AnnotationException;
import edu.jhu.hlt.rebar.Constants;
import edu.jhu.hlt.rebar.RebarException;
import edu.jhu.hlt.rebar.annotations.AbstractRebarAnnotation;
import edu.jhu.hlt.rebar.annotations.RebarSectionSegmentation;

/**
 * @author max
 *
 */
public class SectionStage extends AbstractStageWriter<SectionSegmentation> {
  
  public SectionStage(Connector conn, Stage stage) throws RebarException {
    super(conn, stage);
  }

  public SectionStage(Stage stage) throws RebarException {
    this(Constants.getConnector(), stage);
  }
  
  public void annotate(SectionSegmentation ss, String docId) throws RebarException, AnnotationException {
    AbstractRebarAnnotation<SectionSegmentation> rss = new RebarSectionSegmentation(ss);
    this.annotate(rss, docId);
  }

//  @Override
//  public Iterator<Communication> getDocuments() throws RebarException {
////    Iterator<Entry<Key, Value>> iter = this.
////    return new SectionCommunicationReader(this.conn).mergedIterator(this.stage.name);
//    return null;
//  }
}
