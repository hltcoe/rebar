/**
 * 
 */
package edu.jhu.hlt.rebar.stages;

import org.apache.accumulo.core.client.Connector;

import edu.jhu.hlt.asphalt.Stage;
import edu.jhu.hlt.concrete.SentenceSegmentation;
import edu.jhu.hlt.rebar.AnnotationException;
import edu.jhu.hlt.rebar.Constants;
import edu.jhu.hlt.rebar.RebarException;

/**
 * @author max
 *
 */
public class SentenceStage extends AbstractStageWriter<SentenceSegmentation> {

  /**
   * @param conn
   * @param stage
   * @throws RebarException
   */
  public SentenceStage(Connector conn, Stage stage) throws RebarException {
    super(conn, stage);
    // TODO Auto-generated constructor stub
  }
  
  public SentenceStage(Stage stage) throws RebarException {
    this(Constants.getConnector(), stage);
  }

  /* (non-Javadoc)
   * @see edu.jhu.hlt.rebar.stages.AbstractStage#getDocuments()
   */
//  @Override
//  public Iterator<Communication> getDocuments() throws RebarException {
//    // TODO Auto-generated method stub
//    return null;
//  }

  /* (non-Javadoc)
   * @see edu.jhu.hlt.rebar.stages.AbstractStage#annotate(org.apache.thrift.TBase, java.lang.String)
   */
  @Override
  public void annotate(SentenceSegmentation annotation, String docId) throws RebarException, AnnotationException {
    // TODO Auto-generated method stub
  }
}
