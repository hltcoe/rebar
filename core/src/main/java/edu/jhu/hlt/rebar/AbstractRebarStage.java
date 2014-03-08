/**
 * 
 */
package edu.jhu.hlt.rebar;

import org.apache.accumulo.core.client.Connector;
import org.apache.thrift.TBase;
import org.apache.thrift.TFieldIdEnum;

import edu.jhu.hlt.asphalt.Stage;
import edu.jhu.hlt.rebar.accumulo.AbstractCommunicationWriter;
import edu.jhu.hlt.rebar.accumulo.CommunicationReader;

/**
 * @author max
 *
 */
public abstract class AbstractRebarStage<T extends TBase<T, ? extends TFieldIdEnum>> extends AbstractCommunicationWriter {

  protected final Stage stage;
  protected final CommunicationReader reader;
  
  /**
   * @throws RebarException 
   * 
   */
  public AbstractRebarStage(Stage stage) throws RebarException {
    this(Constants.getConnector(), stage);
  }
  
  /**
   * @throws RebarException 
   * 
   */
  public AbstractRebarStage(Connector conn, Stage stage) throws RebarException {
    super(conn);
    this.stage = stage;
    this.reader = new CommunicationReader(conn);
  }

  public abstract void annotate(T annotation, String docId) throws AnnotationException, RebarException;
}
