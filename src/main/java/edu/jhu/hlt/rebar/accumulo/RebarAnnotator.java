/**
 * 
 */
package edu.jhu.hlt.rebar.accumulo;

import org.apache.accumulo.core.client.Connector;
import org.apache.thrift.TException;

import com.maxjthomas.dumpster.AnnotationException;
import com.maxjthomas.dumpster.Annotator;
import com.maxjthomas.dumpster.LangId;
import com.maxjthomas.dumpster.Stage;

import edu.jhu.hlt.rebar.RebarException;

/**
 * @author max
 *
 */
public class RebarAnnotator implements AutoCloseable, Annotator.Iface {

  private final Connector conn;
  private final String colFamilyAnnotationString = "anno";
  
  /**
   * @throws RebarException 
   * 
   */
  public RebarAnnotator() throws RebarException {
    this(RebarIngester.getConnector());
  }
  
  public RebarAnnotator (Connector conn) {
    this.conn = conn;
  }

  /* (non-Javadoc)
   * @see java.lang.AutoCloseable#close()
   */
  @Override
  public void close() throws Exception {
    // TODO Auto-generated method stub
    
  }

  /* (non-Javadoc)
   * @see com.maxjthomas.dumpster.Annotator.Iface#addLanguageId(java.lang.String, com.maxjthomas.dumpster.Stage, com.maxjthomas.dumpster.LangId)
   */
  @Override
  public void addLanguageId(String documentId, Stage stage, LangId lid) throws AnnotationException, TException {
    // use stage name as colQual
    String cq = stage.name;
  }
  
}
