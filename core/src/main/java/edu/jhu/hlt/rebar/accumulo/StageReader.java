/**
 * 
 */
package edu.jhu.hlt.rebar.accumulo;

import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import edu.jhu.hlt.asphalt.Stage;
import edu.jhu.hlt.rebar.Constants;
import edu.jhu.hlt.rebar.RebarException;

/**
 * @author max
 *
 */
public class StageReader extends AbstractReader<Stage> {

  /**
   * 
   */
  public StageReader() throws RebarException {
    this(Constants.getConnector());
  }

  /**
   * @param conn
   * @throws RebarException
   */
  public StageReader(Connector conn) throws RebarException {
    super(conn, Constants.STAGES_TABLE_NAME, Constants.STAGES_IDX_TABLE_NAME);
    // TODO Auto-generated constructor stub
  }

  @Override
  protected Iterator<Stage> accumuloIterToTIter(Iterator<Entry<Key, Value>> accIter) throws RebarException {
    // TODO Auto-generated method stub
    return null;
  }

}
