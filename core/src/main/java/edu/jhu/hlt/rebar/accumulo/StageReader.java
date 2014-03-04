/**
 * 
 */
package edu.jhu.hlt.rebar.accumulo;

import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.thrift.TException;

import edu.jhu.hlt.asphalt.Stage;
import edu.jhu.hlt.asphalt.StageType;
import edu.jhu.hlt.rebar.Configuration;
import edu.jhu.hlt.rebar.Constants;
import edu.jhu.hlt.rebar.RebarException;

/**
 * @author max
 *
 */
public class StageReader extends AbstractReader<Stage> {

  /**
   * 
   * @throws RebarException
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
  }

  @Override
  protected Iterator<Stage> accumuloIterToTIter(Iterator<Entry<Key, Value>> accIter) throws RebarException {
    return new AbstractThriftIterator<Stage>(accIter) {
      @Override
      public Stage next() {
        try {
          Stage c = new Stage();
          deser.deserialize(c, iter.next().getValue().get());
          return c;
        } catch (TException e) {
          throw new RuntimeException(e);
        }
      }
    };
  }
  
  public boolean exists(Stage s) throws RebarException {
    return this.exists(s.name);
  }
  
  public boolean exists (String stageName) throws RebarException {
    try {
      Scanner sc = this.conn.createScanner(this.tableName, Configuration.getAuths());
      Range r = new Range(stageName);
      sc.setRange(r);
      return sc.iterator().hasNext();
    } catch (TableNotFoundException e) {
      throw new RebarException(e);
    }
  }
  
  public Iterator<Stage> getStages(StageType t) throws RebarException {
    Range r = new Range("type:"+t.toString());
    return this.rangeToIter(r);
  }
}
