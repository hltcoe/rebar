/**
 * 
 */
package edu.jhu.hlt.rebar.stages;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.apache.thrift.TException;

import edu.jhu.hlt.asphalt.Stage;
import edu.jhu.hlt.asphalt.StageType;
import edu.jhu.hlt.concrete.SectionSegmentation;
import edu.jhu.hlt.rebar.Configuration;
import edu.jhu.hlt.rebar.Constants;
import edu.jhu.hlt.rebar.RebarException;
import edu.jhu.hlt.rebar.accumulo.AbstractReader;
import edu.jhu.hlt.rebar.accumulo.AbstractThriftIterator;

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
  
  @Override
  protected Iterator<Entry<Key, Value>> batchScanMainTable(Collection<Range> ids) throws RebarException {
    BatchScanner docsc = null;
    try {
      docsc = this.conn.createBatchScanner(this.tableName, Configuration.getAuths(), 8);
      docsc.setRanges(ids);
      docsc.fetchColumnFamily(new Text(Constants.STAGES_OBJ_COLF));
      return docsc.iterator();
    } catch (TableNotFoundException e) {
      throw new RebarException(e);
    } finally {
      if (docsc != null)
        docsc.close();
    }
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
  
  public Stage get (String stageName) throws RebarException {
    if (this.exists(stageName)) {
      try {
        Scanner sc = this.conn.createScanner(this.tableName, Configuration.getAuths());
        Range r = new Range(stageName);
        sc.setRange(r);
        return this.accumuloIterToTIter(sc.iterator()).next();
      } catch (TableNotFoundException e) {
        throw new RebarException(e);
      }
    } else {
      throw new RebarException("Stage: " + stageName + " does not exist.");
    }
  }

  public Iterator<Stage> getStages() throws RebarException {
    return this.rangeToIter(new Range());
  }
  
  public Iterator<Stage> getStages(StageType t) throws RebarException {
    Range r = new Range("type:"+t.toString());
    return this.rangeToIter(r);
  }
  
  /* (non-Javadoc)
   * @see edu.jhu.hlt.rebar.accumulo.AbstractReader#get()
   */
  @Override
  public Iterator<Stage> getAll() throws RebarException {
    return this.rangeToIter(new Range());
  }
  
  public AbstractStageWriter<SectionSegmentation> retrieveSectionStage (String stageName) throws RebarException {
    Stage generic = this.get(stageName);
    if (generic.type == StageType.SECTION)
      return new SectionStage(this.conn, generic);
    else
      throw new RebarException("You requested a stage with type " + generic.type.toString() + ", which is not a SectionStage.");
  }
  
  public void printStages() throws RebarException {
    Iterator<Stage> iter = this.getStages();
    while (iter.hasNext()) {
      Stage s = iter.next();
      System.out.println(String.format("Stage: %s\nDescription: %s\nType: %s\n", s.name, s.description, s.type.toString()));
    }    
  }
  
  public static void main (String... args) throws RebarException {
    StageReader sr = new StageReader(Constants.getConnector());
    sr.printStages();
  }

  
}
