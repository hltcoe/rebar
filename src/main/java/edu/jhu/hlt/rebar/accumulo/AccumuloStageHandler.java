/**
 * 
 */
package edu.jhu.hlt.rebar.accumulo;

import java.util.Iterator;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.thrift.TException;

import com.maxjthomas.dumpster.Stage;
import com.maxjthomas.dumpster.StageHandler;

import edu.jhu.hlt.rebar.RebarException;
import edu.jhu.hlt.rebar.config.RebarConfiguration;
import edu.jhu.hlt.rebar.util.RebarUtil;

/**
 * @author max
 *
 */
public class AccumuloStageHandler extends AbstractAccumuloClient implements StageHandler.Iface {

  private final BatchWriter stagesTableBW;
  
  /**
   * 
   */
  public AccumuloStageHandler() throws RebarException {
    this(AbstractAccumuloClient.getConnector());
  }
  
  public AccumuloStageHandler(Connector conn) throws RebarException {
    super(conn);
    try {
      this.tableOps.createTableIfNotExists(RebarConfiguration.STAGES_TABLE_NAME);
      this.stagesTableBW = this.conn.createBatchWriter(RebarConfiguration.STAGES_TABLE_NAME, defaultBwOpts.getBatchWriterConfig());
    } catch (TableNotFoundException e) {
      throw new RebarException(e);
    }
  }

  /* (non-Javadoc)
   * @see java.lang.AutoCloseable#close()
   */
  @Override
  public void close() throws Exception {
    this.stagesTableBW.close();
  }

  /* (non-Javadoc)
   * @see edu.jhu.hlt.rebar.accumulo.AbstractAccumuloClient#flush()
   */
  @Override
  public void flush() throws RebarException {
    try {
      this.stagesTableBW.flush();
    } catch (MutationsRejectedException e) {
      throw new RebarException(e);
    }
  }

  /* (non-Javadoc)
   * @see com.maxjthomas.dumpster.StageHandler.Iface#stageExists(java.lang.String)
   */
  @Override
  public boolean stageExists(String stageName) throws TException {
    try {
      Scanner sc = this.conn.createScanner(RebarConfiguration.STAGES_TABLE_NAME, RebarConfiguration.getAuths());
      Range r = new Range(stageName);
      sc.setRange(r);
      Iterator<Entry<Key, Value>> iter = sc.iterator();
      return iter.hasNext();
    } catch (TableNotFoundException e) {
      try {
        throw RebarUtil.wrapException(e);
      } catch (RebarException e1) {
        throw new TException(e1.getMessage());
      }
    }
  }

  /* (non-Javadoc)
   * @see com.maxjthomas.dumpster.StageHandler.Iface#createStage(com.maxjthomas.dumpster.Stage)
   */
  @Override
  public void createStage(Stage stage) throws TException {
    if (this.stageExists(stage.name))
      throw new TException("Can't create a stage that exists.");
    
    if (!isValidStageName(stage))
      throw new TException("Stage names must begin with '"
          + RebarConfiguration.STAGES_PREFIX + "'; " 
          + stage.name + " is not valid.");
    
    // create entry in stage table
    final Mutation m = new Mutation(stage.name);
    m.put("", "", new Value(this.serializer.serialize(stage)));
    try {
      this.stagesTableBW.addMutation(m);
      
      this.tableOps.createTableIfNotExists(stage.name);
      
      
    } catch (MutationsRejectedException | RebarException e) {
      throw new TException(e.getMessage());
    }
  }

  /* (non-Javadoc)
   * @see com.maxjthomas.dumpster.StageHandler.Iface#getStages()
   */
  @Override
  public Set<Stage> getStages() throws TException {
    // TODO Auto-generated method stub
    return null;
  }
  
  public static boolean isValidStageName(Stage s) {
    return isValidStageName(s.name);
  }
  
  public static boolean isValidStageName(String stageName) {
    return stageName.startsWith(RebarConfiguration.STAGES_PREFIX);
  }

}
