/**
 * 
 */
package edu.jhu.hlt.rebar.accumulo;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.apache.thrift.TException;

import com.maxjthomas.dumpster.Document;
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
      throw new TException(e);
    }
  }

  /* (non-Javadoc)
   * @see com.maxjthomas.dumpster.StageHandler.Iface#createStage(com.maxjthomas.dumpster.Stage)
   */
  @Override
  public void createStage(Stage stage) throws TException {
    if (this.stageExists(stage.name))
      throw new TException("Can't create a stage that exists.");
    
    Set<String> deps = stage.dependencies;
    for (String dep : deps) {
      if (!this.stageExists(dep))
        throw new TException("Dependency: " + dep + " doesn't exist, so can't create that stage.");
    }
    
    if (!isValidStageName(stage))
      throw new TException("Stage names must begin with '"
          + RebarConfiguration.STAGES_PREFIX + "'; " 
          + stage.name + " is not valid.");
    
    // create entry in stage table
    final Mutation m = new Mutation(stage.name);
    m.put(RebarConfiguration.STAGES_OBJ_COLF, "", new Value(this.serializer.serialize(stage)));
    try {
      this.stagesTableBW.addMutation(m);
    } catch (MutationsRejectedException  e) {
      throw new TException(e.getMessage());
    }
  }

  /* (non-Javadoc)
   * @see com.maxjthomas.dumpster.StageHandler.Iface#getStages()
   */
  @Override
  public Set<Stage> getStages() throws TException {
    Set<Stage> stagesToReturn = new HashSet<>();
    try {
      Scanner sc = this.conn.createScanner(RebarConfiguration.STAGES_TABLE_NAME, RebarConfiguration.getAuths());
      Range r = new Range();
      sc.setRange(r);
      Iterator<Entry<Key, Value>> iter = sc.iterator();
      while(iter.hasNext()) {
        Value v = iter.next().getValue();
        Stage s = new Stage();
        this.deserializer.deserialize(s, v.get());
        stagesToReturn.add(s);
      }
      
      return stagesToReturn;
    } catch (TableNotFoundException e) {
      throw new TException(e);
    }
  }
  
  public static boolean isValidStageName(Stage s) {
    return isValidStageName(s.name);
  }
  
  public static boolean isValidStageName(String stageName) {
    return stageName.startsWith(RebarConfiguration.STAGES_PREFIX);
  }

  /* (non-Javadoc)
   * @see com.maxjthomas.dumpster.StageHandler.Iface#getAnnotatedDocumentCount(com.maxjthomas.dumpster.Stage)
   */
  @Override
  public int getAnnotatedDocumentCount(Stage stage) throws TException {
    try {
      // TODO Auto-generated method stub
      Scanner sc = this.conn.createScanner(RebarConfiguration.STAGES_TABLE_NAME, RebarConfiguration.getAuths());
      Range r = new Range();
      sc.setRange(r);
      sc.fetchColumnFamily(new Text(RebarConfiguration.STAGES_DOCS_ANNOTATED_IDS_COLF));
      Iterator<Entry<Key, Value>> iter = sc.iterator();
      return RebarUtil.countIteratorResults(iter);
    } catch (TableNotFoundException e) {
      throw new TException(e);
    }
  }
  
  public void addAnnotatedDocument(Stage stage, Document document) throws RebarException {
    try {
      final Mutation m = new Mutation(stage.name);
      m.put(RebarConfiguration.STAGES_DOCS_ANNOTATED_IDS_COLF, document.id, RebarUtil.EMPTY_VALUE);
      this.stagesTableBW.addMutation(m);
    } catch (MutationsRejectedException e) {
      throw new RebarException(e);
    } finally {
      
    }
  }

}
