/**
 * 
 */
package edu.jhu.hlt.rebar.accumulo;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
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

import edu.jhu.hlt.asphalt.Stage;
import edu.jhu.hlt.asphalt.services.StageHandler;
import edu.jhu.hlt.concrete.Communication;
import edu.jhu.hlt.rebar.Configuration;
import edu.jhu.hlt.rebar.Constants;
import edu.jhu.hlt.rebar.RebarException;
import edu.jhu.hlt.rebar.Util;

/**
 * @author max
 * 
 */
public class RebarStageHandler extends AbstractAccumuloClient implements StageHandler.Iface {

  private final BatchWriter stagesTableBW;

  /**
   * 
   */
  public RebarStageHandler() throws RebarException {
    this(Constants.getConnector());
  }

  public RebarStageHandler(Connector conn) throws RebarException {
    super(conn);
    try {
      this.tableOps.createTableIfNotExists(Constants.STAGES_TABLE_NAME);
      this.stagesTableBW = this.conn.createBatchWriter(Constants.STAGES_TABLE_NAME, defaultBwOpts.getBatchWriterConfig());
    } catch (TableNotFoundException e) {
      throw new RebarException(e);
    }
  }

  public void close() throws Exception {
    this.stagesTableBW.close();
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.maxjthomas.dumpster.StageHandler.Iface#stageExists(java.lang.String)
   */
  @Override
  public boolean stageExists(String stageName) throws TException {
    try {
      return this.stageExistsInternal(stageName);
    } catch (RebarException e) {
      throw new TException(e);
    }
  }

  public boolean stageExistsInternal(String stageName) throws RebarException {
    try {
      Scanner sc = this.conn.createScanner(Constants.STAGES_TABLE_NAME, Configuration.getAuths());
      Range r = new Range(stageName);
      sc.setRange(r);
      Iterator<Entry<Key, Value>> iter = sc.iterator();
      return iter.hasNext();
    } catch (TableNotFoundException e) {
      throw new RebarException(e);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.maxjthomas.dumpster.StageHandler.Iface#createStage(com.maxjthomas.dumpster.Stage)
   */
  @Override
  public void createStage(Stage stage) throws TException {
    try {
      this.createStageInternal(stage);
    } catch (RebarException e) {
      throw new TException(e);
    }
  }

  public void createStageInternal(Stage stage) throws RebarException {
    if (this.stageExistsInternal(stage.name))
      throw new RebarException("Can't create a stage that exists.");

    Set<String> deps = stage.dependencies;
    for (String dep : deps) {
      if (!this.stageExistsInternal(dep))
        throw new RebarException("Dependency: " + dep + " doesn't exist, so can't create that stage.");
    }

    if (!isValidStageName(stage))
      throw new RebarException("Stage names must begin with '" + Constants.STAGES_PREFIX + "'; " + stage.name + " is not valid.");

    try {
      // create entry in stage table
      final Mutation m = new Mutation(stage.name);
      m.put(Constants.STAGES_OBJ_COLF, "", new Value(this.serializer.serialize(stage)));
      this.stagesTableBW.addMutation(m);
    } catch (MutationsRejectedException | TException e) {
      throw new RebarException(e);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.maxjthomas.dumpster.StageHandler.Iface#getStages()
   */
  @Override
  public List<Stage> getStages() throws TException {
    try {
      return new ArrayList<>(this.getStagesInternal());
    } catch (RebarException e) {
      throw new TException(e);
    }
  }

  public Set<Stage> getStagesInternal() throws RebarException {
    Set<Stage> stagesToReturn = new HashSet<>();
    try {
      Scanner sc = this.conn.createScanner(Constants.STAGES_TABLE_NAME, Configuration.getAuths());
      Range r = new Range();
      sc.setRange(r);
      sc.fetchColumnFamily(new Text(Constants.STAGES_OBJ_COLF));
      Iterator<Entry<Key, Value>> iter = sc.iterator();
      while (iter.hasNext()) {
        Value v = iter.next().getValue();
        Stage s = new Stage();
        this.deserializer.deserialize(s, v.get());
        stagesToReturn.add(s);
      }

      return stagesToReturn;
    } catch (TableNotFoundException | TException e) {
      throw new RebarException(e);
    }
  }

  public static boolean isValidStageName(Stage s) {
    return isValidStageName(s.name);
  }

  public static boolean isValidStageName(String stageName) {
    return stageName.startsWith(Constants.STAGES_PREFIX);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.maxjthomas.dumpster.StageHandler.Iface#getAnnotatedDocumentCount(com.maxjthomas.dumpster.Stage)
   */
//  @Override
//  public int getAnnotatedDocumentCount(Stage stage) throws TException {
//    try {
//      Scanner sc = this.conn.createScanner(Constants.STAGES_TABLE_NAME, Configuration.getAuths());
//      Range r = new Range(stage.name);
//      sc.setRange(r);
//      sc.fetchColumnFamily(new Text(Constants.STAGES_DOCS_ANNOTATED_IDS_COLF));
//      Iterator<Entry<Key, Value>> iter = sc.iterator();
//      return Util.countIteratorResults(iter);
//    } catch (TableNotFoundException e) {
//      throw new TException(e);
//    }
//  }

  public void addAnnotatedDocument(Stage stage, Communication document) throws RebarException {
    try {
      final Mutation m = new Mutation(stage.name);
      m.put(Constants.STAGES_DOCS_ANNOTATED_IDS_COLF, document.id, Util.EMPTY_VALUE);
      this.stagesTableBW.addMutation(m);
    } catch (MutationsRejectedException e) {
      throw new RebarException(e);
    } finally {

    }
  }

  public Set<String> getAnnotatedDocumentIds(Stage s) throws RebarException {
    Set<String> ids = new HashSet<>();

    try {
      Scanner sc = this.conn.createScanner(Constants.STAGES_TABLE_NAME, Configuration.getAuths());
      Range r = new Range(s.name);
      sc.setRange(r);
      sc.fetchColumnFamily(new Text(Constants.STAGES_DOCS_ANNOTATED_IDS_COLF));
      Iterator<Entry<Key, Value>> iter = sc.iterator();
      while (iter.hasNext()) {
        ids.add(iter.next().getKey().getColumnQualifier().toString());
      }

      return ids;
    } catch (TableNotFoundException e) {
      throw new RebarException(e);
    }
  }

}
