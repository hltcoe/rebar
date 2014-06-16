/*
 * 
 */
package edu.jhu.hlt.rebar.stage;

import java.io.ByteArrayOutputStream;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.io.Output;

import edu.jhu.hlt.concrete.Communication;
import edu.jhu.hlt.rebar.Configuration;
import edu.jhu.hlt.rebar.Constants;
import edu.jhu.hlt.rebar.RebarException;
import edu.jhu.hlt.rebar.Util;
import edu.jhu.hlt.rebar.accumulo.AbstractAccumuloClient;

/**
 * @author max
 * 
 */
public class StageCreator extends AbstractAccumuloClient implements AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(StageCreator.class);
  
  protected final BatchWriter stagesTableBW;
  protected final BatchWriter stagesIdxBW;
  protected final StageReader sr;
  
  /**
   * 
   */
  public StageCreator() throws RebarException {
    this(Constants.getConnector());
  }

  public StageCreator(Connector conn) throws RebarException {
    super(conn);
    try {
      this.tableOps.createTableIfNotExists(Constants.STAGES_TABLE_NAME);
      this.stagesTableBW = this.conn.createBatchWriter(Constants.STAGES_TABLE_NAME, defaultBwOpts.getBatchWriterConfig());
      
      this.tableOps.createTableIfNotExists(Constants.STAGES_IDX_TABLE_NAME);
      this.stagesIdxBW = this.conn.createBatchWriter(Constants.STAGES_IDX_TABLE_NAME, defaultBwOpts.getBatchWriterConfig());
      this.sr = new StageReader(this.conn);
    } catch (TableNotFoundException e) {
      throw new RebarException(e);
    }
  }

  @Override
  public void close() throws Exception {
    this.stagesIdxBW.close();
    this.stagesTableBW.close();
  }

  public void create(Stage stage) throws RebarException {
    if (this.sr.exists(stage.getName()))
      throw new RebarException("Can't create a stage that exists.");
    
    if (stage.getStageType() == null)
      throw new RebarException("No type specified for stage.");

    Set<String> deps = stage.getDependencies();
    for (String dep : deps) {
      if (!this.sr.exists(dep))
        throw new RebarException("Dependency: " + dep + " doesn't exist; can't create that stage.");
    }

    try {
      // create entry in stage table
      final Mutation m = new Mutation(stage.getName());
      Output op = new Output(new ByteArrayOutputStream());
      this.kryo.writeObject(op, stage);
      m.put(Constants.STAGES_OBJ_COLF, "", new Value(op.toBytes()));
      this.stagesTableBW.addMutation(m);
      
      Mutation typeIdx = Util.generateEmptyValueMutation("type:"+stage.getStageType().toString(), stage.getName(), "");
      this.stagesIdxBW.addMutation(typeIdx);
      op.close();
    } catch (MutationsRejectedException e) {
      throw new RebarException(e);
    }
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
      final Mutation m = new Mutation(stage.getName());
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
      Range r = new Range(s.getName());
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

  public static void main(String... args) throws Exception {
    if (args.length != 3) {
      logger.info("Use this program to create a stage with no dependencies, suitable for LIDs or Sections.");
      logger.info("Takes 3 arguments: stage-name, stage-description, stage-type.");
      logger.info("Stage type must be one of (case ignored):");
      logger.info("SECTION");
      logger.info("LANG_ID");
      logger.info("Usage: {} <{}> <{}> <{}>", 
          StageCreator.class.getSimpleName(), "stage-name", "stage-description", "stage-type");
      System.exit(1);
    }
    
    String stageName = args[0];
    if (new StageReader().exists(stageName)) {
      logger.info("Stage {} exists already. Choose a different name.", stageName);
      System.exit(1);
    }
      
    String stageDesc = args[1];
    String stageTypeString = args[2];
    StageType st;
    try {
      st = StageType.valueOf(stageTypeString.toUpperCase());
      if (!(st == StageType.LANG_ID || st == StageType.SECTION)) {
        logger.info("Currently you can only create LANG_ID or SECTION stages, not {}.", st.toString());
        System.exit(1);
      }
    } catch (IllegalArgumentException iae) {
      throw new RuntimeException(stageTypeString + " was not a valid stage.");
    }
    
    Stage s = new Stage(stageName, stageDesc, st, new HashSet<String>());
    try (StageCreator sc = new StageCreator()) {
      sc.create(s);
    }
    
    logger.info("Stage created successfully!");
    System.exit(0);
  }
}
