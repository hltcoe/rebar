/*
 * 
 */
package edu.jhu.hlt.rebar.stage;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Range;
import org.apache.hadoop.io.Text;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;

import edu.jhu.hlt.concrete.SectionSegmentation;
import edu.jhu.hlt.concrete.SentenceSegmentationCollection;
import edu.jhu.hlt.concrete.TokenizationCollection;
import edu.jhu.hlt.rebar.Configuration;
import edu.jhu.hlt.rebar.Constants;
import edu.jhu.hlt.rebar.RebarException;
import edu.jhu.hlt.rebar.accumulo.AbstractReader;
import edu.jhu.hlt.rebar.client.iterators.AbstractThriftIterator;
import edu.jhu.hlt.rebar.stage.reader.SectionStageReader;
import edu.jhu.hlt.rebar.stage.reader.SentenceStageReader;
import edu.jhu.hlt.rebar.stage.reader.TokenizationStageReader;
import edu.jhu.hlt.rebar.stage.writer.SectionStageWriter;
import edu.jhu.hlt.rebar.stage.writer.SentenceStageWriter;
import edu.jhu.hlt.rebar.stage.writer.TokenizationStageWriter;

/**
 * @author max
 *
 */
public final class StageReader extends AbstractReader<Stage> {
  
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
  protected AbstractThriftIterator<Stage> accumuloIterToTIter(ScannerBase sc) throws RebarException {
    return new AbstractThriftIterator<Stage>(sc) {
      
      final Kryo k = new Kryo();
      
      @Override
      public Stage next() {
        byte[] bytes = this.iter.next().getValue().get();
        try (Input i = new Input(bytes)) {
          return this.k.readObject(i, Stage.class);
        }
      }
    };
  }
  
  @Override
  protected BatchScanner batchScanMainTable(Collection<Range> ids) throws RebarException {
    BatchScanner docsc = null;
    try {
      docsc = this.conn.createBatchScanner(this.tableName, Configuration.getAuths(), 8);
      docsc.setRanges(ids);
      docsc.fetchColumnFamily(new Text(Constants.STAGES_OBJ_COLF));
      return docsc;
    } catch (TableNotFoundException e) {
      throw new RebarException(e);
    }
  }
  
  public boolean exists(Stage s) throws RebarException {
    return this.exists(s.getName());
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

        try (AbstractThriftIterator<Stage> iter = this.accumuloIterToTIter(sc);) {
          Stage match = iter.next();
          return match;
        }
      } catch (TableNotFoundException e) {
        throw new RebarException(e);
      }
    } else {
      throw new RebarException("Stage: " + stageName + " does not exist.");
    }
  }

  public AbstractThriftIterator<Stage> getStages() throws RebarException {
    return this.rangeToIter(new Range());
  }
  
  public AbstractThriftIterator<Stage> getStages(StageType t) throws RebarException {
    Range r = new Range("type:"+t.toString());
    return this.rangeToIter(r);
  }
  
  /* (non-Javadoc)
   * @see edu.jhu.hlt.rebar.accumulo.AbstractReader#get()
   */
  @Override
  public AbstractThriftIterator<Stage> getAll() throws RebarException {
    return this.rangeToIter(new Range());
  }
  
  public AbstractStageWriter<SectionSegmentation> getSectionStageWriter (String stageName) throws RebarException {
    Stage generic = this.get(stageName);
    if (generic.getStageType() == StageType.SECTION)
      return new SectionStageWriter(this.conn, generic);
    else
      throw new RebarException("You requested a stage with type " + generic.getStageType().toString() + ", which is not a SectionStage.");
  }
  
  public AbstractStageReader getSectionStageReader (String stageName) throws RebarException {
    Stage generic = this.get(stageName);
    if (generic.getStageType() == StageType.SECTION)
      return new SectionStageReader(this.conn, stageName);
    else
      throw new RebarException("You requested a stage with type " + generic.getStageType().toString() + ", which is not a SectionStage.");
  }
  
  public AbstractStageWriter<SentenceSegmentationCollection> getSentenceStageWriter (String stageName) throws RebarException {
    Stage generic = this.get(stageName);
    if (generic.getStageType() == StageType.SENTENCE)
      return new SentenceStageWriter(this.conn, generic);
    else
      throw new RebarException("You requested a stage with type " + generic.getStageType().toString() + ", which is not a SectionStage.");
  }
  
  /*
   * TODO: better validation logic on stages
   */
  public AbstractStageReader getSentenceStageReader (String stageName) throws RebarException {
    Stage generic = this.get(stageName);
    if (generic.getStageType() == StageType.SENTENCE) {
      Set<String> deps = generic.getDependencies();
      List<String> depList = new ArrayList<>(deps);
      String firstDep = depList.get(0);
      return new SentenceStageReader(this.conn, stageName, firstDep);
    }
    
    else
      throw new RebarException("You requested a stage with type " + generic.getStageType().toString() + ", which is not a SentenceStage.");
  }
  
  public AbstractStageWriter<TokenizationCollection> getTokenizationStageWriter (String stageName) throws RebarException {
    Stage generic = this.get(stageName);
    if (generic.getStageType() == StageType.TOKENIZATION)
      return new TokenizationStageWriter(this.conn, generic);
    else
      throw new RebarException("You requested a stage with type " + generic.getStageType().toString() + ", which is not a TokenizationStage.");
  }
  
  /*
   * TODO: better validation logic on stages
   */
  public AbstractStageReader getTokenizationStageReader (String stageName) throws RebarException {
    Stage generic = this.get(stageName);
    if (generic.getStageType() == StageType.TOKENIZATION) {
      Set<String> deps = generic.getDependencies();
      List<String> depList = new ArrayList<>(deps);
      String firstDep = null;
      String secondDep = null;
      for (String d : depList) {
        Stage s = this.get(d);
        if (s.getStageType() == StageType.SECTION)
          firstDep = d;
        else if (s.getStageType() == StageType.SENTENCE)
          secondDep = d;
      }
      
      if (firstDep != null && secondDep != null)
        return new TokenizationStageReader(this.conn, stageName, firstDep, secondDep);
      else
        throw new RebarException("Couldn't get the dependencies quite right.");
    }
    
    else
      throw new RebarException("You requested a stage with type " + generic.getStageType().toString() + ", which is not a SectionStage.");
  }
  
  public void printStages() throws Exception {
    try (AbstractThriftIterator<Stage> iter = this.getStages();) {
      while (iter.hasNext()) {
        Stage s = iter.next();
        System.out.println(String.format("Stage: %s\nDescription: %s\nType: %s\n", s.getName(), s.getDescription(), s.getStageType().toString()));
      }
    }
  }
  
  public static void main (String... args) throws Exception {
    StageReader sr = new StageReader(Constants.getConnector());
    sr.printStages();
  }
}
