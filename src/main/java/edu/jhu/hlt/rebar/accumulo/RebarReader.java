/**
 * 
 */
package edu.jhu.hlt.rebar.accumulo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.hadoop.io.Text;
import org.apache.thrift.TException;

import edu.jhu.hlt.concrete.Communication;
import edu.jhu.hlt.concrete.LangId;
import edu.jhu.hlt.concrete.LanguagePrediction;
import edu.jhu.hlt.concrete.RebarThriftException;
import edu.jhu.hlt.concrete.Stage;
import edu.jhu.hlt.concrete.StageType;
import edu.jhu.hlt.concrete.Reader;
import edu.jhu.hlt.rebar.Configuration;
import edu.jhu.hlt.rebar.Constants;
import edu.jhu.hlt.rebar.RebarException;

/**
 * @author max
 *
 */
public class RebarReader extends AbstractAccumuloClient implements Reader.Iface {

  private final RebarStageHandler ash;
  private static Map<String, StageType> stageNameToTypeMap = null;
  
  /**
   * @throws RebarException
   */
  public RebarReader() throws RebarException {
    this(Constants.getConnector());
  }

  /**
   * @param conn
   * @throws RebarException
   */
  public RebarReader(Connector conn) throws RebarException {
    super(conn);
    this.ash = new RebarStageHandler(this.conn);
    if (stageNameToTypeMap == null) {
      this.updateCache();
    }
  }
  
  private synchronized void updateCache() throws RebarException {
    Set<Stage> stages = this.ash.getStagesInternal();
    stageNameToTypeMap = new HashMap<>(stages.size());
    for (Stage s : stages) 
      stageNameToTypeMap.put(s.getName(), s.getType());
  }

  /* (non-Javadoc)
   * @see java.lang.AutoCloseable#close()
   */
  @Override
  public void close() throws Exception {
    this.ash.close();
    this.bw.close();
  }

  /* (non-Javadoc)
   * @see edu.jhu.hlt.rebar.accumulo.AbstractAccumuloClient#flush()
   */
  @Override
  public void flush() throws RebarException {
    this.ash.flush();
    try {
      this.bw.flush();
    } catch (MutationsRejectedException e) {
      throw new RebarException(e);
    }
  }
  
  private Set<Communication> constructCommunicationSet(Stage s, Set<String> docIds) throws RebarException, TException, IOException {
    Set<Communication> docSet = new HashSet<>();
    
    // TODO: dependencies.
    // we need to get a list of the dependency names so that if we see those stages, 
    // we can add them to the object.
    Set<String> namesToGet = new HashSet<String>(s.dependencies);
    
    // however, we also want to add the current stage name to get its annotations as well.
    namesToGet.add(s.name);
    
    // check our cache to make sure we have all of these stages - if not, update.
    namesToGet.removeAll(stageNameToTypeMap.entrySet());
    if (namesToGet.size() > 0)
      this.updateCache();

    BatchScanner bsc = this.createScanner(s, docIds);
    Iterator<Entry<Key, Value>> iter = bsc.iterator();
    while (iter.hasNext()) {
      Entry<Key, Value> e = iter.next();
      Map<Key, Value> rows = WholeRowIterator.decodeRow(e.getKey(), e.getValue());
      Communication root = this.getRoot(rows);
      for (Entry<Key, Value> r : rows.entrySet()) {
        Key k = r.getKey();
        String colQ = k.getColumnQualifier().toString();
        StageType t = stageNameToTypeMap.get(colQ);
        switch (t) {
        case LANG_ID:
          LangId lid = new LangId();
          this.deserializer.deserialize(lid, r.getValue().get());
          root.setLid(lid);
          break;
        case LANG_PRED:
          LanguagePrediction lp = new LanguagePrediction();
          this.deserializer.deserialize(lp, r.getValue().get());
          root.setLanguage(lp);
          break;
        default:
          throw new IllegalArgumentException("Case: " + s.type.toString() + " not handled yet.");
        }
      }
      
      docSet.add(root);
    }

    return docSet;
  }
  
  private Communication getRoot(Map<Key, Value> decodedRow) throws TException {
    Communication d = new Communication();
    Iterator<Entry<Key, Value>> iter = decodedRow.entrySet().iterator();
    while (iter.hasNext()) {
      Entry<Key, Value> entry = iter.next();
      if (entry.getKey().compareColumnFamily(new Text(Constants.DOCUMENT_COLF)) == 0) { 
        this.deserializer.deserialize(d, entry.getValue().get());
        iter.remove();
      }
    }
      
    return d;
  }
  
  protected BatchScanner createScanner(Stage stageOfInterest, Set<String> idSet) throws RebarException {
    List<Range> rangeList = new ArrayList<>();
    for (String id : idSet)
      rangeList.add(new Range(id));
    
    try {
      String sName = stageOfInterest.name;
      BatchScanner bsc = this.conn.createBatchScanner(Constants.DOCUMENT_TABLE_NAME, Configuration.getAuths(), 8);
      bsc.setRanges(rangeList);
      bsc.fetchColumnFamily(new Text(Constants.DOCUMENT_COLF));
      // need to fetch columns for all dependencies, as well.
      for (String dep : stageOfInterest.dependencies)
        bsc.fetchColumn(new Text(Constants.DOCUMENT_ANNOTATION_COLF), new Text(dep));
      
      bsc.fetchColumn(new Text(Constants.DOCUMENT_ANNOTATION_COLF), new Text(sName));
      bsc.addScanIterator(new IteratorSetting(1000, "wholeRows", WholeRowIterator.class));
      return bsc;
    } catch (TableNotFoundException e) {
      throw new RebarException(e);
    }
  }

  /* (non-Javadoc)
   * @see edu.jhu.hlt.concrete.Reader.Iface#getAnnotatedCommunications(edu.jhu.hlt.concrete.Stage)
   */
  @Override
  public Set<Communication> getAnnotatedCommunications(Stage stage) throws RebarThriftException, TException {
    if (!this.ash.stageExists(stage.name))
      throw new RebarThriftException("Stage " + stage.name + " doesn't exist; can't get its documents.");

    try {
      Set<String> docIds = this.ash.getAnnotatedDocumentIds(stage);
      Set<Communication> docSet = this.constructCommunicationSet(stage, docIds);
      return docSet;
    } catch (RebarException | IOException e) {
      throw new TException(e);
    }
  }
}
