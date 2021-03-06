/**
 * 
 */
package edu.jhu.hlt.rebar.accumulo;

import org.apache.accumulo.core.client.Connector;

import edu.jhu.hlt.rebar.Constants;
import edu.jhu.hlt.rebar.RebarException;

/**
 * @author max
 * 
 */
public class RebarCorpusHandler extends AbstractAccumuloWriter {

  /**
   * @throws RebarException
   */
  public RebarCorpusHandler() throws RebarException {
    this(Constants.getConnector());
  }

  /**
   * @param conn
   * @throws RebarException
   */
  public RebarCorpusHandler(Connector conn) throws RebarException {
    super(conn, Constants.AVAILABLE_CORPUS_TABLE_NAME, Constants.CORPUS_IDX_TABLE_NAME);
  }

//  public void createCorpus(String corpusName, List<Communication> docList) throws RebarException {
//    if (this.corpusExists(corpusName))
//      throw new RebarException("This corpus already exists.");
//
//    if (!isValidCorpusName(corpusName)) {
//      throw new RebarException("Corpus name must begin with 'corpus_'. Corpus name: " + corpusName + " is not valid.");
//    }
//
//    if (docList.size() == 0) {
//      throw new RebarException("Can't create a corpus with zero documents.");
//    }
//
//    // first, add this to the available corpora table.
//    final Mutation m = new Mutation(corpusName);
//    m.put("", "", new Value(new byte[0]));
//
//    try {
//      this.bw.addMutation(m);
//
//      // now, try to create the new table.
//      this.tableOps.createTableIfNotExists(corpusName);
//
//      BatchWriter bw = this.conn.createBatchWriter(corpusName, defaultBwOpts.getBatchWriterConfig());
//      for (Communication d : docList) {
//        final Mutation subM = new Mutation(d.id);
//        subM.put("", "", new Value(new byte[0]));
//        bw.addMutation(subM);
//      }
//
//      bw.close();
//
//    } catch (MutationsRejectedException | RebarException | TableNotFoundException e) {
//      throw new TException(e);
//    }
//  }
//
//  /*
//   * (non-Javadoc)
//   * 
//   * @see com.maxjthomas.dumpster.CorpusHandler.Iface#getCorpusDocumentSet(java.lang.String)
//   */
//  @Override
//  public List<Communication> getCorpusCommunicationSet(String corpusName) throws AsphaltException, TException {
//    Set<Communication> docSet = new HashSet<>();
//    try {
//      // first hit the corpus table itself, to get the ids for the ranges.
//      Scanner sc = this.conn.createScanner(corpusName, Configuration.getAuths());
//      Range r = new Range();
//      sc.setRange(r);
//      
//      Set<Range> rangeSet = new HashSet<>();
//      Iterator<Entry<Key, Value>> iter = sc.iterator();
//      while (iter.hasNext()) {
//        Key k = iter.next().getKey();
//        String rowId = k.getRow().toString();
//        Range range = new Range(rowId);
//        rangeSet.add(range);
//      }
//      
//      BatchScanner bsc = this.conn.createBatchScanner(Constants.DOCUMENT_TABLE_NAME, Configuration.getAuths(), 10);
//      bsc.setRanges(rangeSet);
//
//      Iterator<Entry<Key, Value>> bscIter = bsc.iterator();
//      while (bscIter.hasNext()) {
//        Value v = bscIter.next().getValue();
//        Communication d = new Communication(); 
//        this.deserializer.deserialize(d, v.get());
//        docSet.add(d);
//      }
//      
//      bsc.close();
//      
//      if (docSet.size() != rangeSet.size()) {
//        throw new AsphaltException("Did not retrieve all documents associated "
//            + "with corpus: " + corpusName + "; expected: " + rangeSet.size()
//            + " but got: " + docSet.size());
//      }
//      
//      return new ArrayList<>(docSet);
//    } catch (TableNotFoundException e) {
//      throw new AsphaltException(e.getMessage());
//    }
//  }
//
//  /*
//   * (non-Javadoc)
//   * 
//   * @see com.maxjthomas.dumpster.CorpusHandler.Iface#listCorpora()
//   */
//  @Override
//  public List<String> listCorpora() throws AsphaltException, TException {
//    try {
//      Scanner sc = this.conn.createScanner(Constants.AVAILABLE_CORPUS_TABLE_NAME, Configuration.getAuths());
//      Range r = new Range();
//      sc.setRange(r);
//      Iterator<Entry<Key, Value>> iter = sc.iterator();
//      Set<String> corporaSet = new HashSet<>();
//      while (iter.hasNext()) {
//        Key k = iter.next().getKey();
//        corporaSet.add(k.getRow().toString());
//      }
//      
//      return new ArrayList<>(corporaSet);
//    } catch (TableNotFoundException e) {
//      throw new AsphaltException(e.getMessage());
//    }
//  }
//
//  public void deleteCorpus(String corpusName) throws RebarException{
//    if (!this.corpusExists(corpusName))
//      throw new RebarException("Can't delete a corpus that doesn't exist.");
//
//    try {
//      // first, delete corpus table.
//      this.tableOps.deleteTable(corpusName);
//
//      // next, remove entry from available_corpora.
//      final Mutation mDel = new Mutation(corpusName);
//      mDel.putDelete("", "");
//      this.corporaTableBW.addMutation(mDel);
//    } catch (MutationsRejectedException e) {
//      throw new RebarException(e);
//    }
//  }
//  
//  public boolean corpusExists(String corpusName) throws RebarException {
//    try {
//      Scanner sc = this.conn.createScanner(Constants.AVAILABLE_CORPUS_TABLE_NAME, Configuration.getAuths());
//      Range r = new Range(corpusName);
//      sc.setRange(r);
//      Iterator<Entry<Key, Value>> iter = sc.iterator();
//      return iter.hasNext();
//    } catch (TableNotFoundException e) {
//      throw new RebarException(e);
//    }
//  }
//
//  protected static String generateCorpusName(String corpusName) {
//    return Constants.CORPUS_PREFIX + corpusName;
//  }
//
//  public static boolean isValidCorpusName(String corpusName) {
//    return corpusName.startsWith(Constants.CORPUS_PREFIX);
//  }
}
