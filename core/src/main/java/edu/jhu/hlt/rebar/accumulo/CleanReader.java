/**
 * 
 */
package edu.jhu.hlt.rebar.accumulo;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.joda.time.DateTime;

import edu.jhu.hlt.concrete.Communication;
import edu.jhu.hlt.concrete.CommunicationType;
import edu.jhu.hlt.rebar.Configuration;
import edu.jhu.hlt.rebar.Constants;
import edu.jhu.hlt.rebar.RebarException;

/**
 * @author max
 *
 */
public class CleanReader extends AbstractAccumuloClient {

  /**
   * @throws RebarException
   */
  public CleanReader() throws RebarException {
    this(Constants.getConnector());
  }

  /**
   * @param conn
   * @throws RebarException
   */
  public CleanReader(Connector conn) throws RebarException {
    super(conn);
    this.tableOps.createTableIfNotExists(Constants.DOCUMENT_IDX_TABLE);
  }

  public boolean exists (String docId) throws RebarException {
    try {
      Scanner sc = this.conn.createScanner(Constants.DOCUMENT_IDX_TABLE, Configuration.getAuths());
      Range r = new Range("doc_id:" + docId);
      sc.setRange(r);
      return sc.iterator().hasNext();
    } catch (TableNotFoundException e) {
      throw new RebarException(e);
    }
  }
  
  private Set<Range> scanIndexTable (Range r) throws RebarException {
    try {
      Set<Range> uuidsToGet = new HashSet<>();
      
      // scan IDX table for ids.
      Scanner sc = this.conn.createScanner(Constants.DOCUMENT_IDX_TABLE, Configuration.getAuths());
      sc.setRange(r);
      Iterator<Entry<Key, Value>> iter = sc.iterator();
      while (iter.hasNext()) {
        String uuid = iter.next().getKey().getColumnFamily().toString();
        uuidsToGet.add(new Range(uuid));
      }
      
      return uuidsToGet;
    } catch (TableNotFoundException e) {
      throw new RebarException(e);
    }
  }
  
  private Iterator<Communication> batchScanDocumentTable(Collection<Range> ids) throws RebarException {
    // scan document table with IDs.
    BatchScanner docsc = null;
    try {
      docsc = this.conn.createBatchScanner(Constants.DOCUMENT_TABLE_NAME, Configuration.getAuths(), 8);
      docsc.setRanges(ids);
      return new CommunicationIterator(docsc.iterator());
    } catch (TableNotFoundException e) {
      throw new RebarException(e);
    } finally {
      if (docsc != null)
        docsc.close();
    }
  }
  
  private Iterator<Communication> rangeToCommIter(Range docIdxRange) throws RebarException {
    Set<Range> uuidsToGet = this.scanIndexTable(docIdxRange);
    // if we didn't find any IDs, there aren't any docs of this type.
    // return empty iterator.
    if (uuidsToGet.isEmpty())
      return new EmptyIterator<Communication>();
    else
      return this.batchScanDocumentTable(uuidsToGet);
  }
  
  public Iterator<Communication> getCommunications(int unixTimeStart, int unixTimeEnd) throws RebarException {
    DateTime start = new DateTime(unixTimeStart * 1000);
    DateTime end = new DateTime(unixTimeEnd * 1000);
    return this.getCommunications(start, end);
  }
  
  public Iterator<Communication> getCommunications(DateTime start, DateTime end) throws RebarException {
    if (start.isAfter(end))
      throw new RebarException("Start date cannot be after end date. Your parameters are probably reversed.");
    Range r = new Range("date:"+start.toString(), "date:"+end.toString());
    return this.rangeToCommIter(r);
  }
  
  public Iterator<Communication> getCommunications(DateTime past) throws RebarException {
    return this.getCommunications(past, new DateTime());
  }
  
  public Iterator<Communication> getCommunications(int unixTimePast) throws RebarException {
    return this.getCommunications(new DateTime(unixTimePast * 1000), new DateTime());
  }
  
  public Iterator<Communication> getCommunications(CommunicationType t) throws RebarException {
    Range r = new Range("type:"+t.toString());
    return rangeToCommIter(r);
  }
  
  public boolean exists (Communication c) throws RebarException {
    return this.exists(c.id);
  }
}
