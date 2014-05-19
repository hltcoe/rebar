/**
 * 
 */
package edu.jhu.hlt.rebar.accumulo;

import java.util.UUID;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Range;
import org.joda.time.DateTime;

import edu.jhu.hlt.concrete.Communication;
import edu.jhu.hlt.rebar.Configuration;
import edu.jhu.hlt.rebar.Constants;
import edu.jhu.hlt.rebar.RebarException;
import edu.jhu.hlt.rebar.client.iterators.AutoCloseableAccumuloIterator;

/**
 * @author max
 *
 */
public class CommunicationReader extends AbstractCommunicationReader {
  
  /**
   * @throws RebarException
   */
  public CommunicationReader() throws RebarException {
    this(Constants.getConnector());
  }

  /**
   * @param conn
   * @throws RebarException
   */
  public CommunicationReader(Connector conn) throws RebarException {
    super(conn);
  }
  
  public boolean exists (Communication c) throws RebarException {
    return this.exists(c.id);
  }

  public boolean exists (String docId) throws RebarException {
    try {
      Scanner sc = this.conn.createScanner(this.idxTableName, Configuration.getAuths());
      Range r = new Range("doc_id:" + docId);
      sc.setRange(r);
      return sc.iterator().hasNext();
    } catch (TableNotFoundException e) {
      throw new RebarException(e);
    }
  }
  
  public Communication byUuid (UUID uuid) throws Exception {
    try (AutoCloseableAccumuloIterator<Communication> it = this.fromMainTable(uuid.toString());) {
      if (it.hasNext())
        return it.next();
      else
        throw new RebarException("Document: " + uuid.toString() + " does not exist.");      
    }
  }
  
  public Communication get(String docId) throws RebarException {
    String idxString = "doc_id:" + docId;
    if (this.exists(docId))
      return this.rangeToIter(new Range(idxString, idxString)).next();
    else
      throw new RebarException("Document: " + docId + " has not been ingested.");
  }
  
  public AutoCloseableAccumuloIterator<Communication> getCommunications(long unixTimeStart, long unixTimeEnd) throws RebarException {
    DateTime start = new DateTime(unixTimeStart * 1000);
    DateTime end = new DateTime(unixTimeStart * 1000);
    return this.getCommunications(start, end);
  }
  
  public AutoCloseableAccumuloIterator<Communication> getCommunications(DateTime start, DateTime end) throws RebarException {
    if (start.isAfter(end))
      throw new RebarException("Start date cannot be after end date. Your parameters are probably reversed.");
    Range r = new Range("date:"+start.toString(), "date:"+end.toString());
    return this.rangeToIter(r);
  }
  
  public AutoCloseableAccumuloIterator<Communication> getCommunications(DateTime past) throws RebarException {
    return this.getCommunications(past, new DateTime());
  }
  
  public AutoCloseableAccumuloIterator<Communication> getCommunications(long unixTimePast) throws RebarException {
    return this.getCommunications(new DateTime(unixTimePast * 1000), new DateTime());
  }
  
  public AutoCloseableAccumuloIterator<Communication> getCommunications(String t) throws RebarException {
    Range r = new Range("type:"+t.toString());
    return this.rangeToIter(r);
  }

  /* (non-Javadoc)
   * @see edu.jhu.hlt.rebar.accumulo.AbstractReader#get()
   */
  @Override
  public AutoCloseableAccumuloIterator<Communication> getAll() throws RebarException {
    return this.rangeToIter(new Range());
  }
}
