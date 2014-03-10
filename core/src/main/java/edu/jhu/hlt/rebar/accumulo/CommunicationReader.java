/**
 * 
 */
package edu.jhu.hlt.rebar.accumulo;

import java.util.Iterator;
import java.util.UUID;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.thrift.TException;
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
public class CommunicationReader extends AbstractReader<Communication> {
  
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
    super(conn, Constants.DOCUMENT_TABLE_NAME, Constants.DOCUMENT_IDX_TABLE);
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
  
  public Communication byUuid (UUID uuid) throws RebarException {
    Iterator<Communication> it = this.fromMainTable(uuid.toString());
    if (it.hasNext())
      return it.next();
    else
      throw new RebarException("Document: " + uuid.toString() + " does not exist.");
  }
  
  public Communication get(String docId) throws RebarException {
    String idxString = "doc_id:" + docId;
    if (this.exists(docId))
      return this.rangeToIter(new Range(idxString, idxString)).next();
    else
      throw new RebarException("Document: " + docId + " has not been ingested.");
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
    return this.rangeToIter(r);
  }
  
  public Iterator<Communication> getCommunications(DateTime past) throws RebarException {
    return this.getCommunications(past, new DateTime());
  }
  
  public Iterator<Communication> getCommunications(int unixTimePast) throws RebarException {
    return this.getCommunications(new DateTime(unixTimePast * 1000), new DateTime());
  }
  
  public Iterator<Communication> getCommunications(CommunicationType t) throws RebarException {
    Range r = new Range("type:"+t.toString());
    return this.rangeToIter(r);
  }

  @Override
  protected Iterator<Communication> accumuloIterToTIter(Iterator<Entry<Key, Value>> accIter) throws RebarException {
    return new CommunicationIterator(accIter);
  }
}
