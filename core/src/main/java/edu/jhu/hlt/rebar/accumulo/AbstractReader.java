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

import edu.jhu.hlt.rebar.Configuration;
import edu.jhu.hlt.rebar.Constants;
import edu.jhu.hlt.rebar.RebarException;

/**
 * @author max
 *
 * TODO: Enforce more bounds on T (e.g., extends ThriftStruct or some such)
 */
public abstract class AbstractReader<T> extends AbstractAccumuloClient {

  protected final String tableName;
  protected final String idxTableName;
  
  /**
   * @throws RebarException
   */
  public AbstractReader(String tableName, String idxTableName) throws RebarException {
    this(Constants.getConnector(), tableName, idxTableName);
  }

  /**
   * @param conn
   * @throws RebarException
   */
  public AbstractReader(Connector conn, String tableName, String idxTableName) throws RebarException {
    super(conn);
    this.tableName = tableName;
    this.idxTableName = idxTableName;
    
    this.tableOps.createTableIfNotExists(tableName);
    this.tableOps.createTableIfNotExists(idxTableName);
  }
  
  protected Set<Range> scanIndexTableColF (Range r) throws RebarException {
    try {
      Set<Range> rowsToGet = new HashSet<>();
      
      // scan IDX table for ids.
      Scanner sc = this.conn.createScanner(this.idxTableName, Configuration.getAuths());
      sc.setRange(r);
      Iterator<Entry<Key, Value>> iter = sc.iterator();
      while (iter.hasNext()) {
        String uuid = iter.next().getKey().getColumnFamily().toString();
        rowsToGet.add(new Range(uuid));
      }
      
      return rowsToGet;
    } catch (TableNotFoundException e) {
      throw new RebarException(e);
    }
  }
  
  protected abstract Iterator<T> accumuloIterToTIter (Iterator<Entry<Key, Value>> accIter) throws RebarException;
  
  protected Iterator<T> fromMainTable(String rowId) throws RebarException {
    try {
      Scanner sc = this.conn.createScanner(this.tableName, Configuration.getAuths());
      Range r = new Range(rowId, rowId);
      sc.setRange(r);
      return this.accumuloIterToTIter(sc.iterator());
    } catch (TableNotFoundException e) {
      throw new RebarException(e);
    }
  }
  
  protected Iterator<T> rangeToIter(Range docIdxRange) throws RebarException {
    Set<Range> uuidsToGet = this.scanIndexTableColF(docIdxRange);
    // if we didn't find any IDs, there aren't any docs of this type.
    // return empty iterator.
    if (uuidsToGet.isEmpty())
      return new EmptyIterator<T>();
    else
      return this.accumuloIterToTIter(this.batchScanMainTable(uuidsToGet));
  }
  
  protected Iterator<Entry<Key, Value>> batchScanMainTable(Collection<Range> ids) throws RebarException {
    // scan document table with IDs.
    BatchScanner docsc = null;
    try {
      docsc = this.conn.createBatchScanner(this.tableName, Configuration.getAuths(), 8);
      docsc.setRanges(ids);
      return docsc.iterator();
    } catch (TableNotFoundException e) {
      throw new RebarException(e);
    } finally {
      if (docsc != null)
        docsc.close();
    }
  }
}
