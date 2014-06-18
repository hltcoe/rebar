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
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;

import edu.jhu.hlt.rebar.Configuration;
import edu.jhu.hlt.rebar.Constants;
import edu.jhu.hlt.rebar.RebarException;
import edu.jhu.hlt.rebar.client.iterators.AbstractThriftIterator;
import edu.jhu.hlt.rebar.client.iterators.EmptyAutoCloseableAccumuloIterator;

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
    Scanner sc = null;
    try {
      Set<Range> rowsToGet = new HashSet<>();
      
      // scan IDX table for ids.
      sc = this.conn.createScanner(this.idxTableName, Configuration.getAuths());
      sc.setRange(r);
      Iterator<Entry<Key, Value>> iter = sc.iterator();
      while (iter.hasNext()) {
        String uuid = iter.next().getKey().getColumnFamily().toString();
        rowsToGet.add(new Range(uuid));
      }
      
      return rowsToGet;
    } catch (TableNotFoundException e) {
      throw new RebarException(e);
    } finally {
      sc.close();
    }
  }
  
  protected abstract AbstractThriftIterator<T> accumuloIterToTIter (ScannerBase sc) throws RebarException;
  
  protected AbstractThriftIterator<T> fromMainTable(String rowId) throws RebarException {
    try {
      Scanner sc = this.conn.createScanner(this.tableName, Configuration.getAuths());
      Range r = new Range(rowId, rowId);
      sc.setRange(r);
      return this.accumuloIterToTIter(sc);
    } catch (TableNotFoundException e) {
      throw new RebarException(e);
    }
  }
  
  protected AbstractThriftIterator<T> rangeToIter(Range docIdxRange) throws RebarException {
    Set<Range> uuidsToGet = this.scanIndexTableColF(docIdxRange);
    // if we didn't find any IDs, there aren't any docs of this type.
    // return empty iterator.
    if (uuidsToGet.isEmpty())
      return new EmptyAutoCloseableAccumuloIterator<T>();
    else
      return this.accumuloIterToTIter(this.batchScanMainTable(uuidsToGet));
  }
  
//  protected Iterator<Entry<Key, Value>> batchScanMainTable(Collection<Range> ids) throws RebarException {
//    // scan document table with IDs.
//    BatchScanner docsc = null;
//    try {
//      docsc = this.conn.createBatchScanner(this.tableName, Configuration.getAuths(), 8);
//      docsc.setRanges(ids);
//      return docsc.iterator();
//    } catch (TableNotFoundException e) {
//      throw new RebarException(e);
//    } finally {
//      if (docsc != null)
//        docsc.close();
//    }
//  }
  protected abstract BatchScanner batchScanMainTable(Collection<Range> ids) throws RebarException;
  
  protected BatchScanner batchScanMainTableWholeRowIterator(Collection<Range> ids) throws RebarException {
    // scan document table with IDs.
    BatchScanner docsc = null;
    try {
      docsc = this.conn.createBatchScanner(this.tableName, Configuration.getAuths(), 8);
      docsc.setRanges(ids);
      IteratorSetting itSettings = new IteratorSetting(1, WholeRowIterator.class);
      docsc.addScanIterator(itSettings);
      return docsc;
    } catch (TableNotFoundException e) {
      throw new RebarException(e);
    }
  }
  
  public abstract AbstractThriftIterator<T> getAll() throws RebarException;
}
