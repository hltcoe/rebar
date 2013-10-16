/**
 * Copyright 2012-2013 Johns Hopkins University HLTCOE. All rights reserved.
 * This software is released under the 2-clause BSD license.
 * See LICENSE in the project root directory.
 */

package edu.jhu.hlt.rebar.accumulo;

import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyIterator;
import org.apache.hadoop.io.Text;

import edu.jhu.hlt.rebar.RebarException;

/**
 * An interface to an "id set table", which associates symbolic names with sets of item identifers.
 */
/* package-private */
abstract class AccumuloIdSetTable<Identifier> {
  // ======================================================================
  // Hooks for Subclass to fill in
  // ======================================================================

  abstract protected Text idToRowId(Identifier id);

  abstract protected Identifier rowIdToId(Text id);

  // ======================================================================
  // Constants
  // ======================================================================

  private final static Text ID_CF = new Text("id");
  private final static Text CTIME_CF = new Text("ctime");
  private final static Text EMPTY_CQ = new Text();
  private final static Value EMPTY_VALUE = new Value(new byte[] {});

  // ======================================================================
  // Private Variables
  // ======================================================================

  /** The name of the accumulo table that backs this AccumuloIdSetTable */
  private final String tableName;
  /** The connection to accumulo */
  private final AccumuloConnector accumuloConnector;

  // ======================================================================
  // Constructor
  // ======================================================================

  AccumuloIdSetTable(AccumuloConnector accumuloConnector, String tableName) {
    this.accumuloConnector = accumuloConnector;
    this.tableName = tableName; // = corpusName+IDSETS_TABLE_SUFFIX
  }

  // ======================================================================
  // Public Methods
  // ======================================================================

  public void registerIdSet(String name, Collection<Identifier> idSet) throws RebarException {
    BatchWriter writer = accumuloConnector.createBatchWriter(tableName);
    Mutation m = new Mutation(new Text(name));
    for (Identifier id : idSet)
      m.put(ID_CF, idToRowId(id), EMPTY_VALUE);
    m.put(CTIME_CF, EMPTY_CQ, new Value(new Date().toString().getBytes()));
    try {
      writer.addMutation(m);
      writer.flush();
      writer.close();
    } catch (MutationsRejectedException e) {
      throw new RebarException(e);
    }
  }

  public Collection<Identifier> lookupIdSet(String name) throws RebarException {
    HashSet<Identifier> result = new HashSet<Identifier>();
    final Scanner scanner = accumuloConnector.createScanner(tableName);
    scanner.fetchColumnFamily(ID_CF);
    scanner.setRange(new Range(new Text(name)));
    final Iterator<Map.Entry<Key, Value>> iterator = scanner.iterator();
    while (iterator.hasNext())
      result.add(rowIdToId(iterator.next().getKey().getColumnQualifier()));
    return result;
  }

  public Collection<String> getSubsetNames() throws RebarException {
    HashSet<String> names = new HashSet<String>();
    final Scanner scanner = accumuloConnector.createScanner(tableName);
    scanner.fetchColumnFamily(CTIME_CF);
    scanner.addScanIterator(new IteratorSetting(1001, "key_only", SortedKeyIterator.class));
    final Iterator<Map.Entry<Key, Value>> iterator = scanner.iterator();
    while (iterator.hasNext()) {
      names.add(iterator.next().getKey().getRow().toString());
    }
    return names;
  }

}
