/**
 * Copyright 2012-2013 Johns Hopkins University HLTCOE. All rights reserved.
 * This software is released under the 2-clause BSD license.
 * See LICENSE in the project root directory.
 */


package edu.jhu.rebar.accumulo;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.LongCombiner.VarLenEncoder;
import org.apache.accumulo.core.iterators.user.SummingCombiner;
import org.apache.hadoop.io.Text;

import edu.jhu.rebar.RebarException;

/** An interface to a "summary table" for a graph or corpus, which
 * keeps track of overall information about the graph/corpus.
 * 
 * Rows in this table are names of corpora or graphs.  The accumulo
 * table that backs each AccumuloSummaryTable can contain the
 * following cells:
 *
 * <pre>
 *   RowId         ColFam     ColQual     Value
 * -------------- --------- ----------- ---------
 *  corpus_name    "count"   "coms"       long
 *  graph_name     "count"   "vertices"   long
 * </pre>
 */

/*package-private*/ 
class AccumuloSummaryTable {
	// ======================================================================
	// Constants
	// ======================================================================

	private final static Text COUNT_CF = new Text("count");
	private final static Value VALUE_ONE = new Value(new VarLenEncoder().encode(1l));

	// ======================================================================
	// Private Variables
	// ======================================================================
	private final String tableName;
	private final AccumuloConnector accumuloConnector;
	private BatchWriter writer = null;

	public AccumuloSummaryTable(AccumuloConnector accumuloConnector, String tablePrefix) 
		throws RebarException 
	{
		this.tableName = tablePrefix+"info";
		this.accumuloConnector = accumuloConnector;
		if (!accumuloConnector.tableExists(tableName)) {
			accumuloConnector.createTable(tableName);
			setupCombiner();
		}
	}

	public void flush() throws RebarException { 
		if (writer != null) {
			try {
				writer.flush(); 
			} catch (MutationsRejectedException e) {
				throw new RebarException(e);
			}
		}
	}

	public void close() throws RebarException { 
		if (writer != null) {
			try {
				writer.close(); 
			} catch (MutationsRejectedException e) {
				throw new RebarException(e);
			}
		}
	}

	// Note: priority must be less than 20 (since that's the
	// priority of the verisoning iterator.)
	private static final int COMBINER_PRIORITY = 10;

	private void setupCombiner() throws RebarException {
		// Add a summing combiner to all columns where family="count"
		IteratorSetting combiner = new IteratorSetting(COMBINER_PRIORITY, SummingCombiner.class);
		combiner.addOption("columns", COUNT_CF.toString());
		combiner.addOption("type", "VARLEN");
		accumuloConnector.attachIterator(this.tableName, combiner);
	}

	public long getCount(String collectionName, String itemType) 
		throws RebarException 
	{
		Scanner scanner = accumuloConnector.createScanner(tableName);
		scanner.setRange(new Range(new Text(collectionName)));
		scanner.fetchColumnFamily(COUNT_CF);
		Text cq = new Text(itemType);
		for (Map.Entry<Key, Value> entry: scanner) {
			if (entry.getKey().compareColumnQualifier(cq) == 0)
				return new VarLenEncoder().decode(entry.getValue().get());
		}
		return -1;
	}

	public void deleteEntry(String collectionName) throws RebarException {
		// Reset all counts. (Note that just deleting the row is not
		// always sufficient for this -- but I'm not sure why.  This
		// might be affected by COMBINER_PRIORITY?)
		Scanner scanner = accumuloConnector.createScanner(tableName);
		scanner.setRange(new Range(new Text(collectionName)));
		scanner.fetchColumnFamily(COUNT_CF);
		List<String> itemTypes = new ArrayList<String>();
		for (Map.Entry<Key, Value> entry: scanner)
			itemTypes.add(entry.getKey().getColumnQualifier().toString());
		for (String itemType: itemTypes)
			resetCount(collectionName, itemType);
		// Now delete the row.
		accumuloConnector.deleteRow(tableName, new Text(collectionName));
	}

	public void resetCount(String collectionName, String itemType)
		throws RebarException 
	{
		long count = getCount(collectionName, itemType);
		Value negCount = new Value(new VarLenEncoder().encode(-count));
		incrementCount(collectionName, itemType, negCount);
	}

	public void incrementCount(String collectionName, String itemType) 
		throws RebarException 
	{
		incrementCount(collectionName, itemType, VALUE_ONE);
	}

	private void incrementCount(String collectionName, String itemType, Value value) 
		throws RebarException 
	{
		if (writer == null)
			writer = accumuloConnector.createBatchWriter(tableName);
		Mutation m = new Mutation(new Text(collectionName));
		m.put(COUNT_CF, new Text(itemType), value);
		try {
			writer.addMutation(m);
		} catch (MutationsRejectedException e) {
			throw new RebarException(e);
		}
	}
}
