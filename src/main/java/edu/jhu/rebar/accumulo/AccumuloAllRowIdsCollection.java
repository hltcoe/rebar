/**
 * Copyright 2012-2013 Johns Hopkins University HLTCOE. All rights reserved.
 * This software is released under the 2-clause BSD license.
 * See LICENSE in the project root directory.
 */


package edu.jhu.rebar.accumulo;

import java.util.AbstractCollection;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyIterator;
import org.apache.hadoop.io.Text;

import edu.jhu.rebar.RebarException;

/** A collection backed by the set of all row identifiers in 
 * a specified accumulo table.  This is used to special-case
 * the task of reading all items from a corpus or knowledge
 * graph. */
abstract public class AccumuloAllRowIdsCollection<Identifier>
	extends AbstractCollection<Identifier>
	implements Collection<Identifier> 
{
	// Hook for subclass:
	abstract protected Identifier rowIdToId(Text row);

	// Private variables:
	private final AccumuloConnector accumuloConnector;
	private final String tableName;
	private Collection<Identifier> identifiers = null;
	
	public AccumuloAllRowIdsCollection(AccumuloConnector accumuloConnector, String tableName) {
		this.accumuloConnector = accumuloConnector;
		this.tableName = tableName;
	}

	public Iterator<Identifier> iterator() {
		if (identifiers == null)
			identifiers = scanIdentifiers();
		return identifiers.iterator();
	}

	public int size() {
		if (identifiers == null)
			identifiers = scanIdentifiers();
		return identifiers.size();
	}

	private Collection<Identifier> scanIdentifiers() {
		try {
			identifiers = new HashSet<Identifier>();
			final Scanner scanner = accumuloConnector.createScanner(tableName);
			scanner.setRange(new Range());
			// [xx] the FirstEntryInRowIterator is broken in Accumulo 1.4 (it throws
			// a NullPointerException because a private variable does not get 
			// initialized in the constructor).  So leave this one out for now:
			//IteratorSetting is = new IteratorSetting(1000, //"first_entry",
			//                                         FirstEntryInRowIterator.class);
			scanner.addScanIterator(new IteratorSetting(1001, "key_only",
			SortedKeyIterator.class));
			final Iterator<Map.Entry<Key, Value>> iterator = scanner.iterator();
			while (iterator.hasNext()) {
				identifiers.add(rowIdToId(iterator.next().getKey().getRow()));
			}
			return identifiers;
		} catch (RebarException e) {
			throw new RuntimeException(e);
		}
	}
}
