/**
 * Copyright 2012-2013 Johns Hopkins University HLTCOE. All rights reserved.
 * This software is released under the 2-clause BSD license.
 * See LICENSE in the project root directory.
 */


package edu.jhu.hlt.rebar.accumulo;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchDeleter;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import edu.jhu.hlt.rebar.RebarException;
import edu.jhu.hlt.rebar.config.RebarConfiguration;

/** REF COUNT HERE DOENS'T REALLY WORK/MAKE SENSE...? */
class AccumuloConnector {
	private boolean released; // has this connector been released?

	//======================================================================
	// Constants
	//======================================================================
	private static int refcount = 0;
	private static Connector connector = null;
	private static FileSystem hdfs = null;
	private final static Authorizations auths = org.apache.accumulo.core.Constants.NO_AUTHS;
	private final static long MAX_WRITE_MEMORY = 1024L * 1024 * 5; // 5 mb
	private final static int MAX_WRITE_LATENCY_MS = 5; // 5 msec
	private final static int NUM_WRITE_THREADS = 5;
	private final static int NUM_DELETER_QUERY_THREADS = 5;
	private final static int NUM_QUERY_THREADS = 8;

	private static List<BatchScanner> batchScanners;
	private static List<BatchWriter> batchWriters;
	private static List<BatchDeleter> batchDeleters;

	//======================================================================
	// Constructor
	//======================================================================
	public AccumuloConnector() throws RebarException {
		if (connector == null)
			createSingleton();
		refcount++;
		this.released = false;
	}

	//======================================================================
	// Public Methods
	//======================================================================
	public void release() throws RebarException {
		if (!this.released) {
			this.released = true;
			refcount--;
			if (refcount == 0)
				deleteSingleton();
		}
	}

	public Scanner createScanner(String tableName) throws RebarException {
		if (this.released)
			throw new RebarException("This AccumuloConnector has been released!");
		try {
			return connector.createScanner(tableName, auths);
		} catch (TableNotFoundException e) {
			throw new RebarException(e);
		}
	}

	public BatchScanner createBatchScanner(String tableName) throws RebarException {
		if (this.released)
			throw new RebarException("This AccumuloConnector has been released!");
		try {
			BatchScanner batchScanner = connector.createBatchScanner(tableName, auths, NUM_QUERY_THREADS);
			batchScanners.add(batchScanner);
			return batchScanner;
		} catch (TableNotFoundException e) {
			throw new RebarException(e);
		}
	}

	public BatchWriter createBatchWriter(String tableName) throws RebarException {
		if (this.released)
			throw new RebarException("This AccumuloConnector has been released!");
		try {
			BatchWriter batchWriter = connector.createBatchWriter
				(tableName, MAX_WRITE_MEMORY, 
				 MAX_WRITE_LATENCY_MS, NUM_WRITE_THREADS);
			batchWriters.add(batchWriter);
			return batchWriter;
		} catch (TableNotFoundException e) {
			throw new RebarException(e);
		}
	}

	/** Delete all cells in a specified table with a given column family. */
	public void deleteColumnFamily(String tableName, Text columnFamily) throws RebarException {
		BatchDeleter deleter = createBatchDeleter(tableName);
		deleter.setRanges(Collections.singletonList(new Range()));
		deleter.fetchColumnFamily(columnFamily);
		try {
			deleter.delete();
			deleter.close();
		} catch (MutationsRejectedException e) {
			throw new RebarException(e);
		} catch (TableNotFoundException e) {
			throw new RebarException(e);
		}
	}
	
	/** Delete a specified row from a table. */
	public void deleteRow(String tableName, Text rowid) throws RebarException {
		BatchDeleter deleter = createBatchDeleter(tableName);
		deleter.setRanges(Collections.singletonList(new Range(rowid)));
		try {
			deleter.delete();
			deleter.close();
		} catch (MutationsRejectedException e) {
			throw new RebarException(e);
		} catch (TableNotFoundException e) {
			throw new RebarException(e);
		}
	}

	private BatchDeleter createBatchDeleter(String tableName) throws RebarException {
		if (this.released)
			throw new RebarException("This AccumuloConnector has been released!");
		try {
			BatchDeleter batchDeleter = connector.createBatchDeleter
				(tableName, auths, NUM_DELETER_QUERY_THREADS,
				 MAX_WRITE_MEMORY, MAX_WRITE_LATENCY_MS, NUM_WRITE_THREADS);
			batchDeleters.add(batchDeleter);
			return batchDeleter;
		} catch (TableNotFoundException e) {
			throw new RebarException(e);
		}
	}

	public void createTable(String tableName) throws RebarException {
		if (connector.tableOperations().exists(tableName))
			throw new RebarException("Table \""+tableName+"\" already exists");
		try {
			connector.tableOperations().create(tableName);
		} catch (AccumuloException e) {
			throw new RebarException(e);
		} catch (AccumuloSecurityException e) {
			throw new RebarException(e);
		} catch (TableExistsException e) {
			throw new RebarException(e);
        }
	}
	
	public void createTableIfNotExists(String tableName) throws RebarException {
    	try {
			if (!connector.tableOperations().exists(tableName)) {
				connector.tableOperations().create(tableName);
			} 
		} catch (AccumuloException e) {
			throw new RebarException(e);
		} catch (AccumuloSecurityException e) {
			throw new RebarException(e);
		} catch (TableExistsException e) {
			throw new RebarException(e);
        }
	}
	
	public void deleteTable(String tableName) throws RebarException {
		try {
			connector.tableOperations().delete(tableName);
		} catch (AccumuloException e) {
			throw new RebarException(e);
		} catch (AccumuloSecurityException e) {
			throw new RebarException(e);
		} catch (TableNotFoundException e) {
			throw new RebarException(e);
        }
	}
	
	public boolean tableExists(String tableName) {
		return connector.tableOperations().exists(tableName);
	}
	
	public Collection<String> listTables() {
		return connector.tableOperations().list();
	}

	public boolean hasIterator(String tableName, String name) 
		throws RebarException
	{
		try {
			return connector.tableOperations().listIterators(tableName).containsKey(name);
		} catch (AccumuloSecurityException e) {
			throw new RebarException(e);
		} catch (AccumuloException e) {
			throw new RebarException(e);
		} catch (TableNotFoundException e) {
			throw new RebarException(e);
		}
	}

	public void attachIterator(String tableName, IteratorSetting setting) 
		throws RebarException
	{
		try {
			connector.tableOperations().attachIterator(tableName, setting);
		} catch (AccumuloSecurityException e) {
			throw new RebarException(e);
		} catch (AccumuloException e) {
			throw new RebarException(e);
		} catch (TableNotFoundException e) {
			throw new RebarException(e);
		}
	}

	public static final Path ACCUMULO_HDFS_ROOT=new Path("/accumulo/tables/");
	public long getSize(String tableName) throws RebarException {
		try {
			if (hdfs == null)  {
				org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
				conf.set("fs.default.name", RebarConfiguration.getHdfsRoot());
				hdfs = FileSystem.get(conf);
			}
			String internalName = connector.tableOperations().tableIdMap().get(tableName);
			Path tableDir = new Path(ACCUMULO_HDFS_ROOT, internalName);
			long size = getSizeRecursive(tableDir);
			//System.err.println("Size of "+tableName+" ("+internalName+") is "+size);
			return size;
		} catch (java.io.IOException e) {
			throw new RebarException(e);
        }
	}

	public long getSizeRecursive(Path path) throws java.io.IOException {
		long size = 0;
		for (org.apache.hadoop.fs.FileStatus stat: hdfs.listStatus(path)) {
			if (stat.isDir())
				size += getSizeRecursive(stat.getPath());
			else {
				//System.err.println(stat.getLen()+"   "+stat.getPath());
				size += stat.getLen();
			}
		}
		return size;
	}
	

	//======================================================================
	// Private Helpers
	//======================================================================
	private static void deleteSingleton() throws RebarException {
		//System.err.println("DELETING SINGLETON ACCUMULO CONNECTOR");
		assert (connector != null);
		// connector doesn't have a release() method.
		connector = null;
		if (hdfs != null) {
			try {
				hdfs.close();
			} catch (java.io.IOException e) {
				throw new RebarException(e);
			}
		}
		hdfs = null;
		for (BatchScanner s: batchScanners) s.close();
		batchScanners = null;
		for (BatchDeleter s: batchDeleters) s.close();
		batchDeleters = null;
		try {
			for (BatchWriter s: batchWriters) s.close();
			batchWriters = null;
		} catch (MutationsRejectedException e) {
			throw new RebarException(e);
		}
	}

	private static void createSingleton() throws RebarException {
		//System.err.println("CREATING SINGELTON ACCUMULO CONNECTOR");
		assert(connector == null);
		Instance zki;
		if (RebarConfiguration.useAccumuloMock())
			zki = new MockInstance(RebarConfiguration.getAccumuloInstanceName());
		else
			zki = new ZooKeeperInstance(RebarConfiguration.getAccumuloInstanceName(), 
										RebarConfiguration.getZookeeperServer());
		try {
			connector = zki.getConnector(RebarConfiguration.getAccumuloUser(), 
									RebarConfiguration.getAccumuloPassword());
		} catch (AccumuloException e) {
			throw new RebarException(e);
		} catch (AccumuloSecurityException e) {
			throw new RebarException(e);
		}
		batchScanners = new ArrayList<BatchScanner>();
		batchWriters = new ArrayList<BatchWriter>();
		batchDeleters = new ArrayList<BatchDeleter>();
	}
}
