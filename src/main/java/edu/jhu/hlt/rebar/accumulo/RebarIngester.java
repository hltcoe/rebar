/**
 * Copyright 2012-2013 Johns Hopkins University HLTCOE. All rights reserved.
 * This software is released under the 2-clause BSD license.
 * See LICENSE in the project root directory.
 */
package edu.jhu.hlt.rebar.accumulo;

import java.util.HashSet;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import edu.jhu.hlt.concrete.Concrete.Communication.Kind;
import edu.jhu.hlt.concrete.index.IndexedCommunication;
import edu.jhu.hlt.rebar.RebarException;
import edu.jhu.hlt.rebar.config.RebarConfiguration;

/**
 * @author max
 *
 */
public class RebarIngester implements AutoCloseable {

  private final Connector conn;
  private final RebarTableOps tableOps;
  private final Jedis jedis;
  
  private BatchWriter bw;
  private Set<String> pendingInserts;
  private Set<String> existingIds;
  
  private static final JedisPool pool = new JedisPool(new JedisPoolConfig(), "localhost");
  private static final String ingestedIdsRedisKey = "ingested-ids";
  
  private final static Authorizations auths = org.apache.accumulo.core.Constants.NO_AUTHS;
  private final static long MAX_WRITE_MEMORY = 1024L * 1024 * 5; // 5 mb
  private final static int MAX_WRITE_LATENCY_MS = 5; // 5 msec
  private final static int NUM_WRITE_THREADS = 5;
  private final static int NUM_DELETER_QUERY_THREADS = 5;
  private final static int NUM_QUERY_THREADS = 8;
  
  /**
   * @throws RebarException 
   * 
   */
  public RebarIngester() throws RebarException {
    // TODO Auto-generated constructor stub
    this(getConnector());
  }
  
  public RebarIngester(Connector conn) throws RebarException {
    this.conn = conn;
    this.tableOps = new RebarTableOps(this.conn);
    this.tableOps.createTableIfNotExists("corpora");
    this.jedis = pool.getResource();
    this.pendingInserts = new HashSet<>();
    this.existingIds = this.jedis.smembers(ingestedIdsRedisKey);
    try {
      this.bw = this.conn.createBatchWriter("corpora", MAX_WRITE_MEMORY, MAX_WRITE_LATENCY_MS, NUM_WRITE_THREADS);
    } catch (TableNotFoundException e) {
      throw new RebarException(e);
    }
  }
  
  private boolean isCommunicationIngested(IndexedCommunication comm) {
    return this.existingIds.contains(comm.getGuid().getCommunicationId());
  }
  
  private boolean isCommunicationPendingIngest(IndexedCommunication comm) {
    return this.pendingInserts.contains(comm.getGuid().getCommunicationId());
  }
  
  public void insert(IndexedCommunication comm) throws RebarException {
    if (isCommunicationIngested(comm) || isCommunicationPendingIngest(comm))
      return;
    
    final Mutation m = new Mutation(comm.generateRowId());
    Value v = new Value(comm.getProto().toByteArray());
    m.put("", "", v);
    try {
      this.bw.addMutation(m);
      this.pendingInserts.add(comm.getGuid().getCommunicationId());
    } catch (MutationsRejectedException e) {
      throw new RebarException(e);
    }
  }
  
  private void flushPendingIds() {
    this.jedis.sadd(ingestedIdsRedisKey, this.pendingInserts.toArray(new String[0]));
    this.pendingInserts = new HashSet<>();
  }
  
  private void updateExistingIds() {
    this.existingIds = this.jedis.smembers(ingestedIdsRedisKey);
  }
  
  public void flush() throws RebarException {
    try {
      this.bw.flush();
      this.flushPendingIds();
      this.updateExistingIds();
    } catch (MutationsRejectedException e) {
      throw new RebarException(e);
    }
  }
  
  public void close() throws RebarException {
    try {
      this.bw.close();
      this.flushPendingIds();
      pool.returnResource(this.jedis);
    } catch (MutationsRejectedException e) {
      throw new RebarException(e);
    } finally {
      
    }
  }
  
  protected static String generateRowId(String commId, Kind kind) {
    return kind.toString() + "_" + commId;
  }
  
  public static Connector getConnector() throws RebarException {
    Instance zki = new ZooKeeperInstance(RebarConfiguration.getAccumuloInstanceName(), RebarConfiguration.getZookeeperServer());
    try {
      return zki.getConnector(RebarConfiguration.getAccumuloUser(), RebarConfiguration.getAccumuloPassword());
    } catch (AccumuloException e) {
      throw new RebarException(e);
    } catch (AccumuloSecurityException e) {
      throw new RebarException(e);
    }
  }
}
