/**
 * Copyright 2012-2013 Johns Hopkins University HLTCOE. All rights reserved.
 * This software is released under the 2-clause BSD license.
 * See LICENSE in the project root directory.
 */
package edu.jhu.hlt.rebar.accumulo;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.HashSet;
import java.util.Set;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.thrift.TException;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import com.maxjthomas.dumpster.Document;
import com.maxjthomas.dumpster.IngestException;
import com.maxjthomas.dumpster.Ingester;

import edu.jhu.hlt.concrete.Concrete.Communication.Kind;
import edu.jhu.hlt.rebar.Constants;
import edu.jhu.hlt.rebar.RebarException;

/**
 * @author max
 *
 */
public class RebarIngester extends AbstractAccumuloClient implements AutoCloseable, Ingester.Iface {

  private final Jedis jedis;
  private Set<String> pendingInserts;
  private Set<String> existingIds;
  
  private static final JedisPool pool = new JedisPool(new JedisPoolConfig(), "localhost");
  private static final String ingestedIdsRedisKey = "ingested-ids";
  
  /**
   * @throws RebarException 
   * 
   */
  public RebarIngester() throws RebarException {
    this(AbstractAccumuloClient.getConnector());
  }
  
  public RebarIngester(Connector conn) throws RebarException {
    super(conn);
    this.jedis = pool.getResource();
    this.pendingInserts = new HashSet<>();
    this.existingIds = this.jedis.smembers(ingestedIdsRedisKey);
  }
  
  private boolean isDocumentIngested(Document d) {
    return this.existingIds.contains(d.getId());
  }
  
  private boolean isDocumentPendingIngest(Document d) {
    return this.pendingInserts.contains(d.getId());
  }
  
  private void flushPendingIds() {
    this.jedis.sadd(ingestedIdsRedisKey, this.pendingInserts.toArray(new String[0]));
    this.pendingInserts = new HashSet<>();
  }
  
  private void updateExistingIds() {
    this.existingIds = this.jedis.smembers(ingestedIdsRedisKey);
  }
  
  @Override
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
  
//  protected static String generateRowId(Document d) {
//    return d.t.toString() + "_" + d.id;
//  }

  /* (non-Javadoc)
   * @see com.maxjthomas.dumpster.Ingester.Iface#ingest(com.maxjthomas.dumpster.Document)
   */
  @Override
  public void ingest(Document d) throws IngestException {
    try {
      if (isDocumentIngested(d) || isDocumentPendingIngest(d))
        return;
      
      final Mutation m = new Mutation(d.id);
      
      try {
        Value v = new Value(this.serializer.serialize(d));
        m.put(Constants.DOCUMENT_COLF, "", v);
        this.bw.addMutation(m);
        this.pendingInserts.add(d.getId());
      } catch (MutationsRejectedException | TException e) {
        throw new RebarException(e);
      }
    } catch (RebarException e) {
      ByteArrayOutputStream os = new ByteArrayOutputStream();
      try {
        new ObjectOutputStream(os).writeObject(e);
      } catch (IOException e1) {
        throw new IngestException(e1.getMessage());
      }

      IngestException ie = new IngestException(e.getMessage());
      ie.setSerEx(os.toByteArray());
      throw ie;
    }
  }
}
