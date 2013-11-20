/**
 * Copyright 2012-2013 Johns Hopkins University HLTCOE. All rights reserved.
 * This software is released under the 2-clause BSD license.
 * See LICENSE in the project root directory.
 */
package edu.jhu.hlt.rebar.accumulo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import redis.clients.jedis.Jedis;


import edu.jhu.hlt.concrete.Communication;
import edu.jhu.hlt.rebar.Configuration;
import edu.jhu.hlt.rebar.RebarException;
import edu.jhu.hlt.rebar.Util;

/**
 * @author max
 *
 */
public class TestRebarIngester extends AbstractAccumuloTest {
  
  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {
    this.initialize();
  }

  /**
   * @throws java.lang.Exception
   */
  @After
  public void tearDown() throws Exception {
  }
  
  @Test
  public void testInsertDocument() throws TException, RebarException, TableNotFoundException {
    Communication d = generateMockDocument();
    //String rowId = RebarIngester.generateRowId(d);
    String docId = d.id;
    byte[] dbytes = this.serializer.serialize(d);
    
    RebarIngester rebar = new RebarIngester(this.conn);
    rebar.ingest(d);
    rebar.close();
    
    Iterator<Entry<Key, Value>> iter = generateIterator(this.conn, edu.jhu.hlt.rebar.Constants.DOCUMENT_TABLE_NAME, new Range(docId));
    assertTrue("Should find results in accumulo.", iter.hasNext());
    assertEquals(0, iter.next().getValue().compareTo(dbytes));
    Jedis jedis = new Jedis("localhost");
    assertTrue(jedis.smembers("ingested-ids").contains(docId));
    jedis.srem("ingested-ids", docId);
  }
  
  @Test
  public void testInsertManyDocuments() throws TException, RebarException, TableNotFoundException {
    int nDocs = 50;
    Set<Communication> docs = generateMockDocumentSet(nDocs);
    
    RebarIngester rebar = new RebarIngester(this.conn);
    for (Communication d : docs)
      rebar.ingest(d);
    rebar.close();
    
    Iterator<Entry<Key, Value>> iter = generateIterator(this.conn, edu.jhu.hlt.rebar.Constants.DOCUMENT_TABLE_NAME, new Range());
    assertEquals("Should find a few results in accumulo.", nDocs, Util.countIteratorResults(iter));
    
    iter = generateIterator(this.conn, edu.jhu.hlt.rebar.Constants.DOCUMENT_TABLE_NAME, new Range());
    Set<Communication> fetchDocs = new HashSet<>(nDocs);
    while(iter.hasNext()) {
      Communication d = new Communication();
      Value v = iter.next().getValue();
      this.deserializer.deserialize(d, v.get());
      fetchDocs.add(d);
    }
    
    BatchScanner bsc = this.conn.createBatchScanner(edu.jhu.hlt.rebar.Constants.DOCUMENT_TABLE_NAME, Configuration.getAuths(), 10);
    List<Range> rangeList = new ArrayList<>();
    rangeList.add(new Range(docs.iterator().next().id));
    bsc.setRanges(rangeList);
    iter = bsc.iterator();
    while (iter.hasNext()) {
      Entry<Key, Value> e = iter.next();
      Key k = e.getKey();
      Value v = e.getValue();
    }
    
    assertEquals("Input and output sets should be the same, but weren't.", docs, fetchDocs);
  }
}
