/**
 * Copyright 2012-2013 Johns Hopkins University HLTCOE. All rights reserved.
 * This software is released under the 2-clause BSD license.
 * See LICENSE in the project root directory.
 */
package edu.jhu.hlt.rebar.accumulo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import redis.clients.jedis.Jedis;

import com.maxjthomas.dumpster.Document;
import com.maxjthomas.dumpster.DocType;

import edu.jhu.hlt.concrete.Concrete.Communication;
import edu.jhu.hlt.concrete.Concrete.Communication.Kind;
import edu.jhu.hlt.concrete.Concrete.CommunicationGUID;
import edu.jhu.hlt.concrete.ConcreteException;
import edu.jhu.hlt.concrete.index.IndexedCommunication;
import edu.jhu.hlt.concrete.util.IdUtil;
import edu.jhu.hlt.rebar.RebarException;

/**
 * @author max
 *
 */
public class TestRebarIngester {

  private List<IndexedCommunication> commList;
  private Instance inst;
  private Connector conn;
  private RebarTableOps tableOps;
  private TSerializer serializer;

  private final Random rand = new Random();
  
  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {
    this.commList = generateMockCommunications(10000, Kind.TWEET);
    this.inst = new MockInstance();
    this.conn = this.inst.getConnector("max", new byte[0]);
    this.tableOps = new RebarTableOps(conn);
    this.serializer = new TSerializer(new TBinaryProtocol.Factory());
  }

  /**
   * @throws java.lang.Exception
   */
  @After
  public void tearDown() throws Exception {
  }
  
  public List<IndexedCommunication> generateMockCommunications(int nComms, Communication.Kind commType) throws ConcreteException {
    List<IndexedCommunication> commList = new ArrayList<>(10000);
    for (int i = 0; i < nComms; i++) {
      commList.add(generateMockCommunication(commType));
    }
    
    return commList;
  }
  
  public IndexedCommunication generateMockCommunication (Communication.Kind commType) throws ConcreteException {
    int randCommId = Math.abs(this.rand.nextInt());
    long ts = (long)(this.rand.nextInt(300000000) * this.rand.nextFloat()) + 1288061605; // min date --> oct 2010 ish
    String commIdString = "Communication_" + randCommId; 
    
    CommunicationGUID.Builder commGuidBuilder = CommunicationGUID.newBuilder()
        .setCommunicationId(commIdString);
    Communication comm = Communication.newBuilder()
        .setGuid(commGuidBuilder)
        .setText("Blah blah blah blah blah")
        .setUuid(IdUtil.generateUUID())
        .setKind(commType)
        .setStartTime(ts)
        .build();
    
    return new IndexedCommunication(comm);
  }
  
  public Document generateMockDocument() {
    Document document = new Document();
    document.t = DocType.TWEET;
    document.text = "hello world!";
    document.id = Integer.toString(Math.abs(this.rand.nextInt()));
    
    return document;
  }
  
  private Iterator<Entry<Key, Value>> generateIterator(Range range) throws TableNotFoundException {
    Scanner sc = this.conn.createScanner("corpora", Constants.NO_AUTHS);
    sc.setRange(range);
    return sc.iterator();
  }
  
  private int countIteratorResults(Iterator<Entry<Key, Value>> iter) {
    int resCt = 0;
    while (iter.hasNext()) {
      iter.next();
      resCt++;
    }
    
    return resCt;
  }
  
  @Test
  public void testInsertCommunication() throws RebarException, ConcreteException, TableNotFoundException {
    IndexedCommunication comm = this.generateMockCommunication(Kind.TWEET);
    String rowId = comm.generateRowId();
    String docId = comm.getGuid().getCommunicationId();
    byte[] commBytes = comm.getProto().toByteArray();
    
    RebarIngester rebar = new RebarIngester(this.conn);
    rebar.insert(comm);
    rebar.close();
    
    Iterator<Entry<Key, Value>> iter = this.generateIterator(new Range(rowId));
    assertTrue("Should find results in accumulo.", iter.hasNext());
    assertEquals(0, iter.next().getValue().compareTo(commBytes));
    Jedis jedis = new Jedis("localhost");
    assertTrue(jedis.smembers("ingested-ids").contains(docId));
    jedis.srem("ingested-ids", docId);
  }
  
  @Test
  public void testInsertDuplicateCommunications() throws RebarException, ConcreteException, TableNotFoundException {
    IndexedCommunication comm = this.generateMockCommunication(Kind.TWEET);
    String docId = comm.getGuid().getCommunicationId();
    
    RebarIngester rebar = new RebarIngester(this.conn);
    rebar.insert(comm);
    rebar.insert(comm);
    rebar.insert(comm);
    rebar.close();
    
    Iterator<Entry<Key, Value>> iter = this.generateIterator(new Range());
    assertTrue("Should find results in accumulo.", iter.hasNext());
    
    
    assertEquals(1, this.countIteratorResults(iter));
    Jedis jedis = new Jedis("localhost");
    assertTrue(jedis.smembers("ingested-ids").contains(docId));
    jedis.srem("ingested-ids", docId);
  }
  
  @Test
  public void testInsertVariedCommunications() throws RebarException, ConcreteException, TableNotFoundException {
    IndexedCommunication comm = this.generateMockCommunication(Kind.TWEET);
    String docId = comm.getGuid().getCommunicationId();
    
    IndexedCommunication comm2 = this.generateMockCommunication(Kind.TWEET);
    String docId2 = comm2.getGuid().getCommunicationId();
    
    IndexedCommunication comm3 = this.generateMockCommunication(Kind.TWEET);
    String docId3 = comm3.getGuid().getCommunicationId();
    
    RebarIngester rebar = new RebarIngester(this.conn);
    rebar.insert(comm);
    rebar.insert(comm2);
    rebar.insert(comm3);
    rebar.close();
    
    Iterator<Entry<Key, Value>> iter = this.generateIterator(new Range());
    assertTrue("Should find results in accumulo.", iter.hasNext());
    
    assertEquals(3, this.countIteratorResults(iter));
    Jedis jedis = new Jedis("localhost");
    Set<String> jedisSet = jedis.smembers("ingested-ids");
    assertTrue(jedisSet.contains(docId));
    assertTrue(jedisSet.contains(docId2));
    assertTrue(jedisSet.contains(docId3));
    jedis.srem("ingested-ids", docId, docId2, docId3);
  }
  
  @Test
  public void testInsertDocument() throws TException, RebarException, TableNotFoundException {
    Document d = this.generateMockDocument();
    String rowId = RebarIngester.generateRowId(d);
    String docId = d.id;
    byte[] dbytes = this.serializer.serialize(d);
    
    RebarIngester rebar = new RebarIngester(this.conn);
    rebar.ingest(d);
    rebar.close();
    
    Iterator<Entry<Key, Value>> iter = this.generateIterator(new Range(rowId));
    assertTrue("Should find results in accumulo.", iter.hasNext());
    assertEquals(0, iter.next().getValue().compareTo(dbytes));
    Jedis jedis = new Jedis("localhost");
    assertTrue(jedis.smembers("ingested-ids").contains(docId));
    jedis.srem("ingested-ids", docId);
  }
}
