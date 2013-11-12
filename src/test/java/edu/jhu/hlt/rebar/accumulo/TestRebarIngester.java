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

import com.maxjthomas.dumpster.Document;
import com.maxjthomas.dumpster.DocType;

import edu.jhu.hlt.concrete.Concrete.Communication;
import edu.jhu.hlt.concrete.Concrete.Communication.Kind;
import edu.jhu.hlt.concrete.Concrete.CommunicationGUID;
import edu.jhu.hlt.concrete.ConcreteException;
import edu.jhu.hlt.concrete.index.IndexedCommunication;
import edu.jhu.hlt.concrete.util.IdUtil;
import edu.jhu.hlt.rebar.RebarException;
import edu.jhu.hlt.rebar.config.RebarConfiguration;

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
  private TDeserializer deserializer;

  private static final Random rand = new Random();
  
  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {
    this.commList = generateMockCommunications(10000, Kind.TWEET);
    this.inst = new MockInstance();
    this.conn = this.inst.getConnector("max", new PasswordToken(""));
    this.tableOps = new RebarTableOps(conn);
    this.serializer = new TSerializer(new TBinaryProtocol.Factory());
    this.deserializer = new TDeserializer(new TBinaryProtocol.Factory());
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
    int randCommId = Math.abs(rand.nextInt());
    long ts = (long)(rand.nextInt(300000000) * rand.nextFloat()) + 1288061605; // min date --> oct 2010 ish
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
  
  public static Document generateMockDocument() {
    Document document = new Document();
    document.t = DocType.TWEET;
    document.text = "hello world!";
    document.id = Integer.toString(Math.abs(rand.nextInt()));
    
    return document;
  }
  
  public static Set<Document> generateMockDocumentSet(int capacity) {
    Set<Document> docSet = new HashSet<>(capacity);
    for (int i = 0; i < capacity ; i++) 
      docSet.add(generateMockDocument());
    
    return docSet;
  }
  
  static Iterator<Entry<Key, Value>> generateIterator(Connector conn, String tableName, Range range) throws TableNotFoundException {
    Scanner sc = conn.createScanner(tableName, Constants.NO_AUTHS);
    sc.setRange(range);
    return sc.iterator();
  }
  
  static int countIteratorResults(Iterator<Entry<Key, Value>> iter) {
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
    
    Iterator<Entry<Key, Value>> iter = generateIterator(this.conn, RebarConfiguration.DOCUMENT_TABLE_NAME, new Range(rowId));
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
    
    Iterator<Entry<Key, Value>> iter = generateIterator(this.conn, RebarConfiguration.DOCUMENT_TABLE_NAME, new Range());
    assertTrue("Should find results in accumulo.", iter.hasNext());
    
    
    assertEquals(1, countIteratorResults(iter));
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
    
    Iterator<Entry<Key, Value>> iter = generateIterator(this.conn, RebarConfiguration.DOCUMENT_TABLE_NAME, new Range());
    assertTrue("Should find results in accumulo.", iter.hasNext());
    
    assertEquals(3, countIteratorResults(iter));
    Jedis jedis = new Jedis("localhost");
    Set<String> jedisSet = jedis.smembers("ingested-ids");
    assertTrue(jedisSet.contains(docId));
    assertTrue(jedisSet.contains(docId2));
    assertTrue(jedisSet.contains(docId3));
    jedis.srem("ingested-ids", docId, docId2, docId3);
  }
  
  @Test
  public void testInsertDocument() throws TException, RebarException, TableNotFoundException {
    Document d = generateMockDocument();
    //String rowId = RebarIngester.generateRowId(d);
    String docId = d.id;
    byte[] dbytes = this.serializer.serialize(d);
    
    RebarIngester rebar = new RebarIngester(this.conn);
    rebar.ingest(d);
    rebar.close();
    
    Iterator<Entry<Key, Value>> iter = generateIterator(this.conn, RebarConfiguration.DOCUMENT_TABLE_NAME, new Range(docId));
    assertTrue("Should find results in accumulo.", iter.hasNext());
    assertEquals(0, iter.next().getValue().compareTo(dbytes));
    Jedis jedis = new Jedis("localhost");
    assertTrue(jedis.smembers("ingested-ids").contains(docId));
    jedis.srem("ingested-ids", docId);
  }
  
  @Test
  public void testInsertManyDocuments() throws TException, RebarException, TableNotFoundException {
    int nDocs = 50;
    Set<Document> docs = generateMockDocumentSet(nDocs);
    
    RebarIngester rebar = new RebarIngester(this.conn);
    for (Document d : docs)
      rebar.ingest(d);
    rebar.close();
    
    Iterator<Entry<Key, Value>> iter = generateIterator(this.conn, RebarConfiguration.DOCUMENT_TABLE_NAME, new Range());
    assertEquals("Should find a few results in accumulo.", nDocs, countIteratorResults(iter));
    
    iter = generateIterator(this.conn, RebarConfiguration.DOCUMENT_TABLE_NAME, new Range());
    Set<Document> fetchDocs = new HashSet<>(nDocs);
    while(iter.hasNext()) {
      Document d = new Document();
      Value v = iter.next().getValue();
      this.deserializer.deserialize(d, v.get());
      fetchDocs.add(d);
    }
    
    BatchScanner bsc = this.conn.createBatchScanner(RebarConfiguration.DOCUMENT_TABLE_NAME, RebarConfiguration.getAuths(), 10);
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
