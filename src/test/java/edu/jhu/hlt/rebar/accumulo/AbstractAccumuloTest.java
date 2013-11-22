/**
 * 
 */
package edu.jhu.hlt.rebar.accumulo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
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

import edu.jhu.hlt.concrete.Communication;
import edu.jhu.hlt.concrete.DocType;
import edu.jhu.hlt.concrete.LangId;
import edu.jhu.hlt.concrete.Stage;
import edu.jhu.hlt.concrete.StageType;
import edu.jhu.hlt.rebar.RebarException;
import edu.jhu.hlt.rebar.Util;

/**
 * @author max
 * 
 */
public class AbstractAccumuloTest {

  protected Instance inst;
  protected Connector conn;
  protected RebarTableOps tableOps;
  protected TSerializer serializer;
  protected TDeserializer deserializer;

  protected static final Random rand = new Random();

  /**
   * 
   */
  public AbstractAccumuloTest() {
    // TODO Auto-generated constructor stub
  }

  protected void initialize() throws AccumuloException, AccumuloSecurityException {
    this.inst = new MockInstance();
    this.conn = this.inst.getConnector("max", new PasswordToken(""));
    this.tableOps = new RebarTableOps(conn);
    this.serializer = new TSerializer(new TBinaryProtocol.Factory());
    this.deserializer = new TDeserializer(new TBinaryProtocol.Factory());
  }

  public static Iterator<Entry<Key, Value>> generateIterator(Connector conn, String tableName, Range range) throws TableNotFoundException {
    Scanner sc = conn.createScanner(tableName, Constants.NO_AUTHS);
    sc.setRange(range);
    return sc.iterator();
  }
  
  public static Iterator<Entry<Key, Value>> generateIterator(String tableName, Range range) throws TableNotFoundException, RebarException {
    return generateIterator(edu.jhu.hlt.rebar.Constants.getConnector(), tableName, range);
  }
  

  public static Communication generateMockDocument() {
    Communication document = new Communication();
    document.type = DocType.TWEET;
    document.text = "hello world!";
    document.id = Integer.toString(Math.abs(rand.nextInt()));

    return document;
  }

  public static Set<Communication> generateMockDocumentSet(int capacity) {
    Set<Communication> docSet = new HashSet<>(capacity);
    for (int i = 0; i < capacity; i++)
      docSet.add(generateMockDocument());

    return docSet;
  }

  protected static Map<String, Double> generateLidMap() {
    Map<String, Double> langIdMap = new HashMap<>();
    langIdMap.put("eng", rand.nextDouble());
    langIdMap.put("spa", rand.nextDouble());
    langIdMap.put("fra", rand.nextDouble());
    return langIdMap;
  }

  protected static LangId generateLangId(Communication d) {
    LangId lid = new LangId();
    lid.id = d.id + "_LID";
    lid.name = "max_lid_test";
    lid.version = "v1";
    lid.languageToProbabilityMap = generateLidMap();
    return lid;
  }

  protected static Set<LangId> generateLangIdSet(Set<Communication> docSet) {
    Set<LangId> lidSet = new HashSet<>();
    for (Communication d : docSet)
      lidSet.add(generateLangId(d));

    return lidSet;
  }

  protected static Stage generateTestStage() {
    return new Stage("stage_foo", "Foo stage for testing", Util.getCurrentUnixTime(), new HashSet<String>(), StageType.LANG_ID);
  }

  protected List<Communication> ingestDocuments(int nDocs) throws RebarException, TException {
    List<Communication> docList = new ArrayList<>(generateMockDocumentSet(nDocs));
    try (RebarIngester re = new RebarIngester(this.conn);) {
      for (Communication d : docList)
        re.ingest(d);
    }
    
    return docList;
  }
}