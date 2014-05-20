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
import java.util.UUID;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
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
import edu.jhu.hlt.concrete.LanguageIdentification;
import edu.jhu.hlt.grommet.Stage;
import edu.jhu.hlt.grommet.StageType;
import edu.jhu.hlt.rebar.RebarException;
import edu.jhu.hlt.rebar.Util;

/**
 * @author max
 * 
 */
public class AbstractAccumuloTest {

  protected Connector conn;
  protected RebarTableOps tableOps;
  protected TSerializer serializer;
  protected TDeserializer deserializer;

  protected static final Random rand = new Random();

  /**
   * @param inst TODO
   * 
   */
  public AbstractAccumuloTest() {

  }
  
  protected void initialize () throws RebarException {
    try {
      this.initialize(new MockInstance().getConnector("max", new PasswordToken("")));
    } catch (AccumuloException | AccumuloSecurityException e) {
      throw new RebarException(e);
    }
  }
  
  protected void initialize(Connector conn) {
    try {
      this.conn = conn;
      this.tableOps = new RebarTableOps(this.conn);
      this.serializer = new TSerializer(new TBinaryProtocol.Factory());
      this.deserializer = new TDeserializer(new TBinaryProtocol.Factory());
    } finally {
      
    }
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
    document.type = "Tweet";
    document.text = "The Roman army fled from the Alemanni. They returned to Capua after 3 weeks.";
    document.id = Integer.toString(Math.abs(rand.nextInt()));
    document.uuid = UUID.randomUUID().toString();

    return document;
  }

  public static Set<Communication> generateMockDocumentSet(int capacity) {
    Set<Communication> docSet = new HashSet<>(capacity);
    for (int i = 0; i < capacity; i++)
      docSet.add(generateMockDocument());

    return docSet;
  }
  
  public static List<Communication> generateMockDocumentList(int capacity) {
    Set<Communication> docSet = new HashSet<>(capacity);
    for (int i = 0; i < capacity; i++)
      docSet.add(generateMockDocument());

    return new ArrayList<>(docSet);
  }

  protected static Map<String, Double> generateLidMap() {
    Map<String, Double> LanguageIdentificationMap = new HashMap<>();
    LanguageIdentificationMap.put("eng", rand.nextDouble());
    LanguageIdentificationMap.put("spa", rand.nextDouble());
    LanguageIdentificationMap.put("fra", rand.nextDouble());
    return LanguageIdentificationMap;
  }

  protected static LanguageIdentification generateLanguageIdentification(Communication d) {
    LanguageIdentification lid = new LanguageIdentification();
    lid.uuid = d.id + "_LID";
    //lid.name = "max_lid_test";
    //lid.version = "v1";
    lid.languageToProbabilityMap = generateLidMap();
    return lid;
  }

  protected static Set<LanguageIdentification> generateLanguageIdentificationSet(Set<Communication> docSet) {
    Set<LanguageIdentification> lidSet = new HashSet<>();
    for (Communication d : docSet)
      lidSet.add(generateLanguageIdentification(d));

    return lidSet;
  }

  protected static Stage generateTestStage() {
    return new Stage("stage_foo", "Foo stage for testing", Util.getCurrentUnixTime(), new HashSet<String>(), StageType.LANG_ID);
  }
  
  protected static Stage generateTestStage(String name, String desc, Set<String> deps, StageType sType) {
    return new Stage(name, desc, Util.getCurrentUnixTime(), deps, sType);
  }

  protected List<Communication> ingestDocuments(int nDocs) throws RebarException, TException {
    List<Communication> docList = new ArrayList<>(generateMockDocumentSet(nDocs));
    try (CleanIngester re = new CleanIngester(this.conn);) {
      for (Communication d : docList)
        re.ingest(d);
    }
    
    return docList;
  }
}
