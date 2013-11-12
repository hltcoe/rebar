/**
 * 
 */
package edu.jhu.hlt.rebar.accumulo;

import org.apache.accumulo.core.cli.BatchWriterOpts;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;

import edu.jhu.hlt.rebar.RebarException;
import edu.jhu.hlt.rebar.config.RebarConfiguration;

/**
 * @author max
 * 
 */
public abstract class AbstractAccumuloClient implements AutoCloseable {

  protected final Connector conn;
  protected final RebarTableOps tableOps;
  protected final TSerializer serializer;
  
  protected static final String colFamilyRoot = "root";
  protected static final String colFamilyAnnotations = "anno";
  protected static final String documentTableName = "documents";
  protected static final BatchWriterOpts defaultBwOpts;
  
  protected BatchWriter bw;
  
  protected final static long MAX_WRITE_MEMORY = 1024L * 1024 * 5; // 5 mb
  protected final static long MAX_WRITE_LATENCY_MS = 5L; // 5 msec
  protected final static int NUM_WRITE_THREADS = 5;
  protected final static int NUM_DELETER_QUERY_THREADS = 5;
  protected final static int NUM_QUERY_THREADS = 8;
  
  static {
    defaultBwOpts = new BatchWriterOpts();
    defaultBwOpts.batchLatency = MAX_WRITE_LATENCY_MS;
    defaultBwOpts.batchMemory = MAX_WRITE_MEMORY;
    defaultBwOpts.batchThreads = NUM_WRITE_THREADS;
    defaultBwOpts.batchTimeout = 3000L;
  }

  /**
   * @throws RebarException 
   * 
   */
  public AbstractAccumuloClient() throws RebarException {
    this(getConnector());
  }

  public AbstractAccumuloClient(Connector conn) throws RebarException {
    this.conn = conn;
    this.tableOps = new RebarTableOps(this.conn);
    this.tableOps.createTableIfNotExists(documentTableName);
    this.serializer = new TSerializer(new TBinaryProtocol.Factory());
    try {
      this.bw = this.conn.createBatchWriter(documentTableName, defaultBwOpts.getBatchWriterConfig());
    } catch (TableNotFoundException e) {
      throw new RebarException(e);
    }
  }
  
  public static Connector getConnector() throws RebarException {
    Instance zki = new ZooKeeperInstance(RebarConfiguration.getAccumuloInstanceName(), RebarConfiguration.getZookeeperServer());
    try {
      return zki.getConnector(RebarConfiguration.getAccumuloUser(), RebarConfiguration.getPasswordToken());
    } catch (AccumuloException e) {
      throw new RebarException(e);
    } catch (AccumuloSecurityException e) {
      throw new RebarException(e);
    }
  }
  
  public abstract void flush() throws RebarException;
}
