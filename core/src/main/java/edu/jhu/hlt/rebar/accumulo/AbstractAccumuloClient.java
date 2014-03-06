/**
 * 
 */
package edu.jhu.hlt.rebar.accumulo;

import org.apache.accumulo.core.cli.BatchWriterOpts;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;

import edu.jhu.hlt.rebar.Constants;
import edu.jhu.hlt.rebar.RebarException;

/**
 * @author max
 * 
 */
public abstract class AbstractAccumuloClient {

  protected final Connector conn;
  protected final RebarTableOps tableOps;
  protected final TSerializer serializer;
  protected final TDeserializer deserializer;
  
  public static final BatchWriterOpts defaultBwOpts;
  
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
    this(Constants.getConnector());
  }

  public AbstractAccumuloClient(Connector conn) throws RebarException {
    this.conn = conn;
    this.tableOps = new RebarTableOps(this.conn);
    
    this.serializer = new TSerializer(new TBinaryProtocol.Factory());
    this.deserializer = new TDeserializer(new TBinaryProtocol.Factory());
  }
  
  protected BatchWriter safeBatchWriter(String tableName) throws RebarException {
    try {
      this.tableOps.createTableIfNotExists(tableName);
      return this.conn.createBatchWriter(tableName, defaultBwOpts.getBatchWriterConfig());
    } catch (TableNotFoundException e) {
      throw new RebarException(e);
    }
  }
}
