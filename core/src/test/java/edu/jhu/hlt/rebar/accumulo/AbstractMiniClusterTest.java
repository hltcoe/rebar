/**
 * 
 */
package edu.jhu.hlt.rebar.accumulo;

import java.io.File;
import java.io.IOException;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.jhu.hlt.rebar.RebarException;


/**
 * @author max
 *
 */
public class AbstractMiniClusterTest extends AbstractAccumuloTest implements AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(AbstractMiniClusterTest.class);
  
  protected MiniAccumuloConfig cfg;
  protected MiniAccumuloCluster cluster;
  
  protected final void initialize(File dir, String pw) throws RebarException {
    try {
      // this.cfg = cfg;
      this.cluster = new MiniAccumuloCluster(dir, pw);
      this.cluster.start();
      logger.info("Started. Sleeping.");
      Thread.sleep(3000);
      Instance inst = new ZooKeeperInstance(this.cluster.getInstanceName(), this.cluster.getZooKeepers());
      logger.info("Got instance.");
      Connector conn = inst.getConnector("root", new PasswordToken(pw));
      logger.info("Got connector.");
      super.initialize(conn);
    } catch (IOException | InterruptedException | AccumuloException | AccumuloSecurityException e) {
      throw new RebarException(e);
    }
  }

  /* (non-Javadoc)
   * @see java.lang.AutoCloseable#close()
   */
  @Override
  public void close() throws Exception {
    this.cluster.stop();
  }
}
