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

import edu.jhu.hlt.rebar.RebarException;


/**
 * @author max
 *
 */
public class AbstractMiniClusterTest extends AbstractAccumuloTest implements AutoCloseable {

  protected MiniAccumuloConfig cfg;
  protected MiniAccumuloCluster cluster;
  
  protected final void initialize(File dir, String pw) throws RebarException {
    try {
      // this.cfg = cfg;
      this.cluster = new MiniAccumuloCluster(dir, pw);
      this.cluster.start();
      Instance inst = new ZooKeeperInstance(this.cluster.getInstanceName(), this.cluster.getZooKeepers());
      Connector conn = inst.getConnector("max", new PasswordToken(pw));
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
