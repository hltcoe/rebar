/**
 * 
 */
package edu.jhu.hlt.rebar.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.basho.riak.client.raw.http.HTTPClientConfig;
import com.basho.riak.client.raw.http.HTTPClusterConfig;

/**
 * @author max
 * 
 */
public class RiakConfiguration {

  private static final Logger logger = LoggerFactory.getLogger(RiakConfiguration.class);

  private static final HTTPClusterConfig config;
  private static final Properties props;

  public static final String RIAK_CONFIG_FILE = "riak.properties";

  static {
    config = new HTTPClusterConfig(500);
    props = new Properties();
    InputStream stream = null;
    try {
      stream = RebarConfiguration.class.getClassLoader().getResourceAsStream(RIAK_CONFIG_FILE);
      if (stream == null)
        throw new RuntimeException("Problem finding " + RIAK_CONFIG_FILE + " on the classpath.");
      props.load(stream);
    } catch (IOException e) {
      throw new RuntimeException("Failed to load properties file " + RIAK_CONFIG_FILE + "!", e);
    } finally {
      try {
        stream.close();
      } catch (IOException ioe) {
        logger.trace("Caught IOE", ioe);
      }
    }
    // Configure the logger.
    PropertyConfigurator.configure(props);

    // Establish riak HTTP cluster
    String[] hostnames = props.getProperty("riak_cluster_ips").split(",");
    int port = Integer.parseInt(props.getProperty("riak_port"));
    for (String host : hostnames) {
      HTTPClientConfig node = new HTTPClientConfig.Builder().withHost(host).withPort(port).build();
      config.addClient(node);
    }
  };

  /**
     * 
     */
  private RiakConfiguration() {

  }

  public static HTTPClusterConfig generateHTTPClusterConfig() {
    return config;
  }

  public static String getFeatureBucketName() {
    return props.getProperty("feature_bucket");
  }

  public static String getVertexBucketName() {
    return props.getProperty("vertex_bucket");
  }
}
