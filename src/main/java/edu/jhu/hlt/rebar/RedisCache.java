/**
 * 
 */
package edu.jhu.hlt.rebar;

import java.util.Set;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.exceptions.JedisConnectionException;

import com.maxjthomas.dumpster.Document;

/**
 * @author max
 *
 */
public class RedisCache {

  public static final JedisPool POOL = new JedisPool(new JedisPoolConfig(), "localhost");
  
  /**
   * 
   */
  private RedisCache() {
    
  }
  
  public static Set<String> getIngestedIds() throws RebarException {
    Jedis j = POOL.getResource();
    try {
      return j.smembers(Constants.INGESTED_IDS_REDIS_KEY);
    } catch (JedisConnectionException e) {
      POOL.returnBrokenResource(j);
      throw new RebarException(e);
    } finally {
      POOL.returnResource(j);
    }
  }
  
  public static boolean isDocumentIngested(Document d) throws RebarException {
    return getIngestedIds().contains(d.getId());
  }
}
