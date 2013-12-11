/**
 *
 */
package rebar.config

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

/**
 * @author max
 *
 */
class RebarConfig (config: Config) {
  config.checkValid(ConfigFactory.defaultReference(), "rebar")
  
  def this() = this(ConfigFactory.load())
  println (config.toString())
  
  private val accumuloConfig = config getConfig "rebar.accumulo"
  val useMock = accumuloConfig getBoolean "useMock"
  val accumuloInstance = accumuloConfig getString "instance"
  val zookeepers = accumuloConfig getString "zookeepers"
  val accumuloUser = accumuloConfig getString "user"
  val accumuloPass = accumuloConfig getString "password"
  
  private val redisConfig = config getConfig "rebar.redis"
  val redisServer = redisConfig getString "server"
  val ingestedIdKey = redisConfig getString "ingested-id-key"
}

object RebarConfigDemo extends App {
  val conf = new RebarConfig()
  println(conf.accumuloInstance)
}