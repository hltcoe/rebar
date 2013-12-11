/**
 *
 */
package edu.jhu.rebar.config

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

/**
 * @author max
 *
 */
class Configuration (config: Config) {
  config.checkValid(ConfigFactory.defaultReference(), "rebar")
  
  def this() = this(ConfigFactory.load())
  println (config.toString())
  
  val accumuloConfig = config getConfig "rebar.accumulo"
  def useMock = accumuloConfig getBoolean "useMock"
  def accumuloInstance = accumuloConfig getString "instance"
  def zookeepers = accumuloConfig getString "zookeepers"
  def accumuloUser = accumuloConfig getString "user"
  def accumuloPass = accumuloConfig getString "password"
  
  val redisConfig = config getConfig "rebar.redis"
  def redisServer = redisConfig getString "server"
  def ingestedIdKey = redisConfig getString "ingested-id-key"
}

