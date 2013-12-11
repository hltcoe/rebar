/**
 *
 */
package edu.jhu.rebar.vortex

import akka.actor.{ ActorRef, ActorSystem, Props, Actor, Inbox }
import scala.concurrent.duration._
import edu.jhu.hlt.concrete.Communication
import org.apache.accumulo.core.client.mock.MockInstance
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import rebar.config.RebarConfig
import edu.jhu.hlt.rebar.accumulo.RebarTableOps

/**
 * @author max
 *
 */

case class CommWrapper(comm: Communication)

class IngesterActor extends Actor {
  val conn = new MockInstance().getConnector("max", new PasswordToken(""))
  val tableOps = new RebarTableOps(conn)
  tableOps.createTableIfNotExists("communications")
  
  def receive = {
    case CommWrapper(comm)        => println(comm.getId())
  }
}


object Vortex extends App {
  // Create the 'vortex' actor system
  val system = ActorSystem("vortex")

  // Create the 'ingester' actor
  val ingester = system.actorOf(Props[IngesterActor], "vortex-1")

  val testComm = new Communication().setId("foo")

  ingester ! new CommWrapper(testComm)
}