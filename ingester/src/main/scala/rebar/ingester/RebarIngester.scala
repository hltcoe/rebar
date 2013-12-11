/**
 *
 */
package rebar.ingester

import akka.actor.{ ActorRef, ActorSystem, Props, Actor, Inbox }
import scala.concurrent.duration._
import edu.jhu.hlt.concrete.Communication

/**
 * @author max
 *
 */

case class CommWrapper(comm: Communication)

class IngesterActor extends Actor {
  //val conn = 
  
  def receive = {
    case CommWrapper(comm)        => println(comm.getId())
  }
}


object RebarIngester extends App {
  println("Starting!")
  
  // Create the 'rebar-ingester' actor system
  val system = ActorSystem("rebar-ingester")

  // Create the 'ingester' actor
  val ingester = system.actorOf(Props[IngesterActor], "ingester-1")

  // Create an "actor-in-a-box"
  //val inbox = Inbox.create(system)
  val testComm = new Communication().setId("foo")

  ingester ! new CommWrapper(testComm)
}