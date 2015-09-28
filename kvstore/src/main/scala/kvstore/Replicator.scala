package kvstore

import akka.actor.{Actor, ActorRef, Props}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

object Replicator {

  def props(replica: ActorRef): Props = Props(new Replicator(replica))

  case class Replicate(key: String, valueOption: Option[String], id: Long)

  case class Replicated(key: String, id: Long)

  case class Snapshot(key: String, valueOption: Option[String], seq: Long)

  case class SnapshotAck(key: String, seq: Long)

  case object RetrySnapShot
}

class Replicator(val replica: ActorRef) extends Actor {

  import Replicator._

  import scala.language.postfixOps

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  // map from sequence number to pair of sender and request
  var acks = Map.empty[Long, (ActorRef, Replicate)]
  // a sequence of not-yet-sent snapshots (you can disregard this if not implementing batching)
  var pending = Vector.empty[Snapshot]

  var _seqCounter = 0L

  context.system.scheduler.schedule(100 millis, 100 millis, self, RetrySnapShot)

  /* TODO Behavior for the Replicator. */
  def receive: Receive = {
    case rep: Replicate =>
      acks += _seqCounter ->(sender(), rep)
      replica ! Snapshot(rep.key, rep.valueOption, _seqCounter)
      nextSeq
    case SnapshotAck(key, seq) if acks.contains(seq) =>
      acks(seq) match {
        case (actor, Replicate(vKey, _, id)) =>
          actor ! Replicated(vKey, id)
      }
      acks -= seq
    case RetrySnapShot if acks.nonEmpty =>
      acks.minBy(_._1) match {
        case (seq, (_, replicate)) =>
          replica ! Snapshot(replicate.key, replicate.valueOption, seq)
      }
  }

  def nextSeq = {
    val ret = _seqCounter
    _seqCounter += 1
    ret
  }
}