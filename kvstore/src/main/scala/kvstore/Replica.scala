package kvstore

import akka.actor.{Actor, ActorRef, PoisonPill, Props, Stash, SupervisorStrategy, Terminated}
import kvstore.Arbiter._

import scala.concurrent.duration._

object Replica {

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))

  sealed trait Operation {
    def key: String

    def id: Long
  }

  sealed trait OperationReply

  case class Insert(key: String, value: String, id: Long) extends Operation

  case class Remove(key: String, id: Long) extends Operation

  case class Get(key: String, id: Long) extends Operation

  case class OperationAck(id: Long) extends OperationReply

  case class OperationFailed(id: Long) extends OperationReply

  case class GetResult(key: String, valueOption: Option[String], id: Long) extends OperationReply

  case object RetryPersist

  case object TimeOut
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor with Stash {

  import Persistence._
  import Replica._
  import Replicator._
  import context.dispatcher

  import scala.language.postfixOps

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  override val supervisorStrategy = SupervisorStrategy.stoppingStrategy
  /* TODO Behavior for  the leader role. */
  val leader: Receive = {
    case Insert(key, value, id) =>
      kv += key -> value
      val persist = Persist(key, Some(value), id)
      persistence ! persist
      replicators foreach {
        _ ! Replicate(key, Some(value), id)
      }
      context.become(leaderWaitingToBeModified(sender(), persist), discardOld = false)
    case Remove(key, id) =>
      kv -= key
      val persist = Persist(key, None, id)
      persistence ! persist
      replicators foreach {
        _ ! Replicate(key, None, id)
      }
      context.become(leaderWaitingToBeModified(sender(), persist), discardOld = false)
    case Get(key, id) =>
      sender ! GetResult(key, kv.get(key), id)
    case Terminated(_) =>
      persistence = context.watch(context.actorOf(persistenceProps))
    case Replicas(rep) =>
      val diff = replicatorDifference(rep)
      replicators --= diff
  }
  /* TODO Behavior for the replica role. */
  val replica: Receive = {
    case Get(key, id) =>
      sender ! GetResult(key, kv.get(key), id)
    case Snapshot(key, option, seq) =>
      if (seq == expectedSeq) {
        val wPersist = Persist(key, option, seq)
        persistence ! Persist(key, option, seq)
        wPersist.valueOption match {
          case Some(value) => kv += key -> value
          case None => kv -= key
        }
        context.become(replicaWaitingPersistence(sender(), wPersist), discardOld = false)
      } else if (seq < expectedSeq) {
        sender ! SnapshotAck(key, seq)
      }
    case Terminated(_) =>
      persistence = context.watch(context.actorOf(persistenceProps))
  }
  var kv = Map.empty[String, String]
  var secondaries = Map.empty[ActorRef, ActorRef]
  var replicators = Set.empty[ActorRef]
  arbiter ! Join
  var persistence: ActorRef = context.watch(context.actorOf(persistenceProps))
  var expectedSeq = 0L

  def receive = {
    case JoinedPrimary => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }

  def leaderWaitingToBeModified(leaderSender: ActorRef, persist: Persist): Receive = {
    val pcan = context.system.scheduler.schedule(100 millis, 100 millis, self, RetryPersist)
    val rcan = context.system.scheduler.scheduleOnce(1 second, self, TimeOut)
    var rset = replicators

    return {
      case Persisted(key, id) if !pcan.isCancelled && id == persist.id =>
        pcan.cancel()
        if (rset.isEmpty) {
          rcan.cancel()
          leaderSender ! OperationAck(id)
          unstashAll()
          context.unbecome()
        }
      case Replicated(key, id) if !rcan.isCancelled && id == persist.id =>
        rset -= sender
        if (rset.isEmpty && pcan.isCancelled) {
          rcan.cancel()
          leaderSender ! OperationAck(id)
          unstashAll()
          context.unbecome()
        }
      case TimeOut =>
        pcan.cancel()
        leaderSender ! OperationFailed(persist.id)
        unstashAll()
        context.unbecome()
      case Terminated(_) =>
        persistence = context.watch(context.actorOf(persistenceProps))
        persistence ! persist
      case RetryPersist =>
        persistence ! persist
      case Get(key, id) =>
        sender ! GetResult(key, kv.get(key), id)
      case Replicas(rep) =>
        val diff = replicatorDifference(rep)
        rset --= diff
        replicators --= diff
        if (rset.isEmpty && pcan.isCancelled) {
          rcan.cancel()
          leaderSender ! OperationAck(persist.id)
          unstashAll()
          context.unbecome()
        }
      case msg =>
        stash()
    }
  }

  def replicatorDifference(rep: Set[ActorRef]): Set[ActorRef] = {
    var newMap = Map.empty[ActorRef, ActorRef]
    rep foreach { r =>
      if (secondaries.contains(r)) {
        newMap += r -> secondaries(r)
        secondaries -= r
      } else if (r != self) {
        val newRep = context.actorOf(Replicator.props(r))
        replicators += newRep
        newMap += r -> newRep
        kv foreach { e =>
          newRep ! Replicate(e._1, Some(e._2), -1)
        }
      }
    }
    val ret = secondaries.values.toSet
    ret foreach { _ ! PoisonPill }
    secondaries = newMap
    ret
  }

  def replicaWaitingPersistence(replicateSender: ActorRef, persist: Persist): Receive = {
    val pcan = context.system.scheduler.schedule(100 millis, 100 millis, self, RetryPersist)

    return {
      case Persisted(key, seq) if seq == expectedSeq =>
        pcan.cancel()
        replicateSender ! SnapshotAck(key, seq)
        expectedSeq += 1
        unstashAll()
        context.unbecome()
      case Get(key, id) =>
        sender ! GetResult(key, kv.get(key), id)
      case Terminated(_) =>
        persistence = context.watch(context.actorOf(persistenceProps))
        persistence ! persist
      case RetryPersist =>
        persistence ! persist
      case msg =>
        stash()
    }
  }

}