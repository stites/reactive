/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package actorbintree

import akka.actor._
import scala.collection.immutable.Queue

object BinaryTreeSet {

  trait Operation {
    def requester: ActorRef
    def id: Int
    def elem: Int
  }

  trait OperationReply {
    def id: Int
  }

  /** Request with identifier `id` to insert an element `elem` into the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Insert(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to check whether an element `elem` is present
    * in the tree. The actor at reference `requester` should be notified when
    * this operation is completed.
    */
  case class Contains(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to remove the element `elem` from the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Remove(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request to perform garbage collection*/
  case object GC

  /** Holds the answer to the Contains request with identifier `id`.
    * `result` is true if and only if the element is present in the tree.
    */
  case class ContainsResult(id: Int, result: Boolean) extends OperationReply
  
  /** Message to signal successful completion of an insert or remove operation. */
  case class OperationFinished(id: Int) extends OperationReply

}


class BinaryTreeSet extends Actor {
  import BinaryTreeSet._
  import BinaryTreeNode._

  def createRoot: ActorRef = context.actorOf(BinaryTreeNode.props(0, initiallyRemoved = true))

  var root = createRoot

  // optional
  var pendingQueue = Queue.empty[Operation]

  // optional
  def receive = normal

  // optional
  /** Accepts `Operation` and `GC` messages. */
  val normal: Receive = {
    case message:Operation => root ! message
    case GC => {
      val newRoot = createRoot
      context.become(garbageCollecting(newRoot))
      root ! CopyTo(newRoot)
    }
  }

  // optional
  /** Handles messages while garbage collection is performed.
    * `newRoot` is the root of the new binary tree where we want to copy
    * all non-removed elements into.
    */
  def garbageCollecting(newRoot: ActorRef): Receive = {
    case message:Operation => pendingQueue = pendingQueue.enqueue(message)
    case CopyFinished => {
      while (!pendingQueue.isEmpty) {
        pendingQueue.dequeue match {
          case (operation, queue) => {
            newRoot ! operation
            pendingQueue = queue
          }
        }
      }
      root ! PoisonPill
      root = newRoot
      context.become(normal)
    }
  }

}

object BinaryTreeNode {
  trait Position

  case object Left extends Position
  case object Right extends Position

  case class CopyTo(treeNode: ActorRef)
  case object CopyFinished

  def props(elem: Int, initiallyRemoved: Boolean) = Props(classOf[BinaryTreeNode],  elem, initiallyRemoved)
}

class BinaryTreeNode(val elem: Int, initiallyRemoved: Boolean) extends Actor {
  import BinaryTreeNode._
  import BinaryTreeSet._

  var subtrees = Map[Position, ActorRef]()
  var removed = initiallyRemoved

  // optional
  def receive = normal

  // optional
  /** Handles `Operation` messages and `CopyTo` requests. */
  val normal: Receive = {

    case msg @ Insert(requester, id, newElem) => {
      newElem match {
        case `elem` => {
          removed = false
          requester ! OperationFinished(id)
        }
        case _ => {
          val direction = traverseDirection(newElem)

          subtrees.get(direction) match {
            case Some(act) => act ! msg
            case None => {
              subtrees += (direction -> context.actorOf(props(newElem, false), id.toString))
              requester ! OperationFinished(id)
            }
          }

        }
      }
    }

    case msg @ Contains(requester, id, reqElem) => {
      reqElem match {
        case `elem` => {
          removed match {
            case false => requester ! ContainsResult(id, true)
            case true => requester ! ContainsResult(id, false)
          }
        }
        case _ => {
          subtrees.get(traverseDirection(reqElem)) match {
            case Some(act) => act ! msg
            case None => requester ! ContainsResult(id, false)
          }
        }
      }
    }

    case msg @ Remove(requester, id, reqElement) => {
      reqElement match {
        case `elem` => {
          removed = true
          requester ! OperationFinished(id)
        }
        case _ => {
          subtrees.get(traverseDirection(reqElement)) match {
            case Some(act) => act ! msg
            case None => requester ! OperationFinished(id)
          }

        }
      }
    }

    case CopyTo(target) => {
      subtrees.values.foreach { act => act ! CopyTo(target) }
      context.become(copying(subtrees.values.toSet, removed))

      removed match {
        case true => self ! OperationFinished(elem)
        case false => target ! Insert(self, elem, elem)
      }
    }

  }

  // optional
  /** `expected` is the set of ActorRefs whose replies we are waiting for,
    * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
    */
  def copying(expected: Set[ActorRef], insertConfirmed: Boolean): Receive = ???


}
