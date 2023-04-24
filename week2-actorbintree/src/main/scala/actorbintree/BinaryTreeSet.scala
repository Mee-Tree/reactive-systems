/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package actorbintree

import akka.actor.*
import scala.collection.immutable.Queue

object BinaryTreeSet:

  trait Operation:
    def requester: ActorRef
    def id: Int
    def elem: Int

  trait OperationReply:
    def id: Int

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

  /** Request to perform garbage collection */
  case object GC

  /** Holds the answer to the Contains request with identifier `id`.
    * `result` is true if and only if the element is present in the tree.
    */
  case class ContainsResult(id: Int, result: Boolean) extends OperationReply

  /** Message to signal successful completion of an insert or remove operation. */
  case class OperationFinished(id: Int) extends OperationReply



class BinaryTreeSet extends Actor:
  import BinaryTreeSet.*
  import BinaryTreeNode.*

  def createRoot: ActorRef = context.actorOf(BinaryTreeNode.props(0, initiallyRemoved = true))

  var root = createRoot

  def receive = normal

  /** Accepts `Operation` and `GC` messages. */
  val normal: Receive = {
    case op: Operation => root ! op
    case GC            =>
      val newRoot = createRoot
      val pending = Queue.empty[Operation]
      // garbage goober
      val goober = garbageCollecting(newRoot, pending)
      root ! CopyTo(newRoot)
      context.become(goober)
  }

  /** Handles messages while garbage collection is performed.
    * `newRoot` is the root of the new binary tree where we want to copy
    * all non-removed elements into.
    */
  def garbageCollecting(newRoot: ActorRef, pending: Queue[Operation]): Receive = {
    case GC            => ()
    case op: Operation =>
      val goober = garbageCollecting(newRoot, pending :+ op)
      context.become(goober)
    case CopyFinished  =>
      root = newRoot
      pending.foreach { op => root ! op }
      context.become(normal)
  }


object BinaryTreeNode:
  trait Position

  case object Left extends Position
  case object Right extends Position

  case class CopyTo(treeNode: ActorRef)
  /**
   * Acknowledges that a copy has been completed. This message should be sent
   * from a node to its parent, when this node and all its children nodes have
   * finished being copied.
   */
  case object CopyFinished

  def props(elem: Int, initiallyRemoved: Boolean) =
    Props(classOf[BinaryTreeNode], elem, initiallyRemoved)

class BinaryTreeNode(val elem: Int, initiallyRemoved: Boolean) extends Actor:
  import BinaryTreeNode.*
  import BinaryTreeSet.*

  var subtrees = Map[Position, ActorRef]()
  var removed = initiallyRemoved

  def createChild(elem: Int): ActorRef =
    context.actorOf(props(elem, initiallyRemoved = false))

  def receive = normal

  /** Handles `Operation` messages and `CopyTo` requests. */
  val normal: Receive = {
    case op @ Insert(requester, id, elem)   =>
      handle(op) {
        removed = false
        requester ! OperationFinished(id)
      } { pos =>
        subtrees = subtrees + (pos -> createChild(op.elem))
        requester ! OperationFinished(id)
      }

    case op @ Contains(requester, id, elem) =>
      handle(op) {
        requester ! ContainsResult(id, !removed)
      } { _ =>
        requester ! ContainsResult(id, false)
      }

    case op @ Remove(requester, id, elem)   =>
      handle(op) {
        removed = true
        requester ! OperationFinished(id)
      } { _ =>
        requester ! OperationFinished(id)
      }

    case copyTo @ CopyTo(tree)              =>
      if (!removed) { tree ! Insert(self, -1, elem) }
      val children = subtrees.values.toSet
      children.foreach { child => child ! copyTo }
      val requester = sender()
      copy(requester, children, insertConfirmed = removed)
  }

  def handle(op: Operation)(onCurrent: => Unit)(onNotExists: Position => Unit): Unit =
    if (op.elem < elem)      propagateTo(Left, op)(onNotExists)
    else if (op.elem > elem) propagateTo(Right, op)(onNotExists)
    else                     onCurrent

  def propagateTo(position: Position, op: Operation)(onNotExists: Position => Unit): Unit =
    subtrees.get(position).fold(onNotExists(position))(child => child ! op)

  def copy(requester: ActorRef, expected: Set[ActorRef], insertConfirmed: Boolean): Unit =
    if (expected.isEmpty && insertConfirmed) {
      requester ! CopyFinished
      context.stop(self)
    } else {
      context.become(copying(requester, expected, insertConfirmed))
    }

  /** `expected` is the set of ActorRefs whose replies we are waiting for,
    * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
    */
  def copying(requester: ActorRef, expected: Set[ActorRef], insertConfirmed: Boolean): Receive = {
    case OperationFinished(-1) => copy(requester, expected, insertConfirmed = true)
    case CopyFinished          => copy(requester, expected - sender(), insertConfirmed)
  }
