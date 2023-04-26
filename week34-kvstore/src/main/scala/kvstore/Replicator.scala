package kvstore

import akka.actor.Props
import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.Cancellable
import akka.actor.Timers
import akka.actor.actorRef2Scala
import scala.concurrent.duration.*

object Replicator:
  case class Replicate(key: String, valueOption: Option[String], id: Long)
  case class Replicated(key: String, id: Long)

  case class Snapshot(key: String, valueOption: Option[String], seq: Long)
  case class SnapshotAck(key: String, seq: Long)

  def props(replica: ActorRef): Props = Props(Replicator(replica))

class Replicator(val replica: ActorRef) extends Actor with Timers:
  import Replicator.*
  import context.dispatcher
  import context.system

  val scheduler = system.scheduler

  // map from sequence number to tuple of sender, request, and scheduled snapshot
  var acks = Map.empty[Long, (ActorRef, Replicate, Cancellable)]
  // a sequence of not-yet-acknowledged snapshots
  var pending = Vector.empty[Snapshot]

  /* Behavior for the Replicator. */
  def receive: Receive = replicate(seq = 0L)

  def replicate(seq: Long): Receive =
    case request @ Replicate(key, opValue, id) =>
      // val batchable = pending.filter(_.key == key)
      val snapshot = /*batch(batchable :+ */Snapshot(key, opValue, seq) //)
      // pending = pending :+ snapshot

      val scheduled = scheduler
        .scheduleWithFixedDelay(100.millis, 100.millis, replica, snapshot)
      acks = acks + (seq -> (sender(), request, scheduled))
      context.become(replicate(seq + 1))

    case SnapshotAck(key, seq) =>
      // val (batchable, remaining) = pending
      //   .partition(s => s.key == key && s.seq <= seq)
      // pending = remaining
      // batchable.foreach { s =>
        val (receiver, request, scheduled) = acks(/*s.*/seq)
        if (!scheduled.isCancelled) scheduled.cancel()
        receiver ! Replicated(key, request.id)
      // }

  // def batch(snapshots: Vector[Snapshot]): Snapshot =
  //   snapshots.reduceLeft { (fst, snd) =>
  //     val (_, _, scheduled) = acks(fst.seq)
  //     scheduled.cancel()
  //     snd
  //   }
