package kvstore

import akka.actor.{ OneForOneStrategy, PoisonPill, Props, SupervisorStrategy, Terminated, ActorRef, Actor, actorRef2Scala }
import kvstore.Arbiter.*
import akka.pattern.{ ask, pipe }
import scala.concurrent.duration.*
import akka.util.Timeout
import akka.pattern.BackoffSupervisor.GetRestartCount
import akka.actor.SupervisorStrategy.Restart
import akka.actor.Cancellable
import akka.actor.Timers

object Replica:
  sealed trait Operation:
    def key: String
    def id: Long
  case class Insert(key: String, value: String, id: Long) extends Operation
  case class Remove(key: String, id: Long) extends Operation
  case class Get(key: String, id: Long) extends Operation

  sealed trait OperationReply
  case class OperationAck(id: Long) extends OperationReply
  case class OperationFailed(id: Long) extends OperationReply
  case class GetResult(key: String, valueOption: Option[String], id: Long) extends OperationReply

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(Replica(arbiter, persistenceProps))

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor with Timers:
  import Replica.*
  import Replicator.*
  import Persistence.*
  import context.dispatcher
  import context.system

  val scheduler = system.scheduler

  override def preStart(): Unit =
    arbiter ! Join

  def createReplicator(replica: ActorRef): ActorRef =
    context.actorOf(Replicator.props(replica))

  lazy val persistence = context.actorOf(persistenceProps)

  override val supervisorStrategy: OneForOneStrategy = OneForOneStrategy() {
    case _: PersistenceException => Restart
  }

  var kv = Map.empty[String, String]
  // map from persistence id to pair of sender and scheduled persist
  var persistAcks = Map.empty[Long, (ActorRef, Cancellable)]
  // map from persistence id to remaining replicators
  var replicateAcks = Map.empty[Long, Set[ActorRef]]
  // map from operation id to scheduled failure
  var failures = Map.empty[Long, Cancellable]

  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]

  def updateReplicasWith(replicas: Set[ActorRef]) =
    val newReplicas    = (replicas &~ secondaries.keySet) - self
    val newSecondaries = newReplicas.map(r => r -> createReplicator(r))
    val newReplicators = newSecondaries.map(_._2)

    val oldReplicas    = secondaries.keySet &~ replicas
    val oldReplicators = oldReplicas.map(secondaries)
    oldReplicators.foreach(context.stop)

    secondaries = secondaries ++ newSecondaries -- oldReplicas
    replicators = replicators &~ oldReplicators | newReplicators

  def receive =
    case JoinedPrimary   => context.become(primary)
    case JoinedSecondary => context.become(secondary(expectedSeq = 0L))

  val get: Receive =
    case Get(key, id) =>
      val opValue = kv.get(key)
      sender() ! GetResult(key, opValue, id)

  /* Behavior for the leader role. */
  val primary: Receive = get.orElse {
    case op: (Insert | Remove) =>
      val opValue = op match
        case Insert(_, value, _) => Some(value)
        case Remove(_, _)        => None
      persist(op.key, opValue, op.id)
      replicate(op.key, opValue, op.id)
      scheduleFailure(op.id)

    case Replicas(replicas) =>
      updateReplicasWith(replicas)
      replicateAll(id = -1)
      for id <- replicateAcks.keySet
      do
        updateRemaining(id)(_ & replicators)
        tryAck(id)

    case Persisted(_, id) if persistAcks.contains(id) =>
      val (receiver, persist) = persistAcks(id)
      persist.cancel()
      tryAck(id)

    case Replicated(key, id) if replicateAcks.contains(id) =>
      val replicator = sender()
      updateRemaining(id)(_ - replicator)
      tryAck(id)
  }

  /* Behavior for the replica role. */
  def secondary(expectedSeq: Long): Receive = get.orElse {
    case Insert(_, _, id) =>
      sender() ! OperationFailed(id)

    case Remove(_, id) =>
      sender() ! OperationFailed(id)

    case Snapshot(key, opValue, seq) =>
      if (seq > expectedSeq) ()
      else if (seq < expectedSeq) sender() ! SnapshotAck(key, seq)
      else
        persist(key, opValue, id = -seq)
        context.become(secondary(expectedSeq + 1))

    case Persisted(key, id) if persistAcks.contains(id) =>
      val (receiver, persist) = persistAcks(id)
      persist.cancel()
      receiver ! SnapshotAck(key, seq = -id)
  }

  def persist(key: String, opValue: Option[String], id: Long) =
    opValue match
      case None        => kv = kv - key
      case Some(value) => kv = kv + (key -> value)

    val persist = Persist(key, opValue, id)
    val scheduled = scheduler
      .scheduleWithFixedDelay(0.millis, 100.millis, persistence, persist)
    persistAcks = persistAcks + (id -> (sender(), scheduled))

  def scheduleFailure(id: Long) =
    val scheduled = scheduler
      .scheduleOnce(1.second, sender(), OperationFailed(id))
    failures = failures + (id -> scheduled)

  def replicate(key: String, opValue: Option[String], id: Long, awaitAcks: Boolean = true) =
    replicators.foreach(_ ! Replicate(key, opValue, id))
    if (awaitAcks) replicateAcks = replicateAcks + (id -> replicators)

  def replicateAll(id: Long) =
    for (key, value) <- kv
    do replicate(key, Some(value), id)

  def updateRemaining(id: Long)(f: Set[ActorRef] => Set[ActorRef]) =
    replicateAcks = replicateAcks.updatedWith(id)(_.map(f))

  def tryAck(id: Long) =
    for
      (receiver, persist) <- persistAcks.get(id)
      remaining <- replicateAcks.get(id)
      if remaining.isEmpty && persist.isCancelled
    do
      failures(id).cancel()
      replicateAcks = replicateAcks - id
      persistAcks = persistAcks - id
      receiver ! OperationAck(id)
