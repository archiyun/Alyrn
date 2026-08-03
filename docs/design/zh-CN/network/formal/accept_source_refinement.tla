--------------------------- MODULE accept_source_refinement ---------------------------

EXTENDS Naturals, Sequences, FiniteSets

(***************************************************************************)
(* AcceptSource 的有界 backend-refinement 模型。                         *)
(*                                                                         *)
(* 抽象层只有一个 source 协议：                                           *)
(*   Idle -> Active -> Pausing -> Paused -> Active                       *)
(*                  \-> Stopping -> Draining -> Terminal                 *)
(*                                                                         *)
(* Concrete backend 保留各自的物理路径：                                   *)
(*   Reactor       readiness -> accept drain                              *)
(*   UringSingle   one SQE -> one terminal CQE -> re-arm                  *)
(*   UringMulti    one request -> F_MORE CQE* -> terminal CQE             *)
(*                                                                         *)
(* 本模型检查的是业务可观察协议，而不是 Linux socket 或 io_uring ABI。   *)
(* MaxEvents/MaxRequests 是有界检查参数，不代表 source 的真实无界运行长度。 *)
(***************************************************************************)

CONSTANT MaxEvents, MaxRequests

Backends      == {"Reactor", "UringSingle", "UringMulti"}
SourceStates  == {"Idle", "Active", "Pausing", "Paused", "Stopping", "Draining", "Terminal"}
RequestStates == {"Idle", "Armed"}
ReactorStates == {"Idle", "Armed", "Ready"}
UringStates   == {"Idle", "Submitted"}
CancelStates  == {"Idle", "Submitted"}
EventIds      == 1..MaxEvents

VARIABLES backend,
          sourceState,
          requestState,
          reactorState,
          uringState,
          cancelState,
          queue,
          producedEvents,
          deliveredEvents,
          nextEventId,
          submitCount,
          terminalRequestCount,
          logicalTerminalCount,
          terminalObserved,
          stopRequested,
          resumeCount

vars == <<backend,
          sourceState,
          requestState,
          reactorState,
          uringState,
          cancelState,
          queue,
          producedEvents,
          deliveredEvents,
          nextEventId,
          submitCount,
          terminalRequestCount,
          logicalTerminalCount,
          terminalObserved,
          stopRequested,
          resumeCount>>

QueueSet == {queue[i] : i \in 1..Len(queue)}

(* A terminal completion of the current physical request moves a stopping
 * source directly to Draining or Terminal. An active source remains active
 * and may arm a new request. *)
StateAfterRequestTerminal(newQueue) ==
  IF sourceState = "Pausing"
  THEN "Paused"
  ELSE IF sourceState = "Stopping"
  THEN IF Len(newQueue) = 0 THEN "Terminal" ELSE "Draining"
  ELSE sourceState

StateAfterBackendFailure ==
  IF Len(queue) = 0 THEN "Terminal" ELSE "Draining"

AppendedQueue == Append(queue, nextEventId)

Init ==
  /\ MaxEvents \in Nat
  /\ MaxEvents > 0
  /\ MaxRequests \in Nat
  /\ MaxRequests > 0
  /\ backend \in Backends
  /\ sourceState = "Idle"
  /\ requestState = "Idle"
  /\ reactorState = "Idle"
  /\ uringState = "Idle"
  /\ cancelState = "Idle"
  /\ queue = <<>>
  /\ producedEvents = {}
  /\ deliveredEvents = {}
  /\ nextEventId = 1
  /\ submitCount = 0
  /\ terminalRequestCount = 0
  /\ logicalTerminalCount = 0
  /\ terminalObserved = FALSE
  /\ stopRequested = FALSE
  /\ resumeCount = 0

Start ==
  /\ sourceState = "Idle"
  /\ sourceState' = "Active"
  /\ UNCHANGED <<backend,
                 requestState,
                 reactorState,
                 uringState,
                 cancelState,
                 queue,
                 producedEvents,
                 deliveredEvents,
                 nextEventId,
                 submitCount,
                 terminalRequestCount,
                 logicalTerminalCount,
                 terminalObserved,
                 stopRequested,
                 resumeCount>>

(* All three backends admit one physical request at a time in this model.
 * This is enough to compare source semantics; Reactor's readiness callback
 * and io_uring's SQE/CQE path are represented by different concrete actions. *)
Arm ==
  /\ sourceState = "Active"
  /\ requestState = "Idle"
  /\ Len(queue) < MaxEvents
  /\ nextEventId <= MaxEvents
  /\ submitCount < MaxRequests
  /\ requestState' = "Armed"
  /\ submitCount' = submitCount + 1
  /\ reactorState' = IF backend = "Reactor" THEN "Armed" ELSE reactorState
  /\ uringState' = IF backend = "Reactor" THEN uringState ELSE "Submitted"
  /\ UNCHANGED <<backend,
                 sourceState,
                 cancelState,
                 queue,
                 producedEvents,
                 deliveredEvents,
                 nextEventId,
                 terminalRequestCount,
                 logicalTerminalCount,
                 terminalObserved,
                 stopRequested,
                 resumeCount>>

ReactorReady ==
  /\ backend = "Reactor"
  /\ sourceState \in {"Active", "Stopping"}
  /\ requestState = "Armed"
  /\ reactorState = "Armed"
  /\ reactorState' = "Ready"
  /\ UNCHANGED <<backend,
                 sourceState,
                 requestState,
                 uringState,
                 cancelState,
                 queue,
                 producedEvents,
                 deliveredEvents,
                 nextEventId,
                 submitCount,
                 terminalRequestCount,
                 logicalTerminalCount,
                 terminalObserved,
                 stopRequested,
                 resumeCount>>

ReactorAccept ==
  /\ backend = "Reactor"
  /\ sourceState \in {"Active", "Stopping"}
  /\ requestState = "Armed"
  /\ reactorState = "Ready"
  /\ Len(queue) < MaxEvents
  /\ nextEventId <= MaxEvents
  /\ requestState' = "Idle"
  /\ reactorState' = "Idle"
  /\ sourceState' = StateAfterRequestTerminal(AppendedQueue)
  /\ queue' = AppendedQueue
  /\ producedEvents' = producedEvents \cup {nextEventId}
  /\ nextEventId' = nextEventId + 1
  /\ terminalRequestCount' = terminalRequestCount + 1
  /\ UNCHANGED <<backend,
                 uringState,
                 cancelState,
                 deliveredEvents,
                 submitCount,
                 logicalTerminalCount,
                 terminalObserved,
                 stopRequested,
                 resumeCount>>

(* Readiness can fire without producing a connection. The source remains
 * active and may arm again; after Stop it drains the already queued events. *)
ReactorNoConnection ==
  /\ backend = "Reactor"
  /\ sourceState \in {"Active", "Stopping"}
  /\ requestState = "Armed"
  /\ reactorState = "Ready"
  /\ requestState' = "Idle"
  /\ reactorState' = "Idle"
  /\ sourceState' = StateAfterRequestTerminal(queue)
  /\ terminalRequestCount' = terminalRequestCount + 1
  /\ UNCHANGED <<backend,
                 uringState,
                 cancelState,
                 queue,
                 producedEvents,
                 deliveredEvents,
                 nextEventId,
                 submitCount,
                 logicalTerminalCount,
                 terminalObserved,
                 stopRequested,
                 resumeCount>>

UringSingleComplete ==
  /\ backend = "UringSingle"
  /\ sourceState \in {"Active", "Pausing", "Stopping"}
  /\ requestState = "Armed"
  /\ uringState = "Submitted"
  /\ requestState' = "Idle"
  /\ uringState' = "Idle"
  /\ sourceState' = StateAfterRequestTerminal(queue)
  /\ terminalRequestCount' = terminalRequestCount + 1
  /\ UNCHANGED <<backend,
                 reactorState,
                 cancelState,
                 queue,
                 producedEvents,
                 deliveredEvents,
                 nextEventId,
                 submitCount,
                 logicalTerminalCount,
                 terminalObserved,
                 stopRequested,
                 resumeCount>>

UringSingleCompleteEvent ==
  /\ backend = "UringSingle"
  /\ sourceState \in {"Active", "Pausing", "Stopping"}
  /\ requestState = "Armed"
  /\ uringState = "Submitted"
  /\ Len(queue) < MaxEvents
  /\ nextEventId <= MaxEvents
  /\ requestState' = "Idle"
  /\ uringState' = "Idle"
  /\ sourceState' = StateAfterRequestTerminal(AppendedQueue)
  /\ queue' = AppendedQueue
  /\ producedEvents' = producedEvents \cup {nextEventId}
  /\ nextEventId' = nextEventId + 1
  /\ terminalRequestCount' = terminalRequestCount + 1
  /\ UNCHANGED <<backend,
                 reactorState,
                 cancelState,
                 deliveredEvents,
                 submitCount,
                 logicalTerminalCount,
                 terminalObserved,
                 stopRequested,
                 resumeCount>>

(* F_MORE CQE: a logical event is produced while the native request remains
 * submitted. *)
UringMultiMore ==
  /\ backend = "UringMulti"
  /\ sourceState \in {"Active", "Pausing", "Stopping"}
  /\ requestState = "Armed"
  /\ uringState = "Submitted"
  /\ Len(queue) < MaxEvents
  /\ nextEventId <= MaxEvents
  /\ queue' = Append(queue, nextEventId)
  /\ producedEvents' = producedEvents \cup {nextEventId}
  /\ nextEventId' = nextEventId + 1
  /\ UNCHANGED <<backend,
                 sourceState,
                 requestState,
                 reactorState,
                 uringState,
                 cancelState,
                 deliveredEvents,
                 submitCount,
                 terminalRequestCount,
                 logicalTerminalCount,
                 terminalObserved,
                 stopRequested,
                 resumeCount>>

UringMultiTerminate ==
  /\ backend = "UringMulti"
  /\ sourceState \in {"Active", "Pausing", "Stopping"}
  /\ requestState = "Armed"
  /\ uringState = "Submitted"
  /\ requestState' = "Idle"
  /\ uringState' = "Idle"
  /\ sourceState' = StateAfterRequestTerminal(queue)
  /\ terminalRequestCount' = terminalRequestCount + 1
  /\ UNCHANGED <<backend,
                 reactorState,
                 cancelState,
                 queue,
                 producedEvents,
                 deliveredEvents,
                 nextEventId,
                 submitCount,
                 logicalTerminalCount,
                 terminalObserved,
                 stopRequested,
                 resumeCount>>

(* A terminal multishot CQE may itself carry one final accepted connection. *)
UringMultiTerminateEvent ==
  /\ backend = "UringMulti"
  /\ sourceState \in {"Active", "Pausing", "Stopping"}
  /\ requestState = "Armed"
  /\ uringState = "Submitted"
  /\ Len(queue) < MaxEvents
  /\ nextEventId <= MaxEvents
  /\ requestState' = "Idle"
  /\ uringState' = "Idle"
  /\ sourceState' = StateAfterRequestTerminal(AppendedQueue)
  /\ queue' = AppendedQueue
  /\ producedEvents' = producedEvents \cup {nextEventId}
  /\ nextEventId' = nextEventId + 1
  /\ terminalRequestCount' = terminalRequestCount + 1
  /\ UNCHANGED <<backend,
                 reactorState,
                 cancelState,
                 deliveredEvents,
                 submitCount,
                 logicalTerminalCount,
                 terminalObserved,
                 stopRequested,
                 resumeCount>>

(* A terminal error/EOF can end a source without an explicit Stop call. The
 * implementation converges this path through the same draining states. *)
BackendFailure ==
  /\ backend \in Backends
  /\ sourceState \in {"Active", "Stopping"}
  /\ requestState = "Armed"
  /\ requestState' = "Idle"
  /\ reactorState' = "Idle"
  /\ uringState' = "Idle"
  /\ sourceState' = StateAfterBackendFailure
  /\ stopRequested' = TRUE
  /\ terminalRequestCount' = terminalRequestCount + 1
  /\ UNCHANGED <<backend,
                 cancelState,
                 queue,
                 producedEvents,
                 deliveredEvents,
                 nextEventId,
                 submitCount,
                 logicalTerminalCount,
                 terminalObserved,
                 resumeCount>>

(* High-water is an admission boundary, not a logical terminal.  Native
 * io_uring paths cancel the current request and wait for its terminal CQE;
 * Reactor removes readiness interest and releases the modeled request
 * immediately. *)
RequestPause ==
  /\ sourceState = "Active"
  /\ stopRequested = FALSE
  /\ Len(queue) >= MaxEvents
  /\ sourceState' =
       IF requestState = "Armed" THEN "Pausing" ELSE "Paused"
  /\ cancelState' =
       IF backend # "Reactor" /\ requestState = "Armed"
       THEN "Submitted"
       ELSE cancelState
  /\ UNCHANGED <<backend,
                 requestState,
                 reactorState,
                 uringState,
                 queue,
                 producedEvents,
                 deliveredEvents,
                 nextEventId,
                 submitCount,
                 terminalRequestCount,
                 logicalTerminalCount,
                 terminalObserved,
                 stopRequested,
                 resumeCount>>

(* Reactor has no target CQE. Removing readable interest releases the one
 * modeled readiness request and reaches the same abstract Paused state. *)
ReactorPauseRelease ==
  /\ backend = "Reactor"
  /\ sourceState = "Pausing"
  /\ requestState = "Armed"
  /\ requestState' = "Idle"
  /\ reactorState' = "Idle"
  /\ sourceState' = "Paused"
  /\ terminalRequestCount' = terminalRequestCount + 1
  /\ UNCHANGED <<backend,
                 uringState,
                 cancelState,
                 queue,
                 producedEvents,
                 deliveredEvents,
                 nextEventId,
                 submitCount,
                 logicalTerminalCount,
                 terminalObserved,
                 stopRequested,
                 resumeCount>>

(* Low-water makes future admission possible again. Arming remains a
 * separate action so all backends keep their native submission mechanism. *)
ResumeAdmission ==
  /\ sourceState = "Paused"
  /\ requestState = "Idle"
  /\ cancelState = "Idle"
  /\ Len(queue) <= MaxEvents \div 2
  /\ sourceState' = "Active"
  /\ UNCHANGED <<backend,
                 requestState,
                 reactorState,
                 uringState,
                 cancelState,
                 queue,
                 producedEvents,
                 deliveredEvents,
                 nextEventId,
                 submitCount,
                 terminalRequestCount,
                 logicalTerminalCount,
                 terminalObserved,
                 stopRequested,
                 resumeCount>>

(* Stop never directly destroys an armed request. For io_uring it starts a
 * separate cancel request; the target request still has to become terminal. *)
RequestStop ==
  /\ sourceState \in {"Active", "Pausing", "Paused"}
  /\ stopRequested = FALSE
  /\ stopRequested' = TRUE
  /\ sourceState' =
       IF requestState = "Armed"
       THEN "Stopping"
       ELSE IF Len(queue) = 0 THEN "Terminal" ELSE "Draining"
  /\ cancelState' =
       IF backend # "Reactor" /\ requestState = "Armed" /\ cancelState = "Idle"
       THEN "Submitted"
       ELSE cancelState
  /\ UNCHANGED <<backend,
                 requestState,
                 reactorState,
                 uringState,
                 queue,
                 producedEvents,
                 deliveredEvents,
                 nextEventId,
                 submitCount,
                 terminalRequestCount,
                 logicalTerminalCount,
                 terminalObserved,
                 resumeCount>>

CancelComplete ==
  /\ backend # "Reactor"
  /\ cancelState = "Submitted"
  /\ cancelState' = "Idle"
  /\ UNCHANGED <<backend,
                 sourceState,
                 requestState,
                 reactorState,
                 uringState,
                 queue,
                 producedEvents,
                 deliveredEvents,
                 nextEventId,
                 submitCount,
                 terminalRequestCount,
                 logicalTerminalCount,
                 terminalObserved,
                 stopRequested,
                 resumeCount>>

DeliverEvent ==
  /\ queue # <<>>
  /\ sourceState \in {"Active", "Pausing", "Paused", "Stopping", "Draining"}
  /\ Head(queue) \notin deliveredEvents
  /\ queue' = Tail(queue)
  /\ deliveredEvents' = deliveredEvents \cup {Head(queue)}
  /\ resumeCount' = resumeCount + 1
  /\ sourceState' =
       IF sourceState = "Draining" /\ Len(Tail(queue)) = 0
       THEN "Terminal"
       ELSE sourceState
  /\ UNCHANGED <<backend,
                 requestState,
                 reactorState,
                 uringState,
                 cancelState,
                 producedEvents,
                 nextEventId,
                 submitCount,
                 terminalRequestCount,
                 logicalTerminalCount,
                 terminalObserved,
                 stopRequested>>

ObserveTerminal ==
  /\ sourceState = "Terminal"
  /\ requestState = "Idle"
  /\ queue = <<>>
  /\ terminalObserved = FALSE
  /\ terminalObserved' = TRUE
  /\ logicalTerminalCount' = logicalTerminalCount + 1
  /\ resumeCount' = resumeCount + 1
  /\ UNCHANGED <<backend,
                 sourceState,
                 requestState,
                 reactorState,
                 uringState,
                 cancelState,
                 queue,
                 producedEvents,
                 deliveredEvents,
                 nextEventId,
                 submitCount,
                 terminalRequestCount,
                 stopRequested>>

Next ==
  \/ Start
  \/ Arm
  \/ ReactorReady
  \/ ReactorAccept
  \/ ReactorNoConnection
  \/ UringSingleComplete
  \/ UringSingleCompleteEvent
  \/ UringMultiMore
  \/ UringMultiTerminate
  \/ UringMultiTerminateEvent
  \/ BackendFailure
  \/ RequestPause
  \/ ReactorPauseRelease
  \/ ResumeAdmission
  \/ RequestStop
  \/ CancelComplete
  \/ DeliverEvent
  \/ ObserveTerminal

Spec == Init /\ [][Next]_vars

TypeOK ==
  /\ backend \in Backends
  /\ sourceState \in SourceStates
  /\ requestState \in RequestStates
  /\ reactorState \in ReactorStates
  /\ uringState \in UringStates
  /\ cancelState \in CancelStates
  /\ queue \in Seq(EventIds)
  /\ producedEvents \subseteq EventIds
  /\ deliveredEvents \subseteq EventIds
  /\ nextEventId \in 1..(MaxEvents + 1)
  /\ submitCount \in Nat
  /\ terminalRequestCount \in Nat
  /\ logicalTerminalCount \in 0..1
  /\ terminalObserved \in BOOLEAN
  /\ stopRequested \in BOOLEAN
  /\ resumeCount \in Nat

(* A queue entry is neither duplicated nor simultaneously delivered. *)
QueueOwnership ==
  /\ Len(queue) = Cardinality(QueueSet)
  /\ QueueSet \cap deliveredEvents = {}
  /\ QueueSet \subseteq producedEvents
  /\ deliveredEvents \subseteq producedEvents

(* Every produced event is either queued or has been delivered. *)
ProducedPartition ==
  /\ producedEvents = QueueSet \cup deliveredEvents
  /\ QueueSet \cap deliveredEvents = {}

(* The only operation that increments resumeCount is delivery of one queued
 * event or observation of the sticky terminal result. *)
ExactlyOnceResume ==
  resumeCount = Cardinality(deliveredEvents) + logicalTerminalCount

(* A physical request has exactly one terminal CQE at most. Re-arming creates
 * another request, so the aggregate terminal count may exceed one. *)
RequestAccounting ==
  /\ terminalRequestCount <= submitCount
  /\ submitCount = terminalRequestCount +
       IF requestState = "Armed" THEN 1 ELSE 0

SubmitBound == submitCount <= MaxRequests

(* Stop is an admission boundary: no action can arm a new request once the
 * source has entered Stopping, Draining, or Terminal. *)
StopAdmission ==
  sourceState \in {"Stopping", "Draining", "Terminal"}
    => stopRequested

(* Paused is recoverable: the target request has converged, no logical
 * terminal has been observed, and a later low-water transition may re-arm
 * after the separate cancel CQE has also arrived. *)
PauseSafety ==
  sourceState = "Paused"
    => /\ requestState = "Idle"
       /\ stopRequested = FALSE
       /\ logicalTerminalCount = 0

(* Concrete backend state is not visible in the abstract source protocol. *)
BackendShape ==
  /\ backend = "Reactor"
       => /\ uringState = "Idle"
          /\ cancelState = "Idle"
          /\ (requestState = "Idle" => reactorState = "Idle")
  /\ backend # "Reactor"
       => /\ reactorState = "Idle"
          /\ (requestState = "Idle" => uringState = "Idle")
  /\ requestState = "Armed"
       => sourceState \in {"Active", "Pausing", "Stopping"}

TerminalSafety ==
  sourceState = "Terminal"
    => /\ requestState = "Idle"
       /\ queue = <<>>
       /\ logicalTerminalCount <= 1

TerminalObservationOnce ==
  /\ terminalObserved => logicalTerminalCount = 1
  /\ logicalTerminalCount = 1 => terminalObserved

=============================================================================
