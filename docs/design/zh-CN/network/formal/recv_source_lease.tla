--------------------------- MODULE recv_source_lease ---------------------------

EXTENDS Naturals, Sequences, FiniteSets

(***************************************************************************)
(* RecvSource + BufferLease 的有界资源模型。                              *)
(*                                                                         *)
(* 一个 provided-buffer multishot request 可以产生多个数据事件。          *)
(* queue 中的 buffer 仍然属于 source；AcquireEvent 后才进入 consumer     *)
(* 持有的 leased 集合；ReleaseLease 才能重新放回 available 集合。         *)
(*                                                                         *)
(* 本模型重点检查：                                                       *)
(*   - queued / leased / available 三者不重叠且覆盖整个 buffer pool；      *)
(*   - Stop 不会在 queue 或 lease 未收敛时完成；                           *)
(*   - 一个事件和一个 terminal result 只恢复一次；                       *)
(*   - cancel CQE 不代替目标 recv 的 terminal CQE。                       *)
(*                                                                         *)
(* MaxEvents 只限制本次有界检查中的数据事件数量。                        *)
(***************************************************************************)

CONSTANT BufferCapacity, EventCapacity, MaxEvents

SourceStates  == {"Idle", "Active", "Stopping", "Draining", "Terminal"}
RequestStates == {"Idle", "Armed"}
CancelStates  == {"Idle", "Submitted"}
BufferIds     == 1..BufferCapacity

VARIABLES sourceState,
          requestState,
          cancelState,
          available,
          queue,
          leased,
          eventCount,
          deliveredCount,
          resumeCount,
          terminalObserved,
          logicalTerminalCount,
          stopRequested,
          stopCompleted,
          submitCount,
          terminalRequestCount

vars == <<sourceState,
          requestState,
          cancelState,
          available,
          queue,
          leased,
          eventCount,
          deliveredCount,
          resumeCount,
          terminalObserved,
          logicalTerminalCount,
          stopRequested,
          stopCompleted,
          submitCount,
          terminalRequestCount>>

QueueSet == {queue[i] : i \in 1..Len(queue)}

StateAfterRequestTerminal ==
  IF Len(queue) = 0 /\ leased = {}
  THEN "Terminal"
  ELSE "Draining"

StateAfterRelease(b) ==
  IF sourceState = "Draining"
       /\ requestState = "Idle"
       /\ Len(queue) = 0
       /\ leased \ {b} = {}
  THEN "Terminal"
  ELSE sourceState

Init ==
  /\ BufferCapacity \in Nat
  /\ BufferCapacity > 0
  /\ EventCapacity \in Nat
  /\ EventCapacity > 0
  /\ EventCapacity <= BufferCapacity
  /\ MaxEvents \in Nat
  /\ MaxEvents > 0
  /\ sourceState = "Idle"
  /\ requestState = "Idle"
  /\ cancelState = "Idle"
  /\ available = BufferIds
  /\ queue = <<>>
  /\ leased = {}
  /\ eventCount = 0
  /\ deliveredCount = 0
  /\ resumeCount = 0
  /\ terminalObserved = FALSE
  /\ logicalTerminalCount = 0
  /\ stopRequested = FALSE
  /\ stopCompleted = FALSE
  /\ submitCount = 0
  /\ terminalRequestCount = 0

Start ==
  /\ sourceState = "Idle"
  /\ sourceState' = "Active"
  /\ UNCHANGED <<requestState,
                 cancelState,
                 available,
                 queue,
                 leased,
                 eventCount,
                 deliveredCount,
                 resumeCount,
                 terminalObserved,
                 logicalTerminalCount,
                 stopRequested,
                 stopCompleted,
                 submitCount,
                 terminalRequestCount>>

(* The native recv source has one armed multishot request. The request itself
 * consumes admission budget, but does not remove a concrete buffer from the
 * provided ring until a CQE selects that buffer. *)
Arm ==
  /\ sourceState = "Active"
  /\ requestState = "Idle"
  /\ eventCount < MaxEvents
  /\ Len(queue) < EventCapacity
  /\ Len(queue) + Cardinality(leased) + 1 <= BufferCapacity
  /\ requestState' = "Armed"
  /\ submitCount' = submitCount + 1
  /\ UNCHANGED <<sourceState,
                 cancelState,
                 available,
                 queue,
                 leased,
                 eventCount,
                 deliveredCount,
                 resumeCount,
                 terminalObserved,
                 logicalTerminalCount,
                 stopRequested,
                 stopCompleted,
                 terminalRequestCount>>

(* A positive F_MORE CQE selects one currently available provided buffer. *)
KernelEvent ==
  \E b \in available :
    /\ sourceState \in {"Active", "Stopping"}
    /\ requestState = "Armed"
    /\ Len(queue) < EventCapacity
    /\ Len(queue) + Cardinality(leased) < BufferCapacity
    /\ eventCount < MaxEvents
    /\ available' = available \ {b}
    /\ queue' = Append(queue, b)
    /\ eventCount' = eventCount + 1
    /\ UNCHANGED <<sourceState,
                   requestState,
                   cancelState,
                   leased,
                   deliveredCount,
                   resumeCount,
                   terminalObserved,
                   logicalTerminalCount,
                   stopRequested,
                   stopCompleted,
                   submitCount,
                   terminalRequestCount>>

(* The final CQE (EOF, error, or cancellation of the target recv) ends the
 * physical request. It does not release queued or consumer-held buffers. *)
KernelTerminal ==
  /\ sourceState \in {"Active", "Stopping"}
  /\ requestState = "Armed"
  /\ requestState' = "Idle"
  /\ sourceState' = StateAfterRequestTerminal
  /\ stopRequested' = TRUE
  /\ terminalRequestCount' = terminalRequestCount + 1
  /\ UNCHANGED <<cancelState,
                 available,
                 queue,
                 leased,
                 eventCount,
                 deliveredCount,
                 resumeCount,
                 terminalObserved,
                 logicalTerminalCount,
                 stopCompleted,
                 submitCount>>

(* Once admission is exhausted the implementation stops the backend request;
 * this is a logical Stop transition, not a buffer release. *)
BackpressureStop ==
  /\ sourceState = "Active"
  /\ stopRequested = FALSE
  /\ requestState = "Armed"
  /\ (Len(queue) >= EventCapacity \/
      Len(queue) + Cardinality(leased) >= BufferCapacity)
  /\ sourceState' = "Stopping"
  /\ stopRequested' = TRUE
  /\ cancelState' = "Submitted"
  /\ UNCHANGED <<requestState,
                 available,
                 queue,
                 leased,
                 eventCount,
                 deliveredCount,
                 resumeCount,
                 terminalObserved,
                 logicalTerminalCount,
                 stopCompleted,
                 submitCount,
                 terminalRequestCount>>

RequestStop ==
  /\ sourceState = "Active"
  /\ stopRequested = FALSE
  /\ sourceState' =
       IF requestState = "Armed"
       THEN "Stopping"
       ELSE IF Len(queue) = 0 /\ leased = {}
            THEN "Terminal"
            ELSE "Draining"
  /\ stopRequested' = TRUE
  /\ cancelState' =
       IF requestState = "Armed" THEN "Submitted" ELSE "Idle"
  /\ UNCHANGED <<requestState,
                 available,
                 queue,
                 leased,
                 eventCount,
                 deliveredCount,
                 resumeCount,
                 terminalObserved,
                 logicalTerminalCount,
                 stopCompleted,
                 submitCount,
                 terminalRequestCount>>

CancelComplete ==
  /\ cancelState = "Submitted"
  /\ cancelState' = "Idle"
  /\ UNCHANGED <<sourceState,
                 requestState,
                 available,
                 queue,
                 leased,
                 eventCount,
                 deliveredCount,
                 resumeCount,
                 terminalObserved,
                 logicalTerminalCount,
                 stopRequested,
                 stopCompleted,
                 submitCount,
                 terminalRequestCount>>

(* Moving an event to the consumer creates the BufferLease lifetime. *)
AcquireEvent ==
  /\ queue # <<>>
  /\ sourceState \in {"Active", "Stopping", "Draining"}
  /\ Head(queue) \notin leased
  /\ queue' = Tail(queue)
  /\ leased' = leased \cup {Head(queue)}
  /\ deliveredCount' = deliveredCount + 1
  /\ resumeCount' = resumeCount + 1
  /\ UNCHANGED <<sourceState,
                 requestState,
                 cancelState,
                 available,
                 eventCount,
                 terminalObserved,
                 logicalTerminalCount,
                 stopRequested,
                 stopCompleted,
                 submitCount,
                 terminalRequestCount>>

(* BufferLease destruction/release is the only transition from leased back
 * to available. This is the resource boundary that Stop must wait for. *)
ReleaseLease ==
  \E b \in leased :
    /\ leased' = leased \ {b}
    /\ available' = available \cup {b}
    /\ sourceState' = StateAfterRelease(b)
    /\ UNCHANGED <<requestState,
                   cancelState,
                   queue,
                   eventCount,
                   deliveredCount,
                   resumeCount,
                   terminalObserved,
                   logicalTerminalCount,
                   stopRequested,
                   stopCompleted,
                   submitCount,
                   terminalRequestCount>>

ObserveTerminal ==
  /\ sourceState = "Terminal"
  /\ requestState = "Idle"
  /\ queue = <<>>
  /\ terminalObserved = FALSE
  /\ terminalObserved' = TRUE
  /\ logicalTerminalCount' = logicalTerminalCount + 1
  /\ resumeCount' = resumeCount + 1
  /\ UNCHANGED <<sourceState,
                 requestState,
                 cancelState,
                 available,
                 queue,
                 leased,
                 eventCount,
                 deliveredCount,
                 stopRequested,
                 stopCompleted,
                 submitCount,
                 terminalRequestCount>>

CompleteStop ==
  /\ stopRequested = TRUE
  /\ stopCompleted = FALSE
  /\ sourceState = "Terminal"
  /\ requestState = "Idle"
  /\ cancelState = "Idle"
  /\ queue = <<>>
  /\ leased = {}
  /\ stopCompleted' = TRUE
  /\ UNCHANGED <<sourceState,
                 requestState,
                 cancelState,
                 available,
                 queue,
                 leased,
                 eventCount,
                 deliveredCount,
                 resumeCount,
                 terminalObserved,
                 logicalTerminalCount,
                 stopRequested,
                 submitCount,
                 terminalRequestCount>>

Next ==
  \/ Start
  \/ Arm
  \/ KernelEvent
  \/ KernelTerminal
  \/ BackpressureStop
  \/ RequestStop
  \/ CancelComplete
  \/ AcquireEvent
  \/ ReleaseLease
  \/ ObserveTerminal
  \/ CompleteStop

Spec == Init /\ [][Next]_vars

TypeOK ==
  /\ sourceState \in SourceStates
  /\ requestState \in RequestStates
  /\ cancelState \in CancelStates
  /\ available \subseteq BufferIds
  /\ queue \in Seq(BufferIds)
  /\ leased \subseteq BufferIds
  /\ eventCount \in 0..MaxEvents
  /\ deliveredCount \in Nat
  /\ resumeCount \in Nat
  /\ terminalObserved \in BOOLEAN
  /\ logicalTerminalCount \in 0..1
  /\ stopRequested \in BOOLEAN
  /\ stopCompleted \in BOOLEAN
  /\ submitCount \in Nat
  /\ terminalRequestCount \in Nat

BufferPartition ==
  /\ available \cup QueueSet \cup leased = BufferIds
  /\ available \cap QueueSet = {}
  /\ available \cap leased = {}
  /\ QueueSet \cap leased = {}

QueueOwnership ==
  /\ Len(queue) = Cardinality(QueueSet)
  /\ Len(queue) <= EventCapacity

OutstandingBound ==
  Len(queue) + Cardinality(leased) <= BufferCapacity

EventAccounting ==
  /\ eventCount = deliveredCount + Len(queue)
  /\ deliveredCount <= eventCount

ExactlyOnceResume ==
  resumeCount = deliveredCount + logicalTerminalCount

RequestAccounting ==
  /\ submitCount <= 1
  /\ terminalRequestCount <= submitCount
  /\ submitCount = terminalRequestCount +
       IF requestState = "Armed" THEN 1 ELSE 0

StopAdmission ==
  sourceState \in {"Stopping", "Draining", "Terminal"}
    => stopRequested

TerminalSafety ==
  sourceState = "Terminal"
    => /\ requestState = "Idle"
       /\ queue = <<>>
       /\ leased = {}

StopCompletionSafety ==
  stopCompleted
    => /\ sourceState = "Terminal"
       /\ requestState = "Idle"
       /\ cancelState = "Idle"
       /\ queue = <<>>
       /\ leased = {}

TerminalObservationOnce ==
  /\ terminalObserved => logicalTerminalCount = 1
  /\ logicalTerminalCount = 1 => terminalObserved

=============================================================================
