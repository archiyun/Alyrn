---------------------- MODULE recv_source_incremental_lease ----------------------

EXTENDS Naturals, Sequences, FiniteSets

(***************************************************************************)
(* F_BUF_MORE incremental provided-buffer recv 的有界资源模型。          *)
(*                                                                         *)
(* 与 recv_source_lease.tla 不同，一个 selected buffer 可以依次产生多     *)
(* 个 segment。每个 segment 是独立 BufferLease，但 buffer 只能在：       *)
(*                                                                         *)
(*   1. 已观察到该 buffer 的最后一个 segment；且                         *)
(*   2. 所有 queued / consumer-held segment lease 都已释放              *)
(*                                                                         *)
(* 后归还 provided-buffer ring。模型只描述一个 multishot request 的       *)
(* 生命周期；request 终态与每个 buffer 的 final boundary 相互独立。      *)
(***************************************************************************)

CONSTANT BufferCapacity, BufferSize, EventCapacity, MaxSegments

SourceStates  == {"Idle", "Active", "Stopping", "Draining", "Terminal"}
RequestStates == {"Idle", "Armed"}
BufferIds     == 1..BufferCapacity
Lengths       == 1..BufferSize
NoBuffer      == 0

Segment(buffer, start, length) ==
  [buffer |-> buffer, start |-> start, length |-> length]

VARIABLES sourceState,
          requestState,
          available,
          activeBuffer,
          nextOffset,
          finalSeen,
          queue,
          leased,
          producedCount,
          deliveredCount,
          terminalRequestCount,
          resumeCount

vars == <<sourceState,
          requestState,
          available,
          activeBuffer,
          nextOffset,
          finalSeen,
          queue,
          leased,
          producedCount,
          deliveredCount,
          terminalRequestCount,
          resumeCount>>

QueueSet == {queue[i] : i \in 1..Len(queue)}
Outstanding == QueueSet \cup leased
OutstandingFor(buffer) == {segment \in Outstanding : segment.buffer = buffer}
ReturnEligible(buffer) == buffer \in finalSeen /\ OutstandingFor(buffer) = {}

AfterTerminal ==
  IF Outstanding = {} THEN "Terminal" ELSE "Draining"

AfterRelease ==
  IF requestState = "Idle" /\ Outstanding = {}
  THEN "Terminal"
  ELSE sourceState

Init ==
  /\ BufferCapacity \in Nat
  /\ BufferCapacity > 0
  /\ BufferSize \in Nat
  /\ BufferSize > 0
  /\ EventCapacity \in Nat
  /\ EventCapacity > 0
  /\ EventCapacity <= MaxSegments
  /\ MaxSegments \in Nat
  /\ MaxSegments > 0
  /\ sourceState = "Idle"
  /\ requestState = "Idle"
  /\ available = BufferIds
  /\ activeBuffer = NoBuffer
  /\ nextOffset = [buffer \in BufferIds |-> 0]
  /\ finalSeen = {}
  /\ queue = <<>>
  /\ leased = {}
  /\ producedCount = 0
  /\ deliveredCount = 0
  /\ terminalRequestCount = 0
  /\ resumeCount = 0

Start ==
  /\ sourceState = "Idle"
  /\ sourceState' = "Active"
  /\ requestState' = "Armed"
  /\ UNCHANGED <<available, activeBuffer, nextOffset, finalSeen, queue, leased,
                 producedCount, deliveredCount, terminalRequestCount, resumeCount>>

(***************************************************************************)
(* A CQE with F_BUF_MORE is represented by bufferMore = TRUE. It may only  *)
(* continue the one active incremental buffer. A final segment clears      *)
(* activeBuffer but cannot return its buffer while any segment remains.    *)
(***************************************************************************)
KernelSegment(buffer, length, bufferMore) ==
  /\ sourceState \in {"Active", "Stopping"}
  /\ requestState = "Armed"
  /\ length \in Lengths
  /\ producedCount < MaxSegments
  /\ Len(queue) < EventCapacity
  /\ (activeBuffer = NoBuffer /\ buffer \in available) \/ buffer = activeBuffer
  /\ nextOffset[buffer] + length <= BufferSize
  /\ LET segment == Segment(buffer, nextOffset[buffer], length) IN
       /\ segment \notin Outstanding
       /\ available' = available \ {buffer}
       /\ activeBuffer' = IF bufferMore THEN buffer ELSE NoBuffer
       /\ nextOffset' = [nextOffset EXCEPT ![buffer] = @ + length]
       /\ finalSeen' = IF bufferMore THEN finalSeen ELSE finalSeen \cup {buffer}
       /\ queue' = Append(queue, segment)
  /\ producedCount' = producedCount + 1
  /\ UNCHANGED <<sourceState, requestState, leased, deliveredCount,
                 terminalRequestCount, resumeCount>>

(* EOF, error, or cancellation terminates the physical multishot request. *)
KernelTerminal ==
  /\ sourceState \in {"Active", "Stopping"}
  /\ requestState = "Armed"
  /\ activeBuffer = NoBuffer
  /\ requestState' = "Idle"
  /\ sourceState' = AfterTerminal
  /\ terminalRequestCount' = terminalRequestCount + 1
  /\ UNCHANGED <<available, activeBuffer, nextOffset, finalSeen, queue, leased,
                 producedCount, deliveredCount, resumeCount>>

(* A terminal CQE can close a buffer without producing another segment. *)
FinalizeIncrementalBuffer ==
  /\ activeBuffer \in BufferIds
  /\ activeBuffer' = NoBuffer
  /\ finalSeen' = finalSeen \cup {activeBuffer}
  /\ UNCHANGED <<sourceState, requestState, available, nextOffset, queue, leased,
                 producedCount, deliveredCount, terminalRequestCount, resumeCount>>

RequestStop ==
  /\ sourceState = "Active"
  /\ sourceState' = "Stopping"
  /\ UNCHANGED <<requestState, available, activeBuffer, nextOffset, finalSeen, queue, leased,
                 producedCount, deliveredCount, terminalRequestCount, resumeCount>>

AcquireEvent ==
  /\ queue # <<>>
  /\ Head(queue) \notin leased
  /\ queue' = Tail(queue)
  /\ leased' = leased \cup {Head(queue)}
  /\ deliveredCount' = deliveredCount + 1
  /\ resumeCount' = resumeCount + 1
  /\ UNCHANGED <<sourceState, requestState, available, activeBuffer, nextOffset, finalSeen,
                 producedCount, terminalRequestCount>>

(* The consumer releases one segment lease. The buffer is not yet reusable  *)
(* unless its final boundary was observed and this was its final lease.     *)
ReleaseLease ==
  \E segment \in leased:
    /\ leased' = leased \ {segment}
    /\ sourceState' = AfterRelease
    /\ UNCHANGED <<requestState, available, activeBuffer, nextOffset, finalSeen, queue,
                   producedCount, deliveredCount, terminalRequestCount, resumeCount>>

ReturnBuffer ==
  \E buffer \in BufferIds:
    /\ ReturnEligible(buffer)
    /\ available' = available \cup {buffer}
    /\ nextOffset' = [nextOffset EXCEPT ![buffer] = 0]
    /\ finalSeen' = finalSeen \ {buffer}
    /\ UNCHANGED <<sourceState, requestState, activeBuffer, queue, leased,
                   producedCount, deliveredCount, terminalRequestCount, resumeCount>>

Next ==
  \/ Start
  \/ \E buffer \in BufferIds, length \in Lengths, more \in BOOLEAN:
       KernelSegment(buffer, length, more)
  \/ KernelTerminal
  \/ FinalizeIncrementalBuffer
  \/ RequestStop
  \/ AcquireEvent
  \/ ReleaseLease
  \/ ReturnBuffer

Spec == Init /\ [][Next]_vars

TypeOK ==
  /\ sourceState \in SourceStates
  /\ requestState \in RequestStates
  /\ available \subseteq BufferIds
  /\ activeBuffer \in BufferIds \cup {NoBuffer}
  /\ nextOffset \in [BufferIds -> 0..BufferSize]
  /\ finalSeen \subseteq BufferIds
  /\ queue \in Seq([buffer: BufferIds, start: 0..(BufferSize - 1), length: Lengths])
  /\ leased \subseteq [buffer: BufferIds, start: 0..(BufferSize - 1), length: Lengths]
  /\ producedCount \in 0..MaxSegments
  /\ deliveredCount \in Nat
  /\ terminalRequestCount \in 0..1
  /\ resumeCount \in Nat

BufferOwnership ==
  /\ \A buffer \in available: OutstandingFor(buffer) = {} /\ buffer \notin finalSeen
  /\ activeBuffer # NoBuffer => activeBuffer \notin available /\ activeBuffer \notin finalSeen
  /\ \A buffer \in BufferIds:
       buffer \in finalSeen => activeBuffer # buffer

SegmentBounds ==
  \A segment \in Outstanding:
    /\ segment.buffer \in BufferIds
    /\ segment.start + segment.length <= BufferSize

NoOverlappingSegments ==
  \A first \in Outstanding:
    \A second \in Outstanding:
      /\ first # second
      /\ first.buffer = second.buffer
      => first.start + first.length <= second.start \/
         second.start + second.length <= first.start

ReturnAuthorization ==
  \A buffer \in available:
    /\ OutstandingFor(buffer) = {}
    /\ buffer \notin finalSeen

RequestAccounting ==
  /\ terminalRequestCount <= 1
  /\ requestState = "Armed" => terminalRequestCount = 0

ExactlyOnceDelivery == resumeCount = deliveredCount

TerminalSafety ==
  sourceState = "Terminal"
    => /\ requestState = "Idle"
       /\ Outstanding = {}

=======================================================================================
