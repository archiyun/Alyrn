------------------------ MODULE luring_loop_stop_retry ------------------------

EXTENDS Naturals

(***************************************************************************)
(* io_uring loop stop / global-cancel retry model.                         *)
(*                                                                         *)
(* Loop::Run() has no error return.  Therefore a local failure while *)
(* preparing or flushing the loop-wide cancel SQE cannot publish Stopped   *)
(* while target requests are still live.  The owner loop remains Stopping, *)
(* reaps CQEs, and retries the failed drain step.                          *)
(*                                                                         *)
(* The global cancel request is a physical request of its own.  Its CQE    *)
(* only makes that request terminal; target requests still need their own  *)
(* terminal CQEs before the dispatcher can stop.                           *)
(***************************************************************************)

Operations == {"Read", "Write"}

LoopStates   == {"Running", "Stopping", "Stopped"}
TargetStates == {"Absent", "Pending", "Terminal"}
CancelStates == {"Idle", "Preparing", "PendingSubmit", "Submitted", "Terminal"}

VARIABLES loopState,
          targetState,
          cancelState,
          cancelFailuresRemaining,
          drainFlushFailuresRemaining,
          targetSubmitCount,
          targetCompletionCount,
          cancelPreparationAttempts,
          cancelPhysicalSubmitCount,
          cancelRequestTerminalCount

vars == <<loopState,
          targetState,
          cancelState,
          cancelFailuresRemaining,
          drainFlushFailuresRemaining,
          targetSubmitCount,
          targetCompletionCount,
          cancelPreparationAttempts,
          cancelPhysicalSubmitCount,
          cancelRequestTerminalCount>>

TargetsDrained ==
  \A op \in Operations: targetState[op] \in {"Absent", "Terminal"}

CancelDrained == cancelState \in {"Idle", "Terminal"}

Init ==
  /\ loopState = "Running"
  /\ targetState = [op \in Operations |-> "Absent"]
  /\ cancelState = "Idle"
  /\ cancelFailuresRemaining = 1
  /\ drainFlushFailuresRemaining = 1
  /\ targetSubmitCount = [op \in Operations |-> 0]
  /\ targetCompletionCount = [op \in Operations |-> 0]
  /\ cancelPreparationAttempts = 0
  /\ cancelPhysicalSubmitCount = 0
  /\ cancelRequestTerminalCount = 0

(* Normal work is admitted only while the dispatcher runs. *)
SubmitTarget(op) ==
  /\ op \in Operations
  /\ loopState = "Running"
  /\ targetState[op] = "Absent"
  /\ targetState' = [targetState EXCEPT ![op] = "Pending"]
  /\ targetSubmitCount' = [targetSubmitCount EXCEPT ![op] = @ + 1]
  /\ UNCHANGED <<loopState,
                 cancelState,
                 cancelFailuresRemaining,
                 drainFlushFailuresRemaining,
                 targetCompletionCount,
                 cancelPreparationAttempts,
                 cancelPhysicalSubmitCount,
                 cancelRequestTerminalCount>>

RequestStop ==
  /\ loopState \in {"Running", "Stopping"}
  /\ loopState' = "Stopping"
  /\ UNCHANGED <<targetState,
                 cancelState,
                 cancelFailuresRemaining,
                 drainFlushFailuresRemaining,
                 targetSubmitCount,
                 targetCompletionCount,
                 cancelPreparationAttempts,
                 cancelPhysicalSubmitCount,
                 cancelRequestTerminalCount>>

(* This is only local SQE preparation.  No kernel cancellation request has
   been submitted yet, and neither target nor loop state may become terminal. *)
PrepareGlobalCancel ==
  /\ loopState = "Stopping"
  /\ (\E op \in Operations: targetState[op] = "Pending")
  /\ cancelState = "Idle"
  /\ cancelState' = "Preparing"
  /\ cancelPreparationAttempts' = cancelPreparationAttempts + 1
  /\ UNCHANGED <<loopState,
                 targetState,
                 cancelFailuresRemaining,
                 drainFlushFailuresRemaining,
                 targetSubmitCount,
                 targetCompletionCount,
                 cancelPhysicalSubmitCount,
                 cancelRequestTerminalCount>>

(* A local allocation/SQ-full/submission preparation error.  The loop must
   remain Stopping; the next turn may prepare the same global cancel again. *)
GlobalCancelPreparationFails ==
  /\ cancelState = "Preparing"
  /\ cancelFailuresRemaining > 0
  /\ cancelState' = "Idle"
  /\ cancelFailuresRemaining' = cancelFailuresRemaining - 1
  /\ UNCHANGED <<loopState,
                 targetState,
                 targetSubmitCount,
                 targetCompletionCount,
                 drainFlushFailuresRemaining,
                 cancelPreparationAttempts,
                 cancelPhysicalSubmitCount,
                 cancelRequestTerminalCount>>

(* Preparation has produced an SQE, but it has not reached the kernel until
   the loop flushes its pending submissions. *)
QueueGlobalCancel ==
  /\ cancelState = "Preparing"
  /\ cancelFailuresRemaining = 0
  /\ cancelState' = "PendingSubmit"
  /\ UNCHANGED <<loopState,
                 targetState,
                 cancelFailuresRemaining,
                 drainFlushFailuresRemaining,
                 targetSubmitCount,
                 targetCompletionCount,
                 cancelPreparationAttempts,
                 cancelPhysicalSubmitCount,
                 cancelRequestTerminalCount>>

(* The SQE remains owned by the loop after a transient io_uring_submit/reap
   failure.  Retrying must not discard it or advance loopState to Stopped. *)
DrainFlushFails ==
  /\ cancelState = "PendingSubmit"
  /\ drainFlushFailuresRemaining > 0
  /\ drainFlushFailuresRemaining' = drainFlushFailuresRemaining - 1
  /\ UNCHANGED <<loopState,
                 targetState,
                 cancelState,
                 cancelFailuresRemaining,
                 targetSubmitCount,
                 targetCompletionCount,
                 cancelPreparationAttempts,
                 cancelPhysicalSubmitCount,
                 cancelRequestTerminalCount>>

(* Retrying the drain flush transfers the prepared global cancel request to
   the kernel exactly once. *)
SubmitGlobalCancel ==
  /\ cancelState = "PendingSubmit"
  /\ drainFlushFailuresRemaining = 0
  /\ cancelState' = "Submitted"
  /\ cancelPhysicalSubmitCount' = cancelPhysicalSubmitCount + 1
  /\ UNCHANGED <<loopState,
                 targetState,
                 cancelFailuresRemaining,
                 drainFlushFailuresRemaining,
                 targetSubmitCount,
                 targetCompletionCount,
                 cancelPreparationAttempts,
                 cancelRequestTerminalCount>>

(* A cancel CQE settles only the global cancel request.  It is not a target
   CQE and does not by itself authorize loop shutdown. *)
GlobalCancelRequestTerminal ==
  /\ cancelState = "Submitted"
  /\ cancelState' = "Terminal"
  /\ cancelRequestTerminalCount' = cancelRequestTerminalCount + 1
  /\ UNCHANGED <<loopState,
                 targetState,
                 cancelFailuresRemaining,
                 drainFlushFailuresRemaining,
                 targetSubmitCount,
                 targetCompletionCount,
                 cancelPreparationAttempts,
                 cancelPhysicalSubmitCount>>

(* Target CQEs remain distinct.  They may arrive before or after the cancel
   CQE, but only after the global cancel request entered the kernel. *)
TargetRequestTerminal(op) ==
  /\ op \in Operations
  /\ targetState[op] = "Pending"
  /\ cancelState \in {"Submitted", "Terminal"}
  /\ targetState' = [targetState EXCEPT ![op] = "Terminal"]
  /\ targetCompletionCount' = [targetCompletionCount EXCEPT ![op] = @ + 1]
  /\ UNCHANGED <<loopState,
                 cancelState,
                 cancelFailuresRemaining,
                 drainFlushFailuresRemaining,
                 targetSubmitCount,
                 cancelPreparationAttempts,
                 cancelPhysicalSubmitCount,
                 cancelRequestTerminalCount>>

(* Stopped is published only after every target and the cancel request itself
   have drained. *)
StopDispatcher ==
  /\ loopState = "Stopping"
  /\ TargetsDrained
  /\ CancelDrained
  /\ loopState' = "Stopped"
  /\ UNCHANGED <<targetState,
                 cancelState,
                 cancelFailuresRemaining,
                 drainFlushFailuresRemaining,
                 targetSubmitCount,
                 targetCompletionCount,
                 cancelPreparationAttempts,
                 cancelPhysicalSubmitCount,
                 cancelRequestTerminalCount>>

AnySubmitTarget == \E op \in Operations: SubmitTarget(op)
AnyTargetRequestTerminal == \E op \in Operations: TargetRequestTerminal(op)

Next ==
  \/ AnySubmitTarget
  \/ RequestStop
  \/ PrepareGlobalCancel
  \/ GlobalCancelPreparationFails
  \/ QueueGlobalCancel
  \/ DrainFlushFails
  \/ SubmitGlobalCancel
  \/ GlobalCancelRequestTerminal
  \/ AnyTargetRequestTerminal
  \/ StopDispatcher

(***************************************************************************)
(* Liveness assumes the owner loop keeps running after RequestStop and that *)
(* submitted cancel/target requests eventually receive CQEs.  It does not   *)
(* claim recovery from a permanently broken ring.                           *)
(***************************************************************************)
Fairness ==
  /\ WF_vars(PrepareGlobalCancel)
  /\ WF_vars(GlobalCancelPreparationFails)
  /\ WF_vars(QueueGlobalCancel)
  /\ WF_vars(DrainFlushFails)
  /\ WF_vars(SubmitGlobalCancel)
  /\ WF_vars(GlobalCancelRequestTerminal)
  /\ WF_vars(AnyTargetRequestTerminal)
  /\ WF_vars(StopDispatcher)

Spec == Init /\ [][Next]_vars /\ Fairness

TypeOK ==
  /\ loopState \in LoopStates
  /\ targetState \in [Operations -> TargetStates]
  /\ cancelState \in CancelStates
  /\ cancelFailuresRemaining \in 0..1
  /\ drainFlushFailuresRemaining \in 0..1
  /\ targetSubmitCount \in [Operations -> Nat]
  /\ targetCompletionCount \in [Operations -> Nat]
  /\ cancelPreparationAttempts \in Nat
  /\ cancelPhysicalSubmitCount \in Nat
  /\ cancelRequestTerminalCount \in Nat

OneTargetSubmission ==
  \A op \in Operations: targetSubmitCount[op] <= 1

OneTargetTerminalCqe ==
  \A op \in Operations: targetCompletionCount[op] <= 1

OneGlobalCancelRequest == cancelPhysicalSubmitCount <= 1

OneGlobalCancelTerminalCqe == cancelRequestTerminalCount <= 1

StoppedRequiresFullDrain ==
  loopState = "Stopped" => /\ TargetsDrained /\ CancelDrained

(* The injected local failure is recoverable: if work was pending, a stopped
   loop could only have reached Stopped after a later physical cancel submit. *)
FailedPreparationRequiresRetryBeforeStop ==
  /\ cancelFailuresRemaining = 0
  /\ cancelPreparationAttempts >= 2
  /\ loopState = "Stopped"
  => cancelPhysicalSubmitCount = 1

(* A local flush failure keeps the prepared SQE pending.  If there were target
   requests, a stopped loop must therefore have retried and submitted it. *)
FailedFlushRequiresRetryBeforeStop ==
  /\ drainFlushFailuresRemaining = 0
  /\ (\E op \in Operations: targetSubmitCount[op] = 1)
  /\ loopState = "Stopped"
  => cancelPhysicalSubmitCount = 1

StopEventuallyDrains ==
  (loopState = "Stopping") ~> (loopState = "Stopped")

=============================================================================
