---------------------------- MODULE loop_stop_control ----------------------------

EXTENDS Naturals

(***************************************************************************
(* Dispatcher stop control, deliberately separate from resource shutdown.  *)
(*                                                                         *)
(* This model captures the contract shared by Loop and Loop:    *)
(*                                                                         *)
(*   RequestStop is idempotent and changes dispatcher admission only.      *)
(*   It may stop dispatch before application resources have been closed.   *)
(*   Close/cancel drain, not LoopState = Stopped, authorizes release.      *)
***************************************************************************)

Operations == {"Read", "Write"}

LoopStates      == {"Created", "Running", "Stopping", "Stopped"}
ResourceStates  == {"Open", "Closing", "Closed"}
OperationStates == {"Absent", "Pending", "Terminal"}

VARIABLES loopState,
          resourceState,
          operationState,
          stopRequested,
          submitCount,
          completionCount

vars == <<loopState,
          resourceState,
          operationState,
          stopRequested,
          submitCount,
          completionCount>>

AllTerminal == \A op \in Operations: operationState[op] \in {"Absent", "Terminal"}

Init ==
  /\ loopState = "Created"
  /\ resourceState = "Open"
  /\ operationState = [op \in Operations |-> "Absent"]
  /\ stopRequested = FALSE
  /\ submitCount = [op \in Operations |-> 0]
  /\ completionCount = [op \in Operations |-> 0]

Run ==
  /\ loopState = "Created"
  /\ loopState' = "Running"
  /\ UNCHANGED <<resourceState,
                 operationState,
                 stopRequested,
                 submitCount,
                 completionCount>>

(* A normal I/O submission is no longer admitted once stopping starts. *)
Submit(op) ==
  /\ op \in Operations
  /\ loopState \in {"Created", "Running"}
  /\ resourceState = "Open"
  /\ operationState[op] = "Absent"
  /\ operationState' = [operationState EXCEPT ![op] = "Pending"]
  /\ submitCount' = [submitCount EXCEPT ![op] = @ + 1]
  /\ UNCHANGED <<loopState,
                 resourceState,
                 stopRequested,
                 completionCount>>

(* The request can come before Run or from a foreign thread. It never settles
   an operation and never releases a resource. Repeated requests preserve the
   same logical state, retaining idempotence without an unbounded counter. *)
RequestStop ==
  /\ loopState \in {"Created", "Running", "Stopping"}
  /\ loopState' = IF loopState \in {"Created", "Running"}
                  THEN "Stopping"
                  ELSE "Stopping"
  /\ stopRequested' = TRUE
  /\ UNCHANGED <<resourceState,
                 operationState,
                 submitCount,
                 completionCount>>

(* Resource close is a distinct owner-loop action. It may start only after
   the dispatcher has entered Stopping, while it is still able to run drain
   work and backend completions. *)
BeginClose ==
  /\ loopState = "Stopping"
  /\ resourceState = "Open"
  /\ resourceState' = "Closing"
  /\ UNCHANGED <<loopState,
                 operationState,
                 stopRequested,
                 submitCount,
                 completionCount>>

(* A readiness/CQE/cancel convergence settles one physical operation. *)
Complete(op) ==
  /\ op \in Operations
  /\ resourceState \in {"Open", "Closing"}
  /\ operationState[op] = "Pending"
  /\ operationState' = [operationState EXCEPT ![op] = "Terminal"]
  /\ completionCount' = [completionCount EXCEPT ![op] = @ + 1]
  /\ UNCHANGED <<loopState,
                 resourceState,
                 stopRequested,
                 submitCount>>

(* Only an explicit close drain may release the resource. *)
ReleaseResource ==
  /\ resourceState = "Closing"
  /\ AllTerminal
  /\ resourceState' = "Closed"
  /\ UNCHANGED <<loopState,
                 operationState,
                 stopRequested,
                 submitCount,
                 completionCount>>

(* The dispatcher reaches Stopped only after all physical operations settle.
   Its resource may remain Open: descriptor/object destruction is a separate
   owner action and is not implied by a drained dispatcher. *)
StopDispatcher ==
  /\ loopState = "Stopping"
  /\ AllTerminal
  /\ loopState' = "Stopped"
  /\ UNCHANGED <<resourceState,
                 operationState,
                 stopRequested,
                 submitCount,
                 completionCount>>

Next ==
  \/ Run
  \/ \E op \in Operations: Submit(op)
  \/ RequestStop
  \/ BeginClose
  \/ \E op \in Operations: Complete(op)
  \/ ReleaseResource
  \/ StopDispatcher

Spec == Init /\ [][Next]_vars

TypeOK ==
  /\ loopState \in LoopStates
  /\ resourceState \in ResourceStates
  /\ operationState \in [Operations -> OperationStates]
  /\ stopRequested \in BOOLEAN
  /\ submitCount \in [Operations -> Nat]
  /\ completionCount \in [Operations -> Nat]

SingleSubmissionPerOperation ==
  \A op \in Operations: submitCount[op] <= 1

OneCompletionPerOperation ==
  \A op \in Operations: completionCount[op] <= 1

ReleaseRequiresDrain ==
  resourceState = "Closed" => AllTerminal

StoppedDrainsPhysicalOperations ==
  loopState = "Stopped" => AllTerminal

=============================================================================
