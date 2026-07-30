------------------------------ MODULE async_stream_multiop ------------------------------

EXTENDS Naturals

(***************************************************************************)
(* AsyncStream 的并发 operation 模型。                                   *)
(*                                                                         *)
(* async_stream_core.tla 保留一个 operation 的最小教学模型。本模型补足   *)
(* 实际 Stream 可以同时拥有一个 pending read 和一个 pending write 的       *)
(* 情况，并给每个 operation 明确绑定其 coroutine owner。                 *)
(*                                                                         *)
(* 它不描述 fd、CQE、buffer 或 scheduler 队列；这些属于 backend            *)
(* refinement。这里仅证明应用可观察的 operation/coroutine/close 协议。    *)
(***************************************************************************)

Operations == {"Read", "Write"}
Coroutines == {"Reader", "Writer"}
Owner(op) == IF op = "Read" THEN "Reader" ELSE "Writer"

ResourceStates == {"Open", "Closing", "Closed"}
OperationStates == {"None", "Pending", "Completed", "Cancelled"}
CoroutineStates == {"Running", "Waiting", "Ready"}
ResultStates == {"NoResult", "Success", "Error", "Cancelled"}

VARIABLES resourceState,
          operationState,
          coroutineState,
          result,
          submitCount,
          completionCount,
          resumeCount

vars == <<resourceState,
          operationState,
          coroutineState,
          result,
          submitCount,
          completionCount,
          resumeCount>>

AllSettled == \A op \in Operations: operationState[op] # "Pending"

Init ==
  /\ resourceState = "Open"
  /\ operationState = [op \in Operations |-> "None"]
  /\ coroutineState = [coroutine \in Coroutines |-> "Running"]
  /\ result = [op \in Operations |-> "NoResult"]
  /\ submitCount = [op \in Operations |-> 0]
  /\ completionCount = [op \in Operations |-> 0]
  /\ resumeCount = [op \in Operations |-> 0]

(* A real asynchronous submission suspends exactly the owning coroutine. *)
Submit(op) ==
  /\ op \in Operations
  /\ resourceState = "Open"
  /\ operationState[op] = "None"
  /\ coroutineState[Owner(op)] = "Running"
  /\ operationState' = [operationState EXCEPT ![op] = "Pending"]
  /\ coroutineState' = [coroutineState EXCEPT ![Owner(op)] = "Waiting"]
  /\ submitCount' = [submitCount EXCEPT ![op] = @ + 1]
  /\ UNCHANGED <<resourceState, result, completionCount, resumeCount>>

(* The nonblocking fast path completes without parking the coroutine. *)
ImmediateComplete(op) ==
  /\ op \in Operations
  /\ resourceState = "Open"
  /\ operationState[op] = "None"
  /\ coroutineState[Owner(op)] = "Running"
  /\ operationState' = [operationState EXCEPT ![op] = "Completed"]
  /\ result' = [result EXCEPT ![op] = "Success"]
  /\ submitCount' = [submitCount EXCEPT ![op] = @ + 1]
  /\ completionCount' = [completionCount EXCEPT ![op] = @ + 1]
  /\ UNCHANGED <<resourceState, coroutineState, resumeCount>>

(* The backend completes one specific pending physical request. *)
Complete(op) ==
  /\ op \in Operations
  /\ resourceState \in {"Open", "Closing"}
  /\ operationState[op] = "Pending"
  /\ completionCount[op] = 0
  /\ operationState' = [operationState EXCEPT ![op] = "Completed"]
  /\ result' \in {[result EXCEPT ![op] = "Success"],
                  [result EXCEPT ![op] = "Error"]}
  /\ completionCount' = [completionCount EXCEPT ![op] = @ + 1]
  /\ UNCHANGED <<resourceState, coroutineState, submitCount, resumeCount>>

(* Close drives each pending target operation to an observable cancellation. *)
Cancel(op) ==
  /\ op \in Operations
  /\ resourceState = "Closing"
  /\ operationState[op] = "Pending"
  /\ completionCount[op] = 0
  /\ operationState' = [operationState EXCEPT ![op] = "Cancelled"]
  /\ result' = [result EXCEPT ![op] = "Cancelled"]
  /\ completionCount' = [completionCount EXCEPT ![op] = @ + 1]
  /\ UNCHANGED <<resourceState, coroutineState, submitCount, resumeCount>>

(* Only the owner of the completed operation may become ready. *)
Resume(op) ==
  /\ op \in Operations
  /\ coroutineState[Owner(op)] = "Waiting"
  /\ operationState[op] \in {"Completed", "Cancelled"}
  /\ resumeCount[op] = 0
  /\ coroutineState' = [coroutineState EXCEPT ![Owner(op)] = "Ready"]
  /\ resumeCount' = [resumeCount EXCEPT ![op] = @ + 1]
  /\ UNCHANGED <<resourceState, operationState, result, submitCount, completionCount>>

(* Close admits no new operations. Pending requests must settle first. *)
Close ==
  /\ resourceState = "Open"
  /\ resourceState' = IF AllSettled THEN "Closed" ELSE "Closing"
  /\ UNCHANGED <<operationState,
                 coroutineState,
                 result,
                 submitCount,
                 completionCount,
                 resumeCount>>

FinalizeClose ==
  /\ resourceState = "Closing"
  /\ AllSettled
  /\ resourceState' = "Closed"
  /\ UNCHANGED <<operationState,
                 coroutineState,
                 result,
                 submitCount,
                 completionCount,
                 resumeCount>>

Next ==
  \/ \E op \in Operations: Submit(op)
  \/ \E op \in Operations: ImmediateComplete(op)
  \/ \E op \in Operations: Complete(op)
  \/ \E op \in Operations: Cancel(op)
  \/ \E op \in Operations: Resume(op)
  \/ Close
  \/ FinalizeClose

Spec == Init /\ [][Next]_vars

TypeOK ==
  /\ resourceState \in ResourceStates
  /\ operationState \in [Operations -> OperationStates]
  /\ coroutineState \in [Coroutines -> CoroutineStates]
  /\ result \in [Operations -> ResultStates]
  /\ submitCount \in [Operations -> Nat]
  /\ completionCount \in [Operations -> Nat]
  /\ resumeCount \in [Operations -> Nat]

SingleSubmission == \A op \in Operations: submitCount[op] <= 1
UniqueCompletion == \A op \in Operations: completionCount[op] <= 1
ExactlyOnceResume == \A op \in Operations: resumeCount[op] <= 1

(* A ready coroutine can only be made ready by its own settled operation. *)
ResumeAuthorization ==
  \A op \in Operations:
    coroutineState[Owner(op)] = "Ready"
      => /\ operationState[op] \in {"Completed", "Cancelled"}
         /\ resumeCount[op] = 1

(* Read and write cannot complete or resume each other's coroutine. *)
OperationOwnership ==
  \A op \in Operations:
    operationState[op] = "Pending" => coroutineState[Owner(op)] = "Waiting"

(* A closed resource has released every physical operation. *)
ClosedHasNoPending == resourceState = "Closed" => AllSettled

========================================================================================
