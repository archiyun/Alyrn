---------------------- MODULE async_stream_backend_refinement ----------------------

EXTENDS Naturals

(***************************************************************************)
(* Reactor / io_uring 到 single-result AsyncStream specification 的有界  *)
(* trace refinement。                                                     *)
(*                                                                         *)
(* 证明范围：                                                             *)
(*   - 一个 stream；                                                       *)
(*   - 一个 single-result ReadSome；                                       *)
(*   - 正常、立即完成和 Close 取消；                                       *)
(*   - backend 在 Init 时固定，不允许动态切换。                           *)
(*                                                                         *)
(* ObsReactor / ObsLUring 是显式 observation function。具体 transition    *)
(* 投影后必须是 LogicalNext，或者是不改变 LogicalObservation 的          *)
(* stuttering step。                                                       *)
(*                                                                         *)
(* 立即完成经由 await_suspend() == false 回到当前协程；它不是一次       *)
(* Scheduler continuation resume。该路径由 InlineContinue 单独表示。   *)
(*                                                                        *)
(* 这是 C++ 路径的有限状态模型，不自动证明 C++ 内存安全或真实内核公平性。*)
(***************************************************************************)

Backends         == {"Reactor", "LUring"}
CoroutineStates  == {"Running", "Waiting", "Ready", "Done"}
ResourceStates   == {"Open", "Closing", "Closed"}
OperationStates  == {"None", "Pending", "Completed", "Cancelled"}
ResultStates     == {"NoResult", "Success", "EOF", "Error", "Cancelled"}
States    == {"Idle", "ChannelWaiting", "Ready"}
UringStates      == {"Idle", "SQEQueued", "Submitted", "CQEReady"}

VARIABLES backend,
          coroutineState,
          resourceState,
          operationState,
          result,
          completionCount,
          submitCount,
          releaseAuthorized,
          continuationAuthorized,
          reactorState,
          uringState

vars == <<backend,
          coroutineState,
          resourceState,
          operationState,
          result,
          completionCount,
          submitCount,
          releaseAuthorized,
          continuationAuthorized,
          reactorState,
          uringState>>

Init ==
  /\ backend \in Backends
  /\ coroutineState = "Running"
  /\ resourceState = "Open"
  /\ operationState = "None"
  /\ result = "NoResult"
  /\ completionCount = 0
  /\ submitCount = 0
  /\ releaseAuthorized = FALSE
  /\ continuationAuthorized = FALSE
  /\ reactorState = "Idle"
  /\ uringState = "Idle"

(***************************************************************************)
(* Reactor concrete actions                                                *)
(***************************************************************************)

SubmitPending ==
  /\ backend = "Reactor"
  /\ coroutineState = "Running"
  /\ resourceState = "Open"
  /\ operationState = "None"
  /\ reactorState = "Idle"
  /\ operationState' = "Pending"
  /\ submitCount' = submitCount + 1
  /\ reactorState' = "ChannelWaiting"
  /\ UNCHANGED <<backend,
                 coroutineState,
                 resourceState,
                 result,
                 completionCount,
                 releaseAuthorized,
                 continuationAuthorized,
                 uringState>>

(* Immediate syscall result: no suspension, physical use is already terminal.
 * The compiler continues through await_resume() inline, without a scheduled
 * continuation authorization. *)
ImmediateComplete ==
  /\ backend = "Reactor"
  /\ coroutineState = "Running"
  /\ resourceState = "Open"
  /\ operationState = "None"
  /\ reactorState = "Idle"
  /\ operationState' = "Completed"
  /\ result' \in {"Success", "EOF", "Error"}
  /\ completionCount' = completionCount + 1
  /\ submitCount' = submitCount + 1
  /\ releaseAuthorized' = TRUE
  /\ continuationAuthorized' = FALSE
  /\ UNCHANGED <<backend,
                 coroutineState,
                 resourceState,
                 reactorState,
                 uringState>>

(* Readiness only changes physical state and therefore projects to stutter. *)
Ready ==
  /\ backend = "Reactor"
  /\ reactorState = "ChannelWaiting"
  /\ reactorState' = "Ready"
  /\ UNCHANGED <<backend,
                 coroutineState,
                 resourceState,
                 operationState,
                 result,
                 completionCount,
                 submitCount,
                 releaseAuthorized,
                 continuationAuthorized,
                 uringState>>

Complete ==
  /\ backend = "Reactor"
  /\ coroutineState = "Waiting"
  /\ resourceState \in {"Open", "Closing"}
  /\ operationState = "Pending"
  /\ reactorState = "Ready"
  /\ operationState' = "Completed"
  /\ result' \in {"Success", "EOF", "Error"}
  /\ completionCount' = completionCount + 1
  /\ releaseAuthorized' = TRUE
  /\ continuationAuthorized' = TRUE
  /\ reactorState' = "Idle"
  /\ UNCHANGED <<backend,
                 coroutineState,
                 resourceState,
                 submitCount,
                 uringState>>

Cancel ==
  /\ backend = "Reactor"
  /\ coroutineState = "Waiting"
  /\ resourceState = "Closing"
  /\ operationState = "Pending"
  /\ reactorState \in {"ChannelWaiting", "Ready"}
  /\ operationState' = "Cancelled"
  /\ result' = "Cancelled"
  /\ completionCount' = completionCount + 1
  /\ releaseAuthorized' = TRUE
  /\ continuationAuthorized' = TRUE
  /\ reactorState' = "Idle"
  /\ UNCHANGED <<backend,
                 coroutineState,
                 resourceState,
                 submitCount,
                 uringState>>

(***************************************************************************)
(* io_uring concrete actions                                               *)
(***************************************************************************)

UringPrepareSQE ==
  /\ backend = "LUring"
  /\ coroutineState = "Running"
  /\ resourceState = "Open"
  /\ operationState = "None"
  /\ uringState = "Idle"
  /\ operationState' = "Pending"
  /\ submitCount' = submitCount + 1
  /\ uringState' = "SQEQueued"
  /\ UNCHANGED <<backend,
                 coroutineState,
                 resourceState,
                 result,
                 completionCount,
                 releaseAuthorized,
                 continuationAuthorized,
                 reactorState>>

(* Local validation or SQE preparation may fail before the coroutine
 * suspends. Just like a Reactor immediate syscall result, it is consumed by
 * await_resume() inline rather than through Scheduler::Schedule(). *)
UringImmediateComplete ==
  /\ backend = "LUring"
  /\ coroutineState = "Running"
  /\ resourceState = "Open"
  /\ operationState = "None"
  /\ uringState = "Idle"
  /\ operationState' = "Completed"
  /\ result' \in {"Success", "EOF", "Error"}
  /\ completionCount' = completionCount + 1
  /\ submitCount' = submitCount + 1
  /\ releaseAuthorized' = TRUE
  /\ continuationAuthorized' = FALSE
  /\ UNCHANGED <<backend,
                 coroutineState,
                 resourceState,
                 reactorState,
                 uringState>>

(* SQE submission is hidden by ObsLUring. *)
UringSubmit ==
  /\ backend = "LUring"
  /\ uringState = "SQEQueued"
  /\ uringState' = "Submitted"
  /\ UNCHANGED <<backend,
                 coroutineState,
                 resourceState,
                 operationState,
                 result,
                 completionCount,
                 submitCount,
                 releaseAuthorized,
                 continuationAuthorized,
                 reactorState>>

(* CQE availability is hidden until the adapter interprets it. *)
UringCQE ==
  /\ backend = "LUring"
  /\ uringState = "Submitted"
  /\ uringState' = "CQEReady"
  /\ UNCHANGED <<backend,
                 coroutineState,
                 resourceState,
                 operationState,
                 result,
                 completionCount,
                 submitCount,
                 releaseAuthorized,
                 continuationAuthorized,
                 reactorState>>

UringComplete ==
  /\ backend = "LUring"
  /\ coroutineState = "Waiting"
  /\ resourceState \in {"Open", "Closing"}
  /\ operationState = "Pending"
  /\ uringState = "CQEReady"
  /\ operationState' = "Completed"
  /\ result' \in {"Success", "EOF", "Error"}
  /\ completionCount' = completionCount + 1
  /\ releaseAuthorized' = TRUE
  /\ continuationAuthorized' = TRUE
  /\ uringState' = "Idle"
  /\ UNCHANGED <<backend,
                 coroutineState,
                 resourceState,
                 submitCount,
                 reactorState>>

(* The detailed target/cancel CQE convergence is modeled separately in     *)
(* resource_close_cancel.tla. This action is its single-result projection.  *)
UringCancel ==
  /\ backend = "LUring"
  /\ coroutineState = "Waiting"
  /\ resourceState = "Closing"
  /\ operationState = "Pending"
  /\ uringState \in {"SQEQueued", "Submitted", "CQEReady"}
  /\ operationState' = "Cancelled"
  /\ result' = "Cancelled"
  /\ completionCount' = completionCount + 1
  /\ releaseAuthorized' = TRUE
  /\ continuationAuthorized' = TRUE
  /\ uringState' = "Idle"
  /\ UNCHANGED <<backend,
                 coroutineState,
                 resourceState,
                 submitCount,
                 reactorState>>

(***************************************************************************)
(* Shared concrete actions                                                 *)
(***************************************************************************)

Suspend ==
  /\ coroutineState = "Running"
  /\ operationState = "Pending"
  /\ coroutineState' = "Waiting"
  /\ UNCHANGED <<backend,
                 resourceState,
                 operationState,
                 result,
                 completionCount,
                 submitCount,
                 releaseAuthorized,
                 continuationAuthorized,
                 reactorState,
                 uringState>>

Close ==
  /\ resourceState = "Open"
  /\ resourceState' =
       IF operationState = "Pending" THEN "Closing" ELSE "Closed"
  /\ UNCHANGED <<backend,
                 coroutineState,
                 operationState,
                 result,
                 completionCount,
                 submitCount,
                 releaseAuthorized,
                 continuationAuthorized,
                 reactorState,
                 uringState>>

FinalizeClose ==
  /\ resourceState = "Closing"
  /\ operationState # "Pending"
  /\ reactorState = "Idle"
  /\ uringState = "Idle"
  /\ resourceState' = "Closed"
  /\ UNCHANGED <<backend,
                 coroutineState,
                 operationState,
                 result,
                 completionCount,
                 submitCount,
                 releaseAuthorized,
                 continuationAuthorized,
                 reactorState,
                 uringState>>

(* await_suspend() returned false. The current coroutine observes the result
 * synchronously through await_resume(); no ResumeWork was enqueued. *)
InlineContinue ==
  /\ coroutineState = "Running"
  /\ operationState = "Completed"
  /\ result \in {"Success", "EOF", "Error"}
  /\ releaseAuthorized
  /\ ~continuationAuthorized
  /\ coroutineState' = "Ready"
  /\ UNCHANGED <<backend,
                 resourceState,
                 operationState,
                 result,
                 completionCount,
                 submitCount,
                 releaseAuthorized,
                 continuationAuthorized,
                 reactorState,
                 uringState>>

Resume ==
  /\ coroutineState = "Waiting"
  /\ operationState \in {"Completed", "Cancelled"}
  /\ continuationAuthorized
  /\ releaseAuthorized
  /\ coroutineState' = "Ready"
  /\ UNCHANGED <<backend,
                 resourceState,
                 operationState,
                 result,
                 completionCount,
                 submitCount,
                 releaseAuthorized,
                 continuationAuthorized,
                 reactorState,
                 uringState>>

Finish ==
  /\ coroutineState = "Ready"
  /\ coroutineState' = "Done"
  /\ UNCHANGED <<backend,
                 resourceState,
                 operationState,
                 result,
                 completionCount,
                 submitCount,
                 releaseAuthorized,
                 continuationAuthorized,
                 reactorState,
                 uringState>>

Next ==
  \/ SubmitPending
  \/ ImmediateComplete
  \/ Ready
  \/ Complete
  \/ Cancel
  \/ UringPrepareSQE
  \/ UringImmediateComplete
  \/ UringSubmit
  \/ UringCQE
  \/ UringComplete
  \/ UringCancel
  \/ Suspend
  \/ Close
  \/ FinalizeClose
  \/ InlineContinue
  \/ Resume
  \/ Finish

Spec == Init /\ [][Next]_vars

(***************************************************************************)
(* Logical specification and explicit observation functions               *)
(***************************************************************************)

(* The two functions intentionally project the same adapter-owned logical  *)
(* fields while hiding different backend execution states.                 *)
ObsReactor ==
  [coroutine |-> coroutineState,
   resource |-> resourceState,
   operation |-> operationState,
   result |-> result,
   resultReady |-> result # "NoResult",
   continuationAuthorized |-> continuationAuthorized,
   releaseAuthorized |-> releaseAuthorized]

ObsLUring ==
  [coroutine |-> coroutineState,
   resource |-> resourceState,
   operation |-> operationState,
   result |-> result,
   resultReady |-> result # "NoResult",
   continuationAuthorized |-> continuationAuthorized,
   releaseAuthorized |-> releaseAuthorized]

LogicalObservation ==
  IF backend = "Reactor" THEN ObsReactor ELSE ObsLUring

LogicalInit(state) ==
  /\ state.coroutine = "Running"
  /\ state.resource = "Open"
  /\ state.operation = "None"
  /\ state.result = "NoResult"
  /\ ~state.resultReady
  /\ ~state.continuationAuthorized
  /\ ~state.releaseAuthorized

LogicalSubmit(before, after) ==
  /\ before.coroutine = "Running"
  /\ before.resource = "Open"
  /\ before.operation = "None"
  /\ after.operation = "Pending"
  /\ after.coroutine = before.coroutine
  /\ after.resource = before.resource
  /\ after.result = before.result
  /\ after.resultReady = before.resultReady
  /\ after.continuationAuthorized = before.continuationAuthorized
  /\ after.releaseAuthorized = before.releaseAuthorized

LogicalImmediateResult(before, after) ==
  /\ before.coroutine = "Running"
  /\ before.resource = "Open"
  /\ before.operation = "None"
  /\ after.operation = "Completed"
  /\ after.result \in {"Success", "EOF", "Error"}
  /\ after.resultReady
  /\ ~after.continuationAuthorized
  /\ after.releaseAuthorized
  /\ after.coroutine = before.coroutine
  /\ after.resource = before.resource

LogicalSuspend(before, after) ==
  /\ before.coroutine = "Running"
  /\ before.operation = "Pending"
  /\ after.coroutine = "Waiting"
  /\ after.resource = before.resource
  /\ after.operation = before.operation
  /\ after.result = before.result
  /\ after.resultReady = before.resultReady
  /\ after.continuationAuthorized = before.continuationAuthorized
  /\ after.releaseAuthorized = before.releaseAuthorized

LogicalResult(before, after) ==
  /\ before.coroutine = "Waiting"
  /\ before.resource \in {"Open", "Closing"}
  /\ before.operation = "Pending"
  /\ after.operation = "Completed"
  /\ after.result \in {"Success", "EOF", "Error"}
  /\ after.resultReady
  /\ after.continuationAuthorized
  /\ after.releaseAuthorized
  /\ after.coroutine = before.coroutine
  /\ after.resource = before.resource

LogicalCancelResult(before, after) ==
  /\ before.coroutine = "Waiting"
  /\ before.resource = "Closing"
  /\ before.operation = "Pending"
  /\ after.operation = "Cancelled"
  /\ after.result = "Cancelled"
  /\ after.resultReady
  /\ after.continuationAuthorized
  /\ after.releaseAuthorized
  /\ after.coroutine = before.coroutine
  /\ after.resource = before.resource

LogicalClose(before, after) ==
  /\ before.resource = "Open"
  /\ after.resource =
       IF before.operation = "Pending" THEN "Closing" ELSE "Closed"
  /\ after.coroutine = before.coroutine
  /\ after.operation = before.operation
  /\ after.result = before.result
  /\ after.resultReady = before.resultReady
  /\ after.continuationAuthorized = before.continuationAuthorized
  /\ after.releaseAuthorized = before.releaseAuthorized

LogicalFinalizeClose(before, after) ==
  /\ before.resource = "Closing"
  /\ before.operation # "Pending"
  /\ after.resource = "Closed"
  /\ after.coroutine = before.coroutine
  /\ after.operation = before.operation
  /\ after.result = before.result
  /\ after.resultReady = before.resultReady
  /\ after.continuationAuthorized = before.continuationAuthorized
  /\ after.releaseAuthorized = before.releaseAuthorized

LogicalInlineContinue(before, after) ==
  /\ before.coroutine = "Running"
  /\ before.operation = "Completed"
  /\ before.result \in {"Success", "EOF", "Error"}
  /\ before.resultReady
  /\ before.releaseAuthorized
  /\ ~before.continuationAuthorized
  /\ after.coroutine = "Ready"
  /\ after.resource = before.resource
  /\ after.operation = before.operation
  /\ after.result = before.result
  /\ after.resultReady = before.resultReady
  /\ after.continuationAuthorized = before.continuationAuthorized
  /\ after.releaseAuthorized = before.releaseAuthorized

LogicalResume(before, after) ==
  /\ before.coroutine = "Waiting"
  /\ before.operation \in {"Completed", "Cancelled"}
  /\ before.resultReady
  /\ before.continuationAuthorized
  /\ before.releaseAuthorized
  /\ after.coroutine = "Ready"
  /\ after.resource = before.resource
  /\ after.operation = before.operation
  /\ after.result = before.result
  /\ after.resultReady = before.resultReady
  /\ after.continuationAuthorized = before.continuationAuthorized
  /\ after.releaseAuthorized = before.releaseAuthorized

LogicalFinish(before, after) ==
  /\ before.coroutine = "Ready"
  /\ after.coroutine = "Done"
  /\ after.resource = before.resource
  /\ after.operation = before.operation
  /\ after.result = before.result
  /\ after.resultReady = before.resultReady
  /\ after.continuationAuthorized = before.continuationAuthorized
  /\ after.releaseAuthorized = before.releaseAuthorized

LogicalNext(before, after) ==
  \/ LogicalSubmit(before, after)
  \/ LogicalImmediateResult(before, after)
  \/ LogicalSuspend(before, after)
  \/ LogicalResult(before, after)
  \/ LogicalCancelResult(before, after)
  \/ LogicalClose(before, after)
  \/ LogicalFinalizeClose(before, after)
  \/ LogicalInlineContinue(before, after)
  \/ LogicalResume(before, after)
  \/ LogicalFinish(before, after)

TraceRefinement ==
  /\ LogicalInit(LogicalObservation)
  /\ [][LogicalNext(LogicalObservation, LogicalObservation')]_LogicalObservation

(***************************************************************************)
(* Concrete safety invariants                                              *)
(***************************************************************************)

TypeOK ==
  /\ backend \in Backends
  /\ coroutineState \in CoroutineStates
  /\ resourceState \in ResourceStates
  /\ operationState \in OperationStates
  /\ result \in ResultStates
  /\ completionCount \in Nat
  /\ submitCount \in Nat
  /\ releaseAuthorized \in BOOLEAN
  /\ continuationAuthorized \in BOOLEAN
  /\ reactorState \in States
  /\ uringState \in UringStates

BackendStateShape ==
  /\ backend = "Reactor" => uringState = "Idle"
  /\ backend = "LUring" => reactorState = "Idle"

RefinementInvariant ==
  /\ backend = "Reactor"
       => (operationState = "Pending"
             <=> reactorState \in {"ChannelWaiting", "Ready"})
  /\ backend = "LUring"
       => (operationState = "Pending"
             <=> uringState \in {"SQEQueued", "Submitted", "CQEReady"})

UniqueCompletion == completionCount <= 1
SingleSubmission == submitCount <= 1

ResultReadiness ==
  (result # "NoResult") <=> operationState \in {"Completed", "Cancelled"}

ContinuationAuthorization ==
  continuationAuthorized
    => /\ operationState \in {"Completed", "Cancelled"}
       /\ result # "NoResult"

ReleaseAuthorization ==
  releaseAuthorized
    => operationState \in {"Completed", "Cancelled"}

ResumeSafety ==
  coroutineState \in {"Ready", "Done"}
    => /\ releaseAuthorized
       /\ result # "NoResult"

InlineCompletionSafety ==
  /\ coroutineState \in {"Ready", "Done"}
  /\ ~continuationAuthorized
  => /\ operationState = "Completed"
     /\ result \in {"Success", "EOF", "Error"}
     /\ releaseAuthorized

ClosedHasNoPending ==
  resourceState = "Closed" => operationState # "Pending"

========================================================================================
