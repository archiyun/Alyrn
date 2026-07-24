---------------------- MODULE async_stream_backend_refinement ----------------------

EXTENDS Naturals

(***************************************************************************)
(* Reactor / io_uring 到 AsyncStream 抽象状态的第一版 refinement 模型。   *)
(*                                                                         *)
(* 证明范围：                                                             *)
(*   - 一个 stream；                                                       *)
(*   - 一个 single-shot ReadSome；                                         *)
(*   - 正常完成、立即完成和 Close 取消；                                  *)
(*   - backend 在 Init 时选择，模型中不允许动态切换。                     *)
(*                                                                         *)
(* 注意：这是对 C++ 实现路径的有限抽象，不是 C++ 内存安全的自动证明。     *)
(***************************************************************************)

Backends       == {"Reactor", "LUring"}
CoroutineStates == {"Running", "Waiting", "Ready", "Done"}
ResourceStates  == {"Open", "Closing", "Closed"}
OperationStates == {"None", "Pending", "Completed", "Cancelled"}
ResultStates    == {"NoResult", "Success", "EOF", "Error", "Cancelled"}

ReactorStates == {"Idle", "ChannelWaiting", "Ready"}
UringStates   == {"Idle", "SQEQueued", "Submitted", "CQEReady"}

VARIABLES backend,
          coroutineState,
          resourceState,
          operationState,
          result,
          completionCount,
          submitCount,
          reactorState,
          uringState

vars == <<backend,
          coroutineState,
          resourceState,
          operationState,
          result,
          completionCount,
          submitCount,
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
  /\ reactorState = "Idle"
  /\ uringState = "Idle"

(***************************************************************************)
(* Reactor concrete actions                                                *)
(***************************************************************************)

(* EAGAIN 后注册 Channel，协程有一个 pending read。 *)
ReactorSubmitPending ==
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
                 uringState>>

(* nonblocking recv/send 立即给出结果，不经过 Suspend。 *)
ReactorImmediateComplete ==
  /\ backend = "Reactor"
  /\ coroutineState = "Running"
  /\ resourceState = "Open"
  /\ operationState = "None"
  /\ reactorState = "Idle"
  /\ operationState' = "Completed"
  /\ result' \in {"Success", "EOF", "Error"}
  /\ completionCount' = completionCount + 1
  /\ submitCount' = submitCount + 1
  /\ UNCHANGED <<backend,
                 coroutineState,
                 resourceState,
                 reactorState,
                 uringState>>

(* epoll readiness 是后端内部事件，对抽象协程状态不产生结果。 *)
ReactorReady ==
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
                 uringState>>

(* Channel callback 重试 recv，产生抽象 Complete。 *)
ReactorComplete ==
  /\ backend = "Reactor"
  /\ coroutineState = "Waiting"
  /\ resourceState \in {"Open", "Closing"}
  /\ operationState = "Pending"
  /\ reactorState = "Ready"
  /\ operationState' = "Completed"
  /\ result' \in {"Success", "EOF", "Error"}
  /\ completionCount' = completionCount + 1
  /\ reactorState' = "Idle"
  /\ UNCHANGED <<backend,
                 coroutineState,
                 resourceState,
                 submitCount,
                 uringState>>

(* Close 后由 Reactor 直接完成 pending read 的取消路径。 *)
ReactorCancel ==
  /\ backend = "Reactor"
  /\ coroutineState = "Waiting"
  /\ resourceState = "Closing"
  /\ operationState = "Pending"
  /\ reactorState \in {"ChannelWaiting", "Ready"}
  /\ operationState' = "Cancelled"
  /\ result' = "Cancelled"
  /\ completionCount' = completionCount + 1
  /\ reactorState' = "Idle"
  /\ UNCHANGED <<backend,
                 coroutineState,
                 resourceState,
                 submitCount,
                 uringState>>

(***************************************************************************)
(* io_uring concrete actions                                               *)
(***************************************************************************)

(* await_suspend 中准备一个 recv SQE。 *)
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
                 reactorState>>

(* FlushSubmit / io_uring_submit：SQE 到达内核。 *)
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
                 reactorState>>

(* 内核产生 CQE，仍然是后端内部状态。 *)
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
                 reactorState>>

(* HandleCqe -> LUringOp::Complete -> completion hook。 *)
UringComplete ==
  /\ backend = "LUring"
  /\ coroutineState = "Waiting"
  /\ resourceState \in {"Open", "Closing"}
  /\ operationState = "Pending"
  /\ uringState = "CQEReady"
  /\ operationState' = "Completed"
  /\ result' \in {"Success", "EOF", "Error"}
  /\ completionCount' = completionCount + 1
  /\ uringState' = "Idle"
  /\ UNCHANGED <<backend,
                 coroutineState,
                 resourceState,
                 submitCount,
                 reactorState>>

(* Close 的 cancel request 和 cancel CQE 被抽象成一次取消收敛。 *)
UringCancel ==
  /\ backend = "LUring"
  /\ coroutineState = "Waiting"
  /\ resourceState = "Closing"
  /\ operationState = "Pending"
  /\ uringState \in {"SQEQueued", "Submitted", "CQEReady"}
  /\ operationState' = "Cancelled"
  /\ result' = "Cancelled"
  /\ completionCount' = completionCount + 1
  /\ uringState' = "Idle"
  /\ UNCHANGED <<backend,
                 coroutineState,
                 resourceState,
                 submitCount,
                 reactorState>>

(***************************************************************************)
(* Shared abstract actions                                                *)
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
                 reactorState,
                 uringState>>

Resume ==
  /\ coroutineState \in {"Running", "Waiting"}
  /\ operationState \in {"Completed", "Cancelled"}
  /\ coroutineState' = "Ready"
  /\ UNCHANGED <<backend,
                 resourceState,
                 operationState,
                 result,
                 completionCount,
                 submitCount,
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
                 reactorState,
                 uringState>>

Next ==
  \/ ReactorSubmitPending
  \/ ReactorImmediateComplete
  \/ ReactorReady
  \/ ReactorComplete
  \/ ReactorCancel
  \/ UringPrepareSQE
  \/ UringSubmit
  \/ UringCQE
  \/ UringComplete
  \/ UringCancel
  \/ Suspend
  \/ Close
  \/ FinalizeClose
  \/ Resume
  \/ Finish

Spec == Init /\ [][Next]_vars

TypeOK ==
  /\ backend \in Backends
  /\ coroutineState \in CoroutineStates
  /\ resourceState \in ResourceStates
  /\ operationState \in OperationStates
  /\ result \in ResultStates
  /\ completionCount \in Nat
  /\ submitCount \in Nat
  /\ reactorState \in ReactorStates
  /\ uringState \in UringStates

(* 非当前 backend 的内部状态必须保持在 Idle。 *)
BackendStateShape ==
  /\ backend = "Reactor"
       => uringState = "Idle"
  /\ backend = "LUring"
       => reactorState = "Idle"

(* 具体 pending 状态必须与抽象 Pending 一致。 *)
RefinementInvariant ==
  /\ backend = "Reactor"
       => (operationState = "Pending"
             <=> reactorState \in {"ChannelWaiting", "Ready"})
  /\ backend = "LUring"
       => (operationState = "Pending"
             <=> uringState \in {"SQEQueued", "Submitted", "CQEReady"})

UniqueCompletion == completionCount <= 1
SingleSubmission == submitCount <= 1

ResumeAuthorization ==
  coroutineState \in {"Ready", "Done"}
    => operationState \in {"Completed", "Cancelled"}

ClosedHasNoPending ==
  resourceState = "Closed" => operationState # "Pending"

========================================================================================
