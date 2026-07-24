------------------------------ MODULE async_stream_core ------------------------------

EXTENDS Naturals

(***************************************************************************)
(* CoroPact AsyncStream 的最小可检查模型。                              *)
(*                                                                         *)
(* 范围：一个协程、一个 stream、一个 single-shot operation。             *)
(* 不描述 fd、SQE、CQE 或具体 backend，只描述协程可观察状态。            *)
(***************************************************************************)

CoroutineStates == {"Running", "Waiting", "Ready", "Done"}
ResourceStates  == {"Open", "Closing", "Closed"}
OperationStates == {"None", "Pending", "Completed", "Cancelled"}
ResultStates    == {"NoResult", "Success", "EOF", "Error", "Cancelled"}

VARIABLES coroutineState,
          resourceState,
          operationState,
          result,
          completionCount,
          submitCount

vars == <<coroutineState,
          resourceState,
          operationState,
          result,
          completionCount,
          submitCount>>

Init ==
  /\ coroutineState = "Running"
  /\ resourceState = "Open"
  /\ operationState = "None"
  /\ result = "NoResult"
  /\ completionCount = 0
  /\ submitCount = 0

(* 一次真正的异步提交；协程随后可以挂起。 *)
Submit ==
  /\ coroutineState = "Running"
  /\ resourceState = "Open"
  /\ operationState = "None"
  /\ operationState' = "Pending"
  /\ submitCount' = submitCount + 1
  /\ UNCHANGED <<coroutineState,
                 resourceState,
                 result,
                 completionCount>>

(* 一次实际挂起；这是可选路径，不是所有 Submit 都必须经过它。 *)
Suspend ==
  /\ coroutineState = "Running"
  /\ operationState = "Pending"
  /\ coroutineState' = "Waiting"
  /\ UNCHANGED <<resourceState,
                 operationState,
                 result,
                 completionCount,
                 submitCount>>

(* Reactor 的 nonblocking fast path：提交和完成在同一个抽象步内发生。 *)
ImmediateComplete ==
  /\ coroutineState = "Running"
  /\ resourceState = "Open"
  /\ operationState = "None"
  /\ operationState' = "Completed"
  /\ result' \in {"Success", "EOF", "Error"}
  /\ completionCount' = completionCount + 1
  /\ submitCount' = submitCount + 1
  /\ UNCHANGED <<coroutineState, resourceState>>

(* pending operation 的正常完成、EOF 或传输错误。 *)
Complete ==
  /\ coroutineState = "Waiting"
  /\ resourceState \in {"Open", "Closing"}
  /\ operationState = "Pending"
  /\ operationState' = "Completed"
  /\ result' \in {"Success", "EOF", "Error"}
  /\ completionCount' = completionCount + 1
  /\ UNCHANGED <<coroutineState,
                 resourceState,
                 submitCount>>

(* Cancel 也必须产生一个可观察结果，不能静默丢弃 operation。 *)
Cancel ==
  /\ coroutineState = "Waiting"
  /\ resourceState \in {"Open", "Closing"}
  /\ operationState = "Pending"
  /\ operationState' = "Cancelled"
  /\ result' = "Cancelled"
  /\ completionCount' = completionCount + 1
  /\ UNCHANGED <<coroutineState,
                 resourceState,
                 submitCount>>

(* Close 后不再接受新的成功提交；有 pending operation 时先进入 Closing。 *)
Close ==
  /\ resourceState = "Open"
  /\ resourceState' =
       IF operationState = "Pending" THEN "Closing" ELSE "Closed"
  /\ UNCHANGED <<coroutineState,
                 operationState,
                 result,
                 completionCount,
                 submitCount>>

(* backend 不再持有 pending operation 后，资源才能进入 Closed。 *)
FinalizeClose ==
  /\ resourceState = "Closing"
  /\ operationState # "Pending"
  /\ resourceState' = "Closed"
  /\ UNCHANGED <<coroutineState,
                 operationState,
                 result,
                 completionCount,
                 submitCount>>

(* Complete 或 Cancel 之后，等待中的协程才能恢复。 *)
Resume ==
  /\ coroutineState \in {"Running", "Waiting"}
  /\ operationState \in {"Completed", "Cancelled"}
  /\ coroutineState' = "Ready"
  /\ UNCHANGED <<resourceState,
                 operationState,
                 result,
                 completionCount,
                 submitCount>>

(* 这个模型只模拟一次 I/O；恢复后的协程结束。 *)
Finish ==
  /\ coroutineState = "Ready"
  /\ coroutineState' = "Done"
  /\ UNCHANGED <<resourceState,
                 operationState,
                 result,
                 completionCount,
                 submitCount>>

Next ==
  \/ Submit
  \/ Suspend
  \/ ImmediateComplete
  \/ Complete
  \/ Cancel
  \/ Close
  \/ FinalizeClose
  \/ Resume
  \/ Finish

Spec == Init /\ [][Next]_vars

(* 所有状态变量都必须保持在模型定义的集合中。 *)
TypeOK ==
  /\ coroutineState \in CoroutineStates
  /\ resourceState \in ResourceStates
  /\ operationState \in OperationStates
  /\ result \in ResultStates
  /\ completionCount \in Nat
  /\ submitCount \in Nat

(* single-shot operation 最多产生一个终态结果。 *)
UniqueCompletion == completionCount <= 1

(* 一个模型实例只允许一次语义提交。 *)
SingleSubmission == submitCount <= 1

(* 协程进入 Ready 或 Done 前，必须已经有 operation 结果。 *)
ResumeAuthorization ==
  coroutineState \in {"Ready", "Done"}
    => operationState \in {"Completed", "Cancelled"}

(* Closed 状态不能再持有 pending operation。 *)
ClosedHasNoPending ==
  resourceState = "Closed" => operationState # "Pending"

========================================================================================
