----------------------- MODULE scheduler_completion_liveness -----------------------

EXTENDS Naturals

(***************************************************************************)
(* LUringLoop completion-ready 队列的条件 liveness 模型。                  *)
(*                                                                         *)
(* ScheduleCompletion() 将已经获得逻辑完成授权的 continuation 放入         *)
(* completion-ready 队列。每个 RunReady() turn 都优先选择该队列中的一个    *)
(* work；normal ready work 不能抢在它之前。                                *)
(*                                                                         *)
(* 本模型的 WF_vars(LoopTurn) 是明确的环境假设：worker 没有退出且事件循环  *)
(* 会继续运行 turn。它不宣称内核调度、进程调度或已停止 worker 的无条件      *)
(* liveness。                                                               *)
(***************************************************************************)

CompletionStates == {"NotAuthorized", "Queued", "Resumed"}

VARIABLES completionState,
          normalReady,
          completionDispatchCount

vars == <<completionState,
          normalReady,
          completionDispatchCount>>

Init ==
  /\ completionState = "NotAuthorized"
  /\ normalReady = FALSE
  /\ completionDispatchCount = 0

(* A backend family has authorized exactly one logical continuation. *)
AuthorizeCompletion ==
  /\ completionState = "NotAuthorized"
  /\ completionState' = "Queued"
  /\ UNCHANGED <<normalReady, completionDispatchCount>>

(* Ordinary ready work may remain continuously backlogged. *)
EnqueueNormal ==
  /\ normalReady = FALSE
  /\ normalReady' = TRUE
  /\ UNCHANGED <<completionState, completionDispatchCount>>

(*
 * This abstracts one RunReady() turn. A queued completion is selected before
 * normal ready work, exactly as LUringLoop::RunReady() resets its per-turn
 * completion budget and picks completion_ready_ first.
 *)
LoopTurn ==
  /\ completionState = "Queued" \/ normalReady
  /\ IF completionState = "Queued"
        THEN /\ completionState' = "Resumed"
             /\ completionDispatchCount' = completionDispatchCount + 1
             /\ UNCHANGED normalReady
        ELSE /\ normalReady' = FALSE
             /\ UNCHANGED <<completionState, completionDispatchCount>>

Next ==
  \/ AuthorizeCompletion
  \/ EnqueueNormal
  \/ LoopTurn

Spec == Init /\ [][Next]_vars /\ WF_vars(LoopTurn)

TypeOK ==
  /\ completionState \in CompletionStates
  /\ normalReady \in BOOLEAN
  /\ completionDispatchCount \in Nat

ExactlyOnceDispatch == completionDispatchCount <= 1
ResumeAuthorization ==
  completionState = "Resumed" => completionDispatchCount = 1

(* Under the stated worker-turn fairness assumption, a queued completion runs. *)
CompletionEventuallyRuns ==
  completionState = "Queued" ~> completionState = "Resumed"

========================================================================================
