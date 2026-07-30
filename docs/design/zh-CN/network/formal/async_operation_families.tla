--------------------------- MODULE async_operation_families ---------------------------

EXTENDS Naturals

CONSTANT MaxCqes

(***************************************************************************)
(* CoroPact operation family 的完成基数模型。                             *)
(*                                                                         *)
(* 本模块不替代 async_stream_core.tla。                                    *)
(*   - async_stream_core.tla 验证一个 single-shot stream operation。      *)
(*   - 本模块验证 physical request 与 logical operation 的完成关系。       *)
(*                                                                         *)
(* 一个 Composite operation 由多个 physical request 组成；一个            *)
(* MultiShot request 只有一次 submit，但可以产生多个 CQE。                *)
(* SplitRelease request 的业务结果与其资源释放由不同 CQE 决定。           *)
(* async_operation_families.cfg 使用 MaxCqes = 4 做有界状态检查。          *)
(* 该检查验证协议形状，不宣称对无界 CQE 序列完成形式证明。               *)
(***************************************************************************)

RequestKinds  == {"SingleShot", "MultiShot", "Composite", "SplitRelease"}
RequestStates == {"Idle", "Active", "Terminal", "Cancelled", "Released"}
ResultStates  == {"NoResult", "Success", "Cancelled", "Ended"}

VARIABLES requestKind,
          requestState,
          submitCount,
          cqeCount,
          terminalCqeCount,
          memberCount,
          completedMembers,
          eventCount,
          consumedEvents,
          logicalTerminalCount,
          logicalResult,
          cancelRequested,
          releaseCount,
          resumeCount

vars == <<requestKind,
          requestState,
          submitCount,
          cqeCount,
          terminalCqeCount,
          memberCount,
          completedMembers,
          eventCount,
          consumedEvents,
          logicalTerminalCount,
          logicalResult,
          cancelRequested,
          releaseCount,
          resumeCount>>

Init ==
  /\ MaxCqes \in Nat
  /\ MaxCqes > 0
  /\ requestKind \in RequestKinds
  /\ requestState = "Idle"
  /\ submitCount = 0
  /\ cqeCount = 0
  /\ terminalCqeCount = 0
  /\ memberCount = IF requestKind = "Composite" THEN 2 ELSE 1
  /\ completedMembers = 0
  /\ eventCount = 0
  /\ consumedEvents = 0
  /\ logicalTerminalCount = 0
  /\ logicalResult = "NoResult"
  /\ cancelRequested = FALSE
  /\ releaseCount = 0
  /\ resumeCount = 0

(*
 * submitCount 是 logical operation 的聚合计数：
 *   SingleShot / MultiShot: 一个 physical request，submitCount = 1；
 *   Composite: 两个 physical request，每个 request 各提交一次。
 *
 * 因此“每个 physical request 只能 submit 一次”由各 family 的 shape
 * invariant 表达，而不是把 Composite 的聚合计数误当成一个 request。
 *)
Submit ==
  /\ requestState = "Idle"
  /\ submitCount = 0
  /\ requestState' = "Active"
  /\ submitCount' = memberCount
  /\ UNCHANGED <<requestKind,
                 cqeCount,
                 terminalCqeCount,
                 memberCount,
                 completedMembers,
                 eventCount,
                 consumedEvents,
                 logicalTerminalCount,
                 logicalResult,
                 cancelRequested,
                 releaseCount,
                 resumeCount>>

(* Single-shot request: one CQE is both the physical and logical completion. *)
SingleShotComplete ==
  /\ requestKind = "SingleShot"
  /\ requestState = "Active"
  /\ cancelRequested = FALSE
  /\ cqeCount < MaxCqes
  /\ requestState' = "Terminal"
  /\ cqeCount' = cqeCount + 1
  /\ terminalCqeCount' = terminalCqeCount + 1
  /\ logicalTerminalCount' = 1
  /\ logicalResult' = "Success"
  /\ UNCHANGED <<requestKind,
                 submitCount,
                 memberCount,
                 completedMembers,
                 eventCount,
                 consumedEvents,
                 cancelRequested,
                 releaseCount,
                 resumeCount>>

(*
 * MultiShot 的 F_MORE CQE：已经产生一个业务事件，但 request 仍然 active。
 * 同一个 physical request 后面仍然可以产生更多 CQE。
 *)
MultiShotEvent ==
  /\ requestKind = "MultiShot"
  /\ requestState = "Active"
  /\ cancelRequested = FALSE
  /\ cqeCount < MaxCqes
  /\ cqeCount' = cqeCount + 1
  /\ eventCount' = eventCount + 1
  /\ UNCHANGED <<requestKind,
                 requestState,
                 submitCount,
                 terminalCqeCount,
                 memberCount,
                 completedMembers,
                 consumedEvents,
                 logicalTerminalCount,
                 logicalResult,
                 cancelRequested,
                 releaseCount,
                 resumeCount>>

(* MultiShot 的终止 CQE：不再允许后续事件。 *)
MultiShotTerminate ==
  /\ requestKind = "MultiShot"
  /\ requestState = "Active"
  /\ cancelRequested = FALSE
  /\ cqeCount < MaxCqes
  /\ requestState' = "Terminal"
  /\ cqeCount' = cqeCount + 1
  /\ terminalCqeCount' = terminalCqeCount + 1
  /\ logicalTerminalCount' = 1
  /\ logicalResult' = "Ended"
  /\ UNCHANGED <<requestKind,
                 submitCount,
                 memberCount,
                 completedMembers,
                 eventCount,
                 consumedEvents,
                 cancelRequested,
                 releaseCount,
                 resumeCount>>

(* Composite 的一个 physical member 完成；最后一个 member 才确定逻辑结果。 *)
CompositeMemberComplete ==
  /\ requestKind = "Composite"
  /\ requestState = "Active"
  /\ cancelRequested = FALSE
  /\ completedMembers < memberCount
  /\ cqeCount < MaxCqes
  /\ completedMembers' = completedMembers + 1
  /\ cqeCount' = cqeCount + 1
  /\ terminalCqeCount' = terminalCqeCount + 1
  /\ requestState' =
       IF completedMembers + 1 = memberCount THEN "Terminal" ELSE "Active"
  /\ logicalTerminalCount' =
       IF completedMembers + 1 = memberCount THEN 1 ELSE logicalTerminalCount
  /\ logicalResult' =
       IF completedMembers + 1 = memberCount THEN "Success" ELSE logicalResult
  /\ UNCHANGED <<requestKind,
                 submitCount,
                 memberCount,
                 eventCount,
                 consumedEvents,
                 cancelRequested,
                 releaseCount,
                 resumeCount>>

(* 取消请求本身不是终态；仍然要等待对应 physical completion。 *)
RequestCancel ==
  /\ requestState = "Active"
  /\ cancelRequested = FALSE
  /\ cancelRequested' = TRUE
  /\ UNCHANGED <<requestKind,
                 requestState,
                 submitCount,
                 cqeCount,
                 terminalCqeCount,
                 memberCount,
                 completedMembers,
                 eventCount,
                 consumedEvents,
                 logicalTerminalCount,
                 logicalResult,
                 releaseCount,
                 resumeCount>>

(*
 * 取消完成也按 physical request 计数：
 *   - SingleShot / MultiShot 只有一个终止 CQE；
 *   - Composite 要逐个收敛 member，最后一个 member 才能结束逻辑操作。
 *)
CancelComplete ==
  /\ requestKind # "SplitRelease"
  /\ requestState = "Active"
  /\ cancelRequested = TRUE
  /\ cqeCount < MaxCqes
  /\ cqeCount' = cqeCount + 1
  /\ terminalCqeCount' = terminalCqeCount + 1
  /\ completedMembers' =
       IF requestKind = "Composite"
       THEN completedMembers + 1
       ELSE completedMembers
  /\ requestState' =
       IF requestKind = "Composite" /\ completedMembers + 1 < memberCount
       THEN "Active"
       ELSE "Cancelled"
  /\ logicalTerminalCount' =
       IF requestKind = "Composite" /\ completedMembers + 1 < memberCount
       THEN logicalTerminalCount
       ELSE 1
  /\ logicalResult' =
       IF requestKind = "Composite" /\ completedMembers + 1 < memberCount
       THEN logicalResult
       ELSE "Cancelled"
  /\ UNCHANGED <<requestKind,
                 submitCount,
                 memberCount,
                 eventCount,
                 consumedEvents,
                 cancelRequested,
                 releaseCount,
                 resumeCount>>

(* 已经产生的 multishot 事件可以在 source 终止后继续被消费。 *)
ConsumeEvent ==
  /\ requestKind = "MultiShot"
  /\ consumedEvents < eventCount
  /\ consumedEvents' = consumedEvents + 1
  /\ UNCHANGED <<requestKind,
                 requestState,
                 submitCount,
                 cqeCount,
                 terminalCqeCount,
                 memberCount,
                 completedMembers,
                 eventCount,
                 logicalTerminalCount,
                 logicalResult,
                 cancelRequested,
                 releaseCount,
                 resumeCount>>

(* request 只有在逻辑终态确定且所有已产生事件被消费后才能释放。 *)
ReleaseRequest ==
  /\ requestKind # "SplitRelease"
  /\ requestState \in {"Terminal", "Cancelled"}
  /\ logicalTerminalCount = 1
  /\ consumedEvents = eventCount
  /\ requestState' = "Released"
  /\ UNCHANGED <<requestKind,
                 submitCount,
                 cqeCount,
                 terminalCqeCount,
                 memberCount,
                 completedMembers,
                 eventCount,
                 consumedEvents,
                 logicalTerminalCount,
                 logicalResult,
                 cancelRequested,
                 releaseCount,
                 resumeCount>>

(*
 * send zerocopy 的 primary CQE 决定业务结果，但内核仍可能引用调用者的
 * buffer。即使 cancel 已请求，target request 的 primary CQE 仍是结果
 * 边界；cancel CQE 本身不是这个 request 的 terminal completion。
 *)
SplitPrimaryComplete ==
  /\ requestKind = "SplitRelease"
  /\ requestState \in {"Active", "Terminal"}
  /\ logicalTerminalCount = 0
  /\ cqeCount < MaxCqes
  /\ cqeCount' = cqeCount + 1
  /\ logicalTerminalCount' = 1
  /\ logicalResult' = IF cancelRequested THEN "Cancelled" ELSE "Success"
  /\ UNCHANGED <<requestKind,
                 requestState,
                 submitCount,
                 terminalCqeCount,
                 memberCount,
                 completedMembers,
                 eventCount,
                 consumedEvents,
                 cancelRequested,
                 releaseCount,
                 resumeCount>>

(*
 * F_NOTIF is the physical terminal boundary for the submitted send-zc
 * request. It may be observed before the primary CQE in this abstract model,
 * so release authorization remains a separate action.
 *)
SplitNotification ==
  /\ requestKind = "SplitRelease"
  /\ requestState = "Active"
  /\ terminalCqeCount = 0
  /\ cqeCount < MaxCqes
  /\ requestState' = "Terminal"
  /\ cqeCount' = cqeCount + 1
  /\ terminalCqeCount' = 1
  /\ UNCHANGED <<requestKind,
                 submitCount,
                 memberCount,
                 completedMembers,
                 eventCount,
                 consumedEvents,
                 logicalTerminalCount,
                 logicalResult,
                 cancelRequested,
                 releaseCount,
                 resumeCount>>

(* Buffer reuse is legal only after both boundaries have been observed. *)
AuthorizeSplitRelease ==
  /\ requestKind = "SplitRelease"
  /\ requestState = "Terminal"
  /\ logicalTerminalCount = 1
  /\ terminalCqeCount = 1
  /\ releaseCount = 0
  /\ requestState' = "Released"
  /\ releaseCount' = 1
  /\ UNCHANGED <<requestKind,
                 submitCount,
                 cqeCount,
                 terminalCqeCount,
                 memberCount,
                 completedMembers,
                 eventCount,
                 consumedEvents,
                 logicalTerminalCount,
                 logicalResult,
                 cancelRequested,
                 resumeCount>>

(* The awaiting coroutine is resumed only after its buffer is reusable. *)
ResumeSplitContinuation ==
  /\ requestKind = "SplitRelease"
  /\ requestState = "Released"
  /\ releaseCount = 1
  /\ resumeCount = 0
  /\ resumeCount' = 1
  /\ UNCHANGED <<requestKind,
                 requestState,
                 submitCount,
                 cqeCount,
                 terminalCqeCount,
                 memberCount,
                 completedMembers,
                 eventCount,
                 consumedEvents,
                 logicalTerminalCount,
                 logicalResult,
                 cancelRequested,
                 releaseCount>>

Next ==
  \/ Submit
  \/ SingleShotComplete
  \/ MultiShotEvent
  \/ MultiShotTerminate
  \/ CompositeMemberComplete
  \/ RequestCancel
  \/ CancelComplete
  \/ ConsumeEvent
  \/ ReleaseRequest
  \/ SplitPrimaryComplete
  \/ SplitNotification
  \/ AuthorizeSplitRelease
  \/ ResumeSplitContinuation

Spec == Init /\ [][Next]_vars

TypeOK ==
  /\ requestKind \in RequestKinds
  /\ requestState \in RequestStates
  /\ submitCount \in Nat
  /\ cqeCount \in Nat
  /\ terminalCqeCount \in Nat
  /\ memberCount \in Nat
  /\ completedMembers \in Nat
  /\ eventCount \in Nat
  /\ consumedEvents \in Nat
  /\ logicalTerminalCount \in Nat
  /\ logicalResult \in ResultStates
  /\ cancelRequested \in BOOLEAN
  /\ releaseCount \in Nat
  /\ resumeCount \in Nat
  /\ cqeCount <= MaxCqes
  /\ eventCount <= MaxCqes

(* 每个 single-shot physical request 只能提交和终止一次。 *)
SingleShotShape ==
  requestKind = "SingleShot"
    => /\ memberCount = 1
       /\ submitCount <= 1
       /\ cqeCount <= 1
       /\ terminalCqeCount <= 1

(* 一个 multishot physical request 只提交一次，但可以产生多个 CQE。 *)
MultiShotShape ==
  requestKind = "MultiShot"
    => /\ memberCount = 1
       /\ submitCount <= 1
       /\ terminalCqeCount <= 1
       /\ logicalTerminalCount <= 1

(* Composite 是两个各自 single-shot 的 physical request 的聚合。 *)
CompositeShape ==
  requestKind = "Composite"
    => /\ memberCount = 2
       /\ submitCount <= memberCount
       /\ completedMembers <= memberCount
       /\ terminalCqeCount <= memberCount
       /\ logicalTerminalCount <= 1

(* One send-zc request emits a primary CQE and one terminal notification. *)
SplitReleaseShape ==
  requestKind = "SplitRelease"
    => /\ memberCount = 1
       /\ submitCount <= 1
       /\ cqeCount <= 2
       /\ terminalCqeCount <= 1
       /\ logicalTerminalCount <= 1
       /\ releaseCount <= 1
       /\ resumeCount <= 1

(* 一个物理请求 family 的终止不能被重复宣告为逻辑终态。 *)
LogicalTerminalOnce == logicalTerminalCount <= 1

(* 逻辑结果确定前不能释放 request。 *)
ReleaseAuthorization ==
  requestState = "Released" => logicalTerminalCount = 1

(* Split release requires both the business result and the notification. *)
SplitReleaseAuthorization ==
  requestKind = "SplitRelease"
    => /\ releaseCount = 1
          => /\ logicalTerminalCount = 1
             /\ terminalCqeCount = 1
       /\ resumeCount = 1 => releaseCount = 1

(* 逻辑终态只能对应已停止产生事件的 request 状态。 *)
TerminalState ==
  requestKind # "SplitRelease" /\ logicalTerminalCount = 1
    => requestState \in {"Terminal", "Cancelled", "Released"}

(* 事件只能消费已经产生的事件。 *)
ConsumedEventsBound == consumedEvents <= eventCount

========================================================================================
