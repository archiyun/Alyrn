----------------------- MODULE linked_timeout_submission_failure -----------------------

EXTENDS Naturals

(***************************************************************************)
(* ReadSomeFor 的 linked read + timeout 提交失败模型。                    *)
(*                                                                         *)
(* read SQE 已经准备完成后，timeout SQE 可能因本地 SQ 空间或提交错误失败。 *)
(* 此时不存在 timeout physical request；awaiter 把 timeout member 视为  *)
(* synthetic-complete，并等待 read 的唯一 CQE。                            *)
(***************************************************************************)

ReadStates == {"Idle", "Submitted", "Completed"}
TimeoutStates == {"Idle", "Submitted", "Completed", "Omitted"}
AwaiterStates == {"Running", "Waiting", "LogicalComplete", "Resumed"}

VARIABLES readState,
          timeoutState,
          awaiterState,
          readSubmitCount,
          timeoutSubmitAttempts,
          timeoutPhysicalSubmitCount,
          logicalCompletionCount,
          resumeCount

vars == <<readState,
          timeoutState,
          awaiterState,
          readSubmitCount,
          timeoutSubmitAttempts,
          timeoutPhysicalSubmitCount,
          logicalCompletionCount,
          resumeCount>>

TimeoutSettled == timeoutState \in {"Completed", "Omitted"}

Init ==
  /\ readState = "Idle"
  /\ timeoutState = "Idle"
  /\ awaiterState = "Running"
  /\ readSubmitCount = 0
  /\ timeoutSubmitAttempts = 0
  /\ timeoutPhysicalSubmitCount = 0
  /\ logicalCompletionCount = 0
  /\ resumeCount = 0

SubmitRead ==
  /\ readState = "Idle"
  /\ awaiterState = "Running"
  /\ readState' = "Submitted"
  /\ awaiterState' = "Waiting"
  /\ readSubmitCount' = 1
  /\ UNCHANGED <<timeoutState,
                 timeoutSubmitAttempts,
                 timeoutPhysicalSubmitCount,
                 logicalCompletionCount,
                 resumeCount>>

SubmitTimeout ==
  /\ readState = "Submitted"
  /\ timeoutState = "Idle"
  /\ timeoutState' = "Submitted"
  /\ timeoutSubmitAttempts' = 1
  /\ timeoutPhysicalSubmitCount' = 1
  /\ UNCHANGED <<readState,
                 awaiterState,
                 readSubmitCount,
                 logicalCompletionCount,
                 resumeCount>>

(* No timeout CQE can follow this branch because no timeout request exists. *)
TimeoutSubmitFails ==
  /\ readState = "Submitted"
  /\ timeoutState = "Idle"
  /\ timeoutState' = "Omitted"
  /\ timeoutSubmitAttempts' = 1
  /\ UNCHANGED <<readState,
                 awaiterState,
                 readSubmitCount,
                 timeoutPhysicalSubmitCount,
                 logicalCompletionCount,
                 resumeCount>>

ReadComplete ==
  /\ readState = "Submitted"
  /\ readState' = "Completed"
  /\ UNCHANGED <<timeoutState,
                 awaiterState,
                 readSubmitCount,
                 timeoutSubmitAttempts,
                 timeoutPhysicalSubmitCount,
                 logicalCompletionCount,
                 resumeCount>>

TimeoutComplete ==
  /\ timeoutState = "Submitted"
  /\ timeoutState' = "Completed"
  /\ UNCHANGED <<readState,
                 awaiterState,
                 readSubmitCount,
                 timeoutSubmitAttempts,
                 timeoutPhysicalSubmitCount,
                 logicalCompletionCount,
                 resumeCount>>

AuthorizeLogicalCompletion ==
  /\ awaiterState = "Waiting"
  /\ readState = "Completed"
  /\ TimeoutSettled
  /\ logicalCompletionCount = 0
  /\ awaiterState' = "LogicalComplete"
  /\ logicalCompletionCount' = 1
  /\ UNCHANGED <<readState,
                 timeoutState,
                 readSubmitCount,
                 timeoutSubmitAttempts,
                 timeoutPhysicalSubmitCount,
                 resumeCount>>

Resume ==
  /\ awaiterState = "LogicalComplete"
  /\ resumeCount = 0
  /\ awaiterState' = "Resumed"
  /\ resumeCount' = 1
  /\ UNCHANGED <<readState,
                 timeoutState,
                 readSubmitCount,
                 timeoutSubmitAttempts,
                 timeoutPhysicalSubmitCount,
                 logicalCompletionCount>>

Next ==
  \/ SubmitRead
  \/ SubmitTimeout
  \/ TimeoutSubmitFails
  \/ ReadComplete
  \/ TimeoutComplete
  \/ AuthorizeLogicalCompletion
  \/ Resume

Spec == Init /\ [][Next]_vars

TypeOK ==
  /\ readState \in ReadStates
  /\ timeoutState \in TimeoutStates
  /\ awaiterState \in AwaiterStates
  /\ readSubmitCount \in Nat
  /\ timeoutSubmitAttempts \in Nat
  /\ timeoutPhysicalSubmitCount \in Nat
  /\ logicalCompletionCount \in Nat
  /\ resumeCount \in Nat

OneReadRequest == readSubmitCount <= 1
OneTimeoutAttempt == timeoutSubmitAttempts <= 1
OneTimeoutRequest == timeoutPhysicalSubmitCount <= 1
TimeoutFailureHasNoRequest ==
  timeoutState = "Omitted" => timeoutPhysicalSubmitCount = 0
LogicalCompletionOnce == logicalCompletionCount <= 1
ExactlyOnceResume == resumeCount <= 1
LogicalCompletionAuthorization ==
  logicalCompletionCount = 1
    => /\ readState = "Completed"
       /\ TimeoutSettled
ResumeAuthorization == resumeCount = 1 => logicalCompletionCount = 1

========================================================================================
