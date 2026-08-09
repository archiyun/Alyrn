-------------------------- MODULE resource_close_cancel --------------------------

EXTENDS Naturals

(***************************************************************************)
(* Resource-level Close / Cancel / Release model.                         *)
(*                                                                         *)
(* This is deliberately independent of Reactor, io_uring, fd, CQE and     *)
(* concrete awaiter classes.  It specifies the common semantic rule:      *)
(*                                                                         *)
(*   Close is a drain barrier. Cancel is only a protocol used to make       *)
(*   physical requests reach their terminal boundary.                      *)
(*                                                                         *)
(* A cancel acknowledgement is not a target completion.  The original      *)
(* target request must still become terminal before the resource, borrowed  *)
(* storage, or awaiter dispatch address may be released.                   *)
(***************************************************************************)

Operations == {"Read", "Write"}

ResourceStates == {"Open", "Closing", "Quiescent", "Closed"}
FdStates == {"Open", "Released"}
OperationStates == {"Absent", "Pending", "Terminal", "Released"}
CancelStates == {"NotRequested", "Submitting", "Submitted", "Acknowledged", "SubmitFailed"}
LogicalStates == {"Pending", "Ready"}
StorageStates == {"NoStorage", "Retained", "ReleaseAuthorized"}
ContinuationStates == {"NotBound", "Waiting", "Scheduled", "Resumed"}
Results == {"NoResult", "Success", "Error", "Cancelled"}

VARIABLES resourceState,
          fdState,
          closeStarted,
          operationState,
          cancelState,
          cancelFailuresRemaining,
          logicalState,
          result,
          storageState,
          continuationState,
          closeContinuationState,
          submitCount,
          targetCompletionCount,
          resumeCount,
          cancelSubmitAttempts

vars == <<resourceState,
          fdState,
          closeStarted,
          operationState,
          cancelState,
          cancelFailuresRemaining,
          logicalState,
          result,
          storageState,
          continuationState,
          closeContinuationState,
          submitCount,
          targetCompletionCount,
          resumeCount,
          cancelSubmitAttempts>>

TargetTerminal(op) == operationState[op] \in {"Terminal", "Released"}
TargetDrained(op) == operationState[op] \in {"Absent", "Terminal", "Released"}
CancelCommandDrained(op) == cancelState[op] \notin {"Submitting", "Submitted"}
StorageReleasedForBackend(op) == storageState[op] \in {"NoStorage", "ReleaseAuthorized"}

AllTargetsQuiescent ==
  \A op \in Operations:
    /\ TargetDrained(op)
    /\ CancelCommandDrained(op)
    /\ StorageReleasedForBackend(op)

Init ==
  /\ resourceState = "Open"
  /\ fdState = "Open"
  /\ closeStarted = FALSE
  /\ operationState = [op \in Operations |-> "Absent"]
  /\ cancelState = [op \in Operations |-> "NotRequested"]
  /\ cancelFailuresRemaining = [op \in Operations |-> 1]
  /\ logicalState = [op \in Operations |-> "Pending"]
  /\ result = [op \in Operations |-> "NoResult"]
  /\ storageState = [op \in Operations |-> "NoStorage"]
  /\ continuationState = [op \in Operations |-> "NotBound"]
  /\ closeContinuationState = "NotBound"
  /\ submitCount = [op \in Operations |-> 0]
  /\ targetCompletionCount = [op \in Operations |-> 0]
  /\ resumeCount = [op \in Operations |-> 0]
  /\ cancelSubmitAttempts = [op \in Operations |-> 0]

(* A normal awaiter reserves backend-visible borrowed/owned storage and binds
   its continuation.  This bounded model intentionally gives each direction
   one logical operation identity. *)
Submit(op) ==
  /\ op \in Operations
  /\ resourceState = "Open"
  /\ ~closeStarted
  /\ operationState[op] = "Absent"
  /\ operationState' = [operationState EXCEPT ![op] = "Pending"]
  /\ storageState' = [storageState EXCEPT ![op] = "Retained"]
  /\ continuationState' = [continuationState EXCEPT ![op] = "Waiting"]
  /\ submitCount' = [submitCount EXCEPT ![op] = @ + 1]
  /\ UNCHANGED <<resourceState,
                 fdState,
                 closeStarted,
                 cancelState,
                 cancelFailuresRemaining,
                 logicalState,
                 result,
                 closeContinuationState,
                 targetCompletionCount,
                 resumeCount,
                 cancelSubmitAttempts>>

(* Close linearizes here.  From this point the resource never becomes Open
   again, including when submitting a later cancel request fails. *)
BeginClose ==
  /\ resourceState = "Open"
  /\ ~closeStarted
  /\ resourceState' = "Closing"
  /\ closeStarted' = TRUE
  /\ closeContinuationState' = "Waiting"
  /\ UNCHANGED <<fdState,
                 operationState,
                 cancelState,
                 cancelFailuresRemaining,
                 logicalState,
                 result,
                 storageState,
                 continuationState,
                 submitCount,
                 targetCompletionCount,
                 resumeCount,
                 cancelSubmitAttempts>>

(* Requesting cancellation has no completion or release effect. *)
RequestCancel(op) ==
  /\ op \in Operations
  /\ resourceState = "Closing"
  /\ operationState[op] = "Pending"
  /\ cancelState[op] \in {"NotRequested", "SubmitFailed"}
  /\ cancelState' = [cancelState EXCEPT ![op] = "Submitting"]
  /\ cancelSubmitAttempts' = [cancelSubmitAttempts EXCEPT ![op] = @ + 1]
  /\ UNCHANGED <<resourceState,
                 fdState,
                 closeStarted,
                 operationState,
                 cancelFailuresRemaining,
                 logicalState,
                 result,
                 storageState,
                 continuationState,
                 closeContinuationState,
                 submitCount,
                 targetCompletionCount,
                 resumeCount>>

(* This models a local failure before a cancel request reaches the backend.
   The one-step budget forces a later retry to take the submitted path, so TLC
   checks both failure handling and eventual close under a finite assumption. *)
CancelSubmitFails(op) ==
  /\ op \in Operations
  /\ cancelState[op] = "Submitting"
  /\ cancelFailuresRemaining[op] > 0
  /\ cancelState' = [cancelState EXCEPT ![op] = "SubmitFailed"]
  /\ cancelFailuresRemaining' = [cancelFailuresRemaining EXCEPT ![op] = @ - 1]
  /\ UNCHANGED <<resourceState,
                 fdState,
                 closeStarted,
                 operationState,
                 logicalState,
                 result,
                 storageState,
                 continuationState,
                 closeContinuationState,
                 submitCount,
                 targetCompletionCount,
                 resumeCount,
                 cancelSubmitAttempts>>

CancelSubmitted(op) ==
  /\ op \in Operations
  /\ cancelState[op] = "Submitting"
  /\ cancelState' = [cancelState EXCEPT ![op] = "Submitted"]
  /\ UNCHANGED <<resourceState,
                 fdState,
                 closeStarted,
                 operationState,
                 cancelFailuresRemaining,
                 logicalState,
                 result,
                 storageState,
                 continuationState,
                 closeContinuationState,
                 submitCount,
                 targetCompletionCount,
                 resumeCount,
                 cancelSubmitAttempts>>

(* The cancel CQE acknowledges only the cancel command.  It deliberately does
   not settle or release the original read/write request. *)
CancelAcknowledged(op) ==
  /\ op \in Operations
  /\ cancelState[op] = "Submitted"
  /\ cancelState' = [cancelState EXCEPT ![op] = "Acknowledged"]
  /\ UNCHANGED <<resourceState,
                 fdState,
                 closeStarted,
                 operationState,
                 cancelFailuresRemaining,
                 logicalState,
                 result,
                 storageState,
                 continuationState,
                 closeContinuationState,
                 submitCount,
                 targetCompletionCount,
                 resumeCount,
                 cancelSubmitAttempts>>

(* The target CQE is the only action that ends backend access to its storage.
   A Close race may still yield a successful or an error result; cancellation
   is a possible result, not a forced rewrite of an earlier completion. *)
TargetComplete(op) ==
  /\ op \in Operations
  /\ operationState[op] = "Pending"
  /\ operationState' = [operationState EXCEPT ![op] = "Terminal"]
  /\ logicalState' = [logicalState EXCEPT ![op] = "Ready"]
  /\ result' \in {[result EXCEPT ![op] = "Success"],
                  [result EXCEPT ![op] = "Error"],
                  [result EXCEPT ![op] = "Cancelled"]}
  /\ targetCompletionCount' = [targetCompletionCount EXCEPT ![op] = @ + 1]
  /\ UNCHANGED <<resourceState,
                 fdState,
                 closeStarted,
                 cancelState,
                 cancelFailuresRemaining,
                 storageState,
                 continuationState,
                 closeContinuationState,
                 submitCount,
                 resumeCount,
                 cancelSubmitAttempts>>

(* This is the physical release authorization for a borrowed/owned buffer.
   It is independent from resuming the user coroutine. *)
AuthorizeStorageRelease(op) ==
  /\ op \in Operations
  /\ TargetTerminal(op)
  /\ storageState[op] = "Retained"
  /\ storageState' = [storageState EXCEPT ![op] = "ReleaseAuthorized"]
  /\ UNCHANGED <<resourceState,
                 fdState,
                 closeStarted,
                 operationState,
                 cancelState,
                 cancelFailuresRemaining,
                 logicalState,
                 result,
                 continuationState,
                 closeContinuationState,
                 submitCount,
                 targetCompletionCount,
                 resumeCount,
                 cancelSubmitAttempts>>

ScheduleOperationContinuation(op) ==
  /\ op \in Operations
  /\ logicalState[op] = "Ready"
  /\ storageState[op] = "ReleaseAuthorized"
  /\ continuationState[op] = "Waiting"
  /\ continuationState' = [continuationState EXCEPT ![op] = "Scheduled"]
  /\ UNCHANGED <<resourceState,
                 fdState,
                 closeStarted,
                 operationState,
                 cancelState,
                 cancelFailuresRemaining,
                 logicalState,
                 result,
                 storageState,
                 closeContinuationState,
                 submitCount,
                 targetCompletionCount,
                 resumeCount,
                 cancelSubmitAttempts>>

ResumeOperation(op) ==
  /\ op \in Operations
  /\ continuationState[op] = "Scheduled"
  /\ continuationState' = [continuationState EXCEPT ![op] = "Resumed"]
  /\ resumeCount' = [resumeCount EXCEPT ![op] = @ + 1]
  /\ UNCHANGED <<resourceState,
                 fdState,
                 closeStarted,
                 operationState,
                 cancelState,
                 cancelFailuresRemaining,
                 logicalState,
                 result,
                 storageState,
                 closeContinuationState,
                 submitCount,
                 targetCompletionCount,
                 cancelSubmitAttempts>>

(* The awaiter/frame dispatch address may disappear only after its scheduled
   continuation has resumed.  It is no longer part of the close barrier. *)
ReleaseOperation(op) ==
  /\ op \in Operations
  /\ operationState[op] = "Terminal"
  /\ continuationState[op] = "Resumed"
  /\ operationState' = [operationState EXCEPT ![op] = "Released"]
  /\ UNCHANGED <<resourceState,
                 fdState,
                 closeStarted,
                 cancelState,
                 cancelFailuresRemaining,
                 logicalState,
                 result,
                 storageState,
                 continuationState,
                 closeContinuationState,
                 submitCount,
                 targetCompletionCount,
                 resumeCount,
                 cancelSubmitAttempts>>

(* All target requests, cancel commands, and backend-held storage have drained.
   Only then may the resource release its fd/channel/ring registration. *)
EnterQuiescent ==
  /\ resourceState = "Closing"
  /\ AllTargetsQuiescent
  /\ resourceState' = "Quiescent"
  /\ UNCHANGED <<fdState,
                 closeStarted,
                 operationState,
                 cancelState,
                 cancelFailuresRemaining,
                 logicalState,
                 result,
                 storageState,
                 continuationState,
                 closeContinuationState,
                 submitCount,
                 targetCompletionCount,
                 resumeCount,
                 cancelSubmitAttempts>>

ReleaseFd ==
  /\ resourceState = "Quiescent"
  /\ fdState = "Open"
  /\ resourceState' = "Closed"
  /\ fdState' = "Released"
  /\ UNCHANGED <<closeStarted,
                 operationState,
                 cancelState,
                 cancelFailuresRemaining,
                 logicalState,
                 result,
                 storageState,
                 continuationState,
                 closeContinuationState,
                 submitCount,
                 targetCompletionCount,
                 resumeCount,
                 cancelSubmitAttempts>>

ScheduleCloseContinuation ==
  /\ resourceState = "Closed"
  /\ closeContinuationState = "Waiting"
  /\ closeContinuationState' = "Scheduled"
  /\ UNCHANGED <<resourceState,
                 fdState,
                 closeStarted,
                 operationState,
                 cancelState,
                 cancelFailuresRemaining,
                 logicalState,
                 result,
                 storageState,
                 continuationState,
                 submitCount,
                 targetCompletionCount,
                 resumeCount,
                 cancelSubmitAttempts>>

ResumeCloseContinuation ==
  /\ closeContinuationState = "Scheduled"
  /\ closeContinuationState' = "Resumed"
  /\ UNCHANGED <<resourceState,
                 fdState,
                 closeStarted,
                 operationState,
                 cancelState,
                 cancelFailuresRemaining,
                 logicalState,
                 result,
                 storageState,
                 continuationState,
                 submitCount,
                 targetCompletionCount,
                 resumeCount,
                 cancelSubmitAttempts>>

AnyRequestCancel == \E op \in Operations: RequestCancel(op)
AnyCancelSubmitFails == \E op \in Operations: CancelSubmitFails(op)
AnyCancelSubmitted == \E op \in Operations: CancelSubmitted(op)
AnyCancelAcknowledged == \E op \in Operations: CancelAcknowledged(op)
AnyTargetComplete == \E op \in Operations: TargetComplete(op)
AnyAuthorizeStorageRelease == \E op \in Operations: AuthorizeStorageRelease(op)
AnyScheduleOperationContinuation == \E op \in Operations: ScheduleOperationContinuation(op)
AnyResumeOperation == \E op \in Operations: ResumeOperation(op)
AnyReleaseOperation == \E op \in Operations: ReleaseOperation(op)

Next ==
  \/ \E op \in Operations: Submit(op)
  \/ BeginClose
  \/ AnyRequestCancel
  \/ AnyCancelSubmitFails
  \/ AnyCancelSubmitted
  \/ AnyCancelAcknowledged
  \/ AnyTargetComplete
  \/ AnyAuthorizeStorageRelease
  \/ AnyScheduleOperationContinuation
  \/ AnyResumeOperation
  \/ AnyReleaseOperation
  \/ EnterQuiescent
  \/ ReleaseFd
  \/ ScheduleCloseContinuation
  \/ ResumeCloseContinuation

(***************************************************************************)
(* Liveness is conditional: the owner loop continues to run, submitted      *)
(* target and cancel requests eventually get a CQE, and the injected local  *)
(* cancel-submit failure is transient.  This is not a claim about a stopped  *)
(* worker or an unfair kernel scheduler.                                   *)
(***************************************************************************)
Fairness ==
  /\ WF_vars(AnyRequestCancel)
  /\ WF_vars(AnyCancelSubmitFails)
  /\ WF_vars(AnyCancelSubmitted)
  /\ WF_vars(AnyCancelAcknowledged)
  /\ WF_vars(AnyTargetComplete)
  /\ WF_vars(AnyAuthorizeStorageRelease)
  /\ WF_vars(AnyScheduleOperationContinuation)
  /\ WF_vars(AnyResumeOperation)
  /\ WF_vars(AnyReleaseOperation)
  /\ WF_vars(EnterQuiescent)
  /\ WF_vars(ReleaseFd)
  /\ WF_vars(ScheduleCloseContinuation)
  /\ WF_vars(ResumeCloseContinuation)

Spec == Init /\ [][Next]_vars /\ Fairness

TypeOK ==
  /\ resourceState \in ResourceStates
  /\ fdState \in FdStates
  /\ closeStarted \in BOOLEAN
  /\ operationState \in [Operations -> OperationStates]
  /\ cancelState \in [Operations -> CancelStates]
  /\ cancelFailuresRemaining \in [Operations -> Nat]
  /\ logicalState \in [Operations -> LogicalStates]
  /\ result \in [Operations -> Results]
  /\ storageState \in [Operations -> StorageStates]
  /\ continuationState \in [Operations -> ContinuationStates]
  /\ closeContinuationState \in ContinuationStates
  /\ submitCount \in [Operations -> Nat]
  /\ targetCompletionCount \in [Operations -> Nat]
  /\ resumeCount \in [Operations -> Nat]
  /\ cancelSubmitAttempts \in [Operations -> Nat]

OneSubmissionPerOperation == \A op \in Operations: submitCount[op] <= 1
OneTargetCompletionPerOperation == \A op \in Operations: targetCompletionCount[op] <= 1
OneResumePerOperation == \A op \in Operations: resumeCount[op] <= 1

(* Cancel acknowledgement is not evidence that the original target finished. *)
CancelAckDoesNotSettleTarget ==
  \A op \in Operations:
    /\ cancelState[op] = "Acknowledged"
    /\ operationState[op] = "Pending"
    => /\ logicalState[op] = "Pending"
       /\ result[op] = "NoResult"
       /\ targetCompletionCount[op] = 0

(* A borrowed/owned buffer remains unavailable while a target request exists. *)
PendingRetainsStorage ==
  \A op \in Operations:
    operationState[op] = "Pending" => storageState[op] = "Retained"

(* No coroutine can observe the result before backend storage release is safe. *)
ResumeRequiresStorageRelease ==
  \A op \in Operations:
    continuationState[op] \in {"Scheduled", "Resumed"}
      => /\ logicalState[op] = "Ready"
         /\ storageState[op] = "ReleaseAuthorized"

(* An awaiter dispatch address can disappear only after its continuation ran. *)
OperationReleaseRequiresResume ==
  \A op \in Operations:
    operationState[op] = "Released" => continuationState[op] = "Resumed"

(* BeginClose is irreversible, including the cancel-submit failure branch. *)
CloseNeverReopens == closeStarted => resourceState \in {"Closing", "Quiescent", "Closed"}

(* fd/channel/ring release is legal only after every target and cancel command
   is drained and every backend-visible storage reference is released. *)
FdReleaseRequiresQuiescence ==
  fdState = "Released" => AllTargetsQuiescent

CloseContinuationRequiresFdRelease ==
  closeContinuationState \in {"Scheduled", "Resumed"} => fdState = "Released"

(* Under Fairness, Close reaches a caller-visible terminal result. *)
CloseEventuallyReturns ==
  closeStarted ~> closeContinuationState = "Resumed"

(* Any submitted operation eventually reaches one logical terminal result. *)
OperationEventuallySettles(op) ==
  operationState[op] = "Pending" ~> logicalState[op] = "Ready"

AllOperationsEventuallySettle ==
  /\ OperationEventuallySettles("Read")
  /\ OperationEventuallySettles("Write")

=============================================================================
