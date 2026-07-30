----------------------- MODULE send_zc_split_release_refinement -----------------------

EXTENDS Naturals, Sequences, FiniteSets

(***************************************************************************)
(* LUringStream::SendZeroCopy 的具体 split-release refinement。           *)
(*                                                                         *)
(* primary CQE 决定业务结果；F_NOTIF 是内核不再访问 send buffer 的边界。   *)
(* 两者的到达顺序在模型中故意不固定。只有两个边界都已观察后，             *)
(* SplitReleaseLifecycle 才授权释放 buffer，并将 ResumeWork 交给           *)
(* LUringLoop::ScheduleCompletion。                                        *)
(*                                                                         *)
(* trace 记录的是该协议的可观察生命周期，不把 SQE 提交与普通业务写入      *)
(* 混为一个 single-shot Complete。                                         *)
(***************************************************************************)

RequestStates == {"Idle", "SQEQueued", "Submitted", "Terminal", "Released"}
ResultStates == {"NoResult", "Success", "Cancelled"}
BufferStates == {"CallerOwned", "KernelMayAccess", "Reusable"}
ContinuationStates == {"Running", "Waiting", "Queued", "Resumed"}

VARIABLES requestState,
          resultState,
          bufferState,
          continuationState,
          cancelRequested,
          primaryCount,
          notificationCount,
          releaseCount,
          resumeCount,
          trace

vars == <<requestState,
          resultState,
          bufferState,
          continuationState,
          cancelRequested,
          primaryCount,
          notificationCount,
          releaseCount,
          resumeCount,
          trace>>

TraceEvents == {"Submit", "Primary", "Notification", "Release", "Schedule", "Resume"}
Occurrences(event) == Cardinality({i \in 1..Len(trace): trace[i] = event})

Init ==
  /\ requestState = "Idle"
  /\ resultState = "NoResult"
  /\ bufferState = "CallerOwned"
  /\ continuationState = "Running"
  /\ cancelRequested = FALSE
  /\ primaryCount = 0
  /\ notificationCount = 0
  /\ releaseCount = 0
  /\ resumeCount = 0
  /\ trace = <<>>

(* await_suspend prepares the send-zc SQE and transfers buffer access to the kernel. *)
PrepareSend ==
  /\ requestState = "Idle"
  /\ continuationState = "Running"
  /\ requestState' = "SQEQueued"
  /\ bufferState' = "KernelMayAccess"
  /\ continuationState' = "Waiting"
  /\ trace' = Append(trace, "Submit")
  /\ UNCHANGED <<resultState,
                 cancelRequested,
                 primaryCount,
                 notificationCount,
                 releaseCount,
                 resumeCount>>

(* FlushSubmit makes the request visible to the kernel; it is not a trace event. *)
SubmitToKernel ==
  /\ requestState = "SQEQueued"
  /\ requestState' = "Submitted"
  /\ UNCHANGED <<resultState,
                 bufferState,
                 continuationState,
                 cancelRequested,
                 primaryCount,
                 notificationCount,
                 releaseCount,
                 resumeCount,
                 trace>>

(* An async cancel request does not replace either target CQE. *)
RequestCancel ==
  /\ requestState \in {"SQEQueued", "Submitted", "Terminal"}
  /\ cancelRequested = FALSE
  /\ cancelRequested' = TRUE
  /\ UNCHANGED <<requestState,
                 resultState,
                 bufferState,
                 continuationState,
                 primaryCount,
                 notificationCount,
                 releaseCount,
                 resumeCount,
                 trace>>

(*
 * The first non-notification CQE records the caller-visible result. The
 * model permits it after F_NOTIF to verify that release remains gated by both
 * facts rather than by a presumed CQE ordering.
 *)
PrimaryCQE ==
  /\ requestState \in {"Submitted", "Terminal"}
  /\ primaryCount = 0
  /\ primaryCount' = 1
  /\ resultState' = IF cancelRequested THEN "Cancelled" ELSE "Success"
  /\ trace' = Append(trace, "Primary")
  /\ UNCHANGED <<requestState,
                 bufferState,
                 continuationState,
                 cancelRequested,
                 notificationCount,
                 releaseCount,
                 resumeCount>>

(* F_NOTIF is the target request's physical terminal CQE. *)
NotificationCQE ==
  /\ requestState = "Submitted"
  /\ notificationCount = 0
  /\ requestState' = "Terminal"
  /\ notificationCount' = 1
  /\ trace' = Append(trace, "Notification")
  /\ UNCHANGED <<resultState,
                 bufferState,
                 continuationState,
                 cancelRequested,
                 primaryCount,
                 releaseCount,
                 resumeCount>>

(* TryAuthorizeRelease clears pending_write_ and makes the buffer reusable. *)
AuthorizeRelease ==
  /\ requestState = "Terminal"
  /\ primaryCount = 1
  /\ notificationCount = 1
  /\ releaseCount = 0
  /\ requestState' = "Released"
  /\ bufferState' = "Reusable"
  /\ releaseCount' = 1
  /\ trace' = Append(trace, "Release")
  /\ UNCHANGED <<resultState,
                 continuationState,
                 cancelRequested,
                 primaryCount,
                 notificationCount,
                 resumeCount>>

(* LUringLoop accepts the authorized ResumeWork into completion_ready_. *)
ScheduleContinuation ==
  /\ requestState = "Released"
  /\ continuationState = "Waiting"
  /\ releaseCount = 1
  /\ continuationState' = "Queued"
  /\ trace' = Append(trace, "Schedule")
  /\ UNCHANGED <<requestState,
                 resultState,
                 bufferState,
                 cancelRequested,
                 primaryCount,
                 notificationCount,
                 releaseCount,
                 resumeCount>>

(* The scheduler liveness model separately proves this queued work runs. *)
Resume ==
  /\ continuationState = "Queued"
  /\ resumeCount = 0
  /\ continuationState' = "Resumed"
  /\ resumeCount' = 1
  /\ trace' = Append(trace, "Resume")
  /\ UNCHANGED <<requestState,
                 resultState,
                 bufferState,
                 cancelRequested,
                 primaryCount,
                 notificationCount,
                 releaseCount>>

Next ==
  \/ PrepareSend
  \/ SubmitToKernel
  \/ RequestCancel
  \/ PrimaryCQE
  \/ NotificationCQE
  \/ AuthorizeRelease
  \/ ScheduleContinuation
  \/ Resume

Spec == Init /\ [][Next]_vars

TypeOK ==
  /\ requestState \in RequestStates
  /\ resultState \in ResultStates
  /\ bufferState \in BufferStates
  /\ continuationState \in ContinuationStates
  /\ cancelRequested \in BOOLEAN
  /\ primaryCount \in Nat
  /\ notificationCount \in Nat
  /\ releaseCount \in Nat
  /\ resumeCount \in Nat
  /\ trace \in Seq(TraceEvents)

ExactlyOnceBoundaries ==
  /\ primaryCount <= 1
  /\ notificationCount <= 1
  /\ releaseCount <= 1
  /\ resumeCount <= 1

ReleaseAuthorization ==
  releaseCount = 1
    => /\ primaryCount = 1
       /\ notificationCount = 1
       /\ requestState = "Released"
       /\ bufferState = "Reusable"

BufferSafety ==
  bufferState = "Reusable" => notificationCount = 1

ResumeAuthorization ==
  continuationState = "Resumed"
    => /\ releaseCount = 1
       /\ bufferState = "Reusable"

TraceCountMatchesState ==
  /\ Occurrences("Primary") = primaryCount
  /\ Occurrences("Notification") = notificationCount
  /\ Occurrences("Release") = releaseCount
  /\ Occurrences("Resume") = resumeCount

TraceReleaseAfterBothCqes ==
  \A i \in 1..Len(trace):
    trace[i] = "Release"
      => /\ \E primary \in 1..(i - 1): trace[primary] = "Primary"
         /\ \E notification \in 1..(i - 1): trace[notification] = "Notification"

TraceResumeAfterRelease ==
  \A i \in 1..Len(trace):
    trace[i] = "Resume"
      => \E release \in 1..(i - 1): trace[release] = "Release"

========================================================================================
