----------------------- MODULE send_zc_split_release_refinement -----------------------

EXTENDS Naturals, Sequences, FiniteSets

(***************************************************************************)
(* Stream::SendZeroCopy 的具体 split-release refinement。           *)
(*                                                                         *)
(* primary CQE 决定业务结果。带 F_MORE 的 primary 承诺后续 F_NOTIF；      *)
(* 无 F_MORE 的 primary 本身就是 kernel 不再访问 send buffer 的边界。     *)
(* 只有逻辑结果与相应的物理终态都已观察后，SplitReleaseLifecycle 才授权    *)
(* 释放 buffer，并将 ResumeWork 交给 Loop::ScheduleCompletion。      *)
(*                                                                         *)
(* trace 记录的是该协议的可观察生命周期，不把 SQE 提交与普通业务写入      *)
(* 混为一个 single-shot Complete。                                         *)
(***************************************************************************)

RequestStates == {"Idle", "SQEQueued", "Submitted", "Terminal", "Released"}
ResultStates == {"NoResult", "Success", "Cancelled"}
BufferStates == {"CallerOwned", "KernelMayAccess", "Reusable"}
ContinuationStates == {"Running", "Waiting", "Queued", "Resumed"}
PrimaryModes == {"Unknown", "Terminal", "Notification"}

VARIABLES requestState,
          resultState,
          bufferState,
          continuationState,
          cancelRequested,
          primaryCount,
          primaryMode,
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
          primaryMode,
          notificationCount,
          releaseCount,
          resumeCount,
          trace>>

TraceEvents == {"Submit", "PrimaryTerminal", "PrimaryWithNotification", "Notification",
                "Release", "Schedule", "Resume"}
Occurrences(event) == Cardinality({i \in 1..Len(trace): trace[i] = event})

Init ==
  /\ requestState = "Idle"
  /\ resultState = "NoResult"
  /\ bufferState = "CallerOwned"
  /\ continuationState = "Running"
  /\ cancelRequested = FALSE
  /\ primaryCount = 0
  /\ primaryMode = "Unknown"
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
                 primaryMode,
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
                 primaryMode,
                 notificationCount,
                 releaseCount,
                 resumeCount,
                 trace>>

(* An async cancel request does not replace the target CQE. *)
RequestCancel ==
  /\ requestState \in {"SQEQueued", "Submitted", "Terminal"}
  /\ cancelRequested = FALSE
  /\ cancelRequested' = TRUE
  /\ UNCHANGED <<requestState,
                 resultState,
                 bufferState,
                 continuationState,
                 primaryCount,
                 primaryMode,
                 notificationCount,
                 releaseCount,
                 resumeCount,
                 trace>>

(* A primary CQE without F_MORE is the request's physical terminal event. *)
PrimaryTerminalCQE ==
  /\ requestState = "Submitted"
  /\ primaryCount = 0
  /\ requestState' = "Terminal"
  /\ resultState' = IF cancelRequested THEN "Cancelled" ELSE "Success"
  /\ primaryCount' = 1
  /\ primaryMode' = "Terminal"
  /\ trace' = Append(trace, "PrimaryTerminal")
  /\ UNCHANGED <<bufferState,
                 continuationState,
                 cancelRequested,
                 notificationCount,
                 releaseCount,
                 resumeCount>>

(*
 * A primary CQE with F_MORE records the result but leaves the request active.
 * The model also permits it after F_NOTIF, so the two CQE orders exercise the
 * same release gate without assuming a dispatch order.
 *)
PrimaryWithNotificationCQE ==
  /\ requestState \in {"Submitted", "Terminal"}
  /\ primaryCount = 0
  /\ resultState' = IF cancelRequested THEN "Cancelled" ELSE "Success"
  /\ primaryCount' = 1
  /\ primaryMode' = "Notification"
  /\ trace' = Append(trace, "PrimaryWithNotification")
  /\ UNCHANGED <<requestState,
                 bufferState,
                 continuationState,
                 cancelRequested,
                 notificationCount,
                 releaseCount,
                 resumeCount>>

(* F_NOTIF is the physical terminal CQE promised by primary F_MORE. *)
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
                 primaryMode,
                 releaseCount,
                 resumeCount>>

(* TryAuthorizeRelease clears pending_write_ and makes the buffer reusable. *)
AuthorizeRelease ==
  /\ requestState = "Terminal"
  /\ primaryCount = 1
  /\ (primaryMode = "Terminal" \/ notificationCount = 1)
  /\ releaseCount = 0
  /\ requestState' = "Released"
  /\ bufferState' = "Reusable"
  /\ releaseCount' = 1
  /\ trace' = Append(trace, "Release")
  /\ UNCHANGED <<resultState,
                 continuationState,
                 cancelRequested,
                 primaryCount,
                 primaryMode,
                 notificationCount,
                 resumeCount>>

(* Loop accepts the authorized ResumeWork into completion_ready_. *)
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
                 primaryMode,
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
                 primaryMode,
                 notificationCount,
                 releaseCount>>

Next ==
  \/ PrepareSend
  \/ SubmitToKernel
  \/ RequestCancel
  \/ PrimaryTerminalCQE
  \/ PrimaryWithNotificationCQE
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
  /\ primaryMode \in PrimaryModes
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
       /\ requestState = "Released"
       /\ bufferState = "Reusable"
       /\ (primaryMode = "Terminal" \/ notificationCount = 1)

BufferSafety ==
  bufferState = "Reusable"
    => /\ primaryCount = 1
       /\ requestState = "Released"
       /\ (primaryMode = "Terminal" \/ notificationCount = 1)

NotificationRequiresFMore ==
  (notificationCount = 1 /\ primaryCount = 1) => primaryMode = "Notification"

TerminalPrimaryHasNoNotification ==
  primaryMode = "Terminal" => notificationCount = 0

ResumeAuthorization ==
  continuationState = "Resumed"
    => /\ releaseCount = 1
       /\ bufferState = "Reusable"

TraceCountMatchesState ==
  /\ Occurrences("PrimaryTerminal") + Occurrences("PrimaryWithNotification") = primaryCount
  /\ Occurrences("Notification") = notificationCount
  /\ Occurrences("Release") = releaseCount
  /\ Occurrences("Resume") = resumeCount

TraceReleaseAfterPrimary ==
  \A i \in 1..Len(trace):
    trace[i] = "Release"
      => \E primary \in 1..(i - 1):
           trace[primary] \in {"PrimaryTerminal", "PrimaryWithNotification"}

TraceReleaseAfterNotificationWhenRequired ==
  \A i \in 1..Len(trace):
    trace[i] = "Release" /\ primaryMode = "Notification"
      => \E notification \in 1..(i - 1): trace[notification] = "Notification"

TraceResumeAfterRelease ==
  \A i \in 1..Len(trace):
    trace[i] = "Resume"
      => \E release \in 1..(i - 1): trace[release] = "Release"

========================================================================================
