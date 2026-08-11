----------------------- MODULE stream_shutdown_transaction -----------------------

EXTENDS Naturals

(***************************************************************************)
(* Stream write-half shutdown transaction.                                *)
(*                                                                         *)
(* StreamLifecycle::PrepareShutdown() temporarily excludes new writes,     *)
(* then the adapter performs a synchronous shutdown(2).  A successful      *)
(* syscall commits the write direction to Shutdown; a local error rolls     *)
(* it back to Writable so the caller may retry.                             *)
(*                                                                         *)
(* This model intentionally omits pending physical write requests: the     *)
(* production API rejects those with EBUSY before PrepareShutdown(), and    *)
(* async request/cancel convergence is modeled by resource_close_cancel.   *)
(***************************************************************************)

ResourceStates == {"Open", "Closing", "Closed"}
WriteStates == {"Writable", "ShutdownPreparing", "Shutdown"}

(* TLC checks a bounded instance: two ordinary writes and two explicit
   shutdown attempts are enough to cover success, local rollback, and retry. *)
MaxWriteSubmissions == 2
MaxShutdownAttempts == 2
MaxCloseRejections == 1

VARIABLES resourceState,
          writeState,
          shutdownPrepareCount,
          shutdownCommitCount,
          shutdownAbortCount,
          writeSubmitCount,
          writesAtShutdownPreparation,
          closeRejectCount

vars == <<resourceState,
          writeState,
          shutdownPrepareCount,
          shutdownCommitCount,
          shutdownAbortCount,
          writeSubmitCount,
          writesAtShutdownPreparation,
          closeRejectCount>>

Init ==
  /\ resourceState = "Open"
  /\ writeState = "Writable"
  /\ shutdownPrepareCount = 0
  /\ shutdownCommitCount = 0
  /\ shutdownAbortCount = 0
  /\ writeSubmitCount = 0
  /\ writesAtShutdownPreparation = 0
  /\ closeRejectCount = 0

(* A normal write is admitted only while the write direction is fully open. *)
SubmitWrite ==
  /\ resourceState = "Open"
  /\ writeState = "Writable"
  /\ writeSubmitCount < MaxWriteSubmissions
  /\ writeSubmitCount' = writeSubmitCount + 1
  /\ UNCHANGED <<resourceState,
                 writeState,
                 shutdownPrepareCount,
                 shutdownCommitCount,
                 shutdownAbortCount,
                 writesAtShutdownPreparation,
                 closeRejectCount>>

(* This is the internal preparation transition after the public pending-write
   check has passed.  No physical syscall has happened yet. *)
PrepareShutdown ==
  /\ resourceState = "Open"
  /\ writeState = "Writable"
  /\ shutdownPrepareCount < MaxShutdownAttempts
  /\ writeState' = "ShutdownPreparing"
  /\ shutdownPrepareCount' = shutdownPrepareCount + 1
  /\ writesAtShutdownPreparation' = writeSubmitCount
  /\ UNCHANGED <<resourceState,
                 shutdownCommitCount,
                 shutdownAbortCount,
                 writeSubmitCount,
                 closeRejectCount>>

(* shutdown(2) succeeded, so the write direction becomes irreversibly shut
   down while the stream resource remains readable and Open. *)
CommitShutdown ==
  /\ resourceState = "Open"
  /\ writeState = "ShutdownPreparing"
  /\ writeState' = "Shutdown"
  /\ shutdownCommitCount' = shutdownCommitCount + 1
  /\ UNCHANGED <<resourceState,
                 shutdownPrepareCount,
                 shutdownAbortCount,
                 writeSubmitCount,
                 writesAtShutdownPreparation,
                 closeRejectCount>>

(* A local shutdown(2) error does not close the stream and must restore the
   pre-syscall write state.  A retry is a fresh PrepareShutdown action. *)
AbortShutdownPreparation ==
  /\ resourceState = "Open"
  /\ writeState = "ShutdownPreparing"
  /\ writeState' = "Writable"
  /\ shutdownAbortCount' = shutdownAbortCount + 1
  /\ UNCHANGED <<resourceState,
                 shutdownPrepareCount,
                 shutdownCommitCount,
                 writeSubmitCount,
                 writesAtShutdownPreparation,
                 closeRejectCount>>

(* Close must not steal a partially prepared Shutdown transaction.  The API
   returns EBUSY and leaves both lifecycle dimensions untouched. *)
RejectCloseDuringShutdownPreparation ==
  /\ resourceState = "Open"
  /\ writeState = "ShutdownPreparing"
  /\ closeRejectCount < MaxCloseRejections
  /\ closeRejectCount' = closeRejectCount + 1
  /\ UNCHANGED <<resourceState,
                 writeState,
                 shutdownPrepareCount,
                 shutdownCommitCount,
                 shutdownAbortCount,
                 writeSubmitCount,
                 writesAtShutdownPreparation>>

PrepareClose ==
  /\ resourceState = "Open"
  /\ writeState # "ShutdownPreparing"
  /\ resourceState' = "Closing"
  /\ UNCHANGED <<writeState,
                 shutdownPrepareCount,
                 shutdownCommitCount,
                 shutdownAbortCount,
                 writeSubmitCount,
                 writesAtShutdownPreparation,
                 closeRejectCount>>

CommitClose ==
  /\ resourceState = "Closing"
  /\ resourceState' = "Closed"
  /\ UNCHANGED <<writeState,
                 shutdownPrepareCount,
                 shutdownCommitCount,
                 shutdownAbortCount,
                 writeSubmitCount,
                 writesAtShutdownPreparation,
                 closeRejectCount>>

Next ==
  \/ SubmitWrite
  \/ PrepareShutdown
  \/ CommitShutdown
  \/ AbortShutdownPreparation
  \/ RejectCloseDuringShutdownPreparation
  \/ PrepareClose
  \/ CommitClose

Spec == Init /\ [][Next]_vars

TypeOK ==
  /\ resourceState \in ResourceStates
  /\ writeState \in WriteStates
  /\ shutdownPrepareCount \in Nat
  /\ shutdownCommitCount \in Nat
  /\ shutdownAbortCount \in Nat
  /\ writeSubmitCount \in Nat
  /\ writesAtShutdownPreparation \in Nat
  /\ closeRejectCount \in Nat

(* Every preparation settles exactly once, unless it is the currently active
   synchronous syscall transaction. *)
PreparationAccounting ==
  shutdownPrepareCount = shutdownCommitCount + shutdownAbortCount +
    (IF writeState = "ShutdownPreparing" THEN 1 ELSE 0)

(* No new write may enter after the shutdown preparation takes its snapshot. *)
NoWriteAfterPreparation ==
  writeState \in {"ShutdownPreparing", "Shutdown"}
    => writeSubmitCount = writesAtShutdownPreparation

(* Closing and write-shutdown preparation are mutually exclusive internal
   transactions. *)
CloseDoesNotOverlapShutdownPreparation ==
  writeState = "ShutdownPreparing" => resourceState = "Open"

(* The committed write-half shutdown is terminal until resource Close. *)
ShutdownCommitIsTerminal ==
  writeState = "Shutdown" => shutdownCommitCount = 1

CloseDoesNotReopen ==
  resourceState \in {"Closing", "Closed"} => writeState # "ShutdownPreparing"

========================================================================================
