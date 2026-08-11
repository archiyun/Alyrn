------------------- MODULE async_operation_lifecycle_shapes -------------------

EXTENDS Naturals

CONSTANT MaxEvents

(***************************************************************************)
(* CoroPact Logical Operation 的正交 lifecycle shape 模型。                *)
(*                                                                         *)
(* 本模块不把 single-result、composite、event-source 和 split-release     *)
(* 建模成互斥 family。每个 operation 由三个独立维度描述：                 *)
(*                                                                         *)
(*   resultCardinality \in {"Single", "Multiple"}                        *)
(*   convergence       \in {"Single", "Composite"}                       *)
(*   releaseCoupling   \in {"Coupled", "Split"}                          *)
(*                                                                         *)
(* 因而 Multiple + Single + Split（例如 multishot recv + BufferLease）    *)
(* 与 Single + Composite + Split 都是合法 shape。                         *)
(*                                                                         *)
(* 该模型验证有界协议形状，不代替各 backend/source 的具体 refinement。   *)
(***************************************************************************)

ResultCardinalities == {"Single", "Multiple"}
ConvergenceModes    == {"Single", "Composite"}
ReleaseCouplings    == {"Coupled", "Split"}
ExecutionStates     == {"Idle", "Active", "Terminal", "Cancelled", "Released"}

VARIABLES resultCardinality,
          convergence,
          releaseCoupling,
          executionState,
          physicalRequestCount,
          submittedRequests,
          terminalRequests,
          backendEventCount,
          resultReady,
          eventCount,
          consumedEvents,
          logicalTerminalCount,
          cancelRequested,
          releaseCount,
          resumeCount

vars == <<resultCardinality,
          convergence,
          releaseCoupling,
          executionState,
          physicalRequestCount,
          submittedRequests,
          terminalRequests,
          backendEventCount,
          resultReady,
          eventCount,
          consumedEvents,
          logicalTerminalCount,
          cancelRequested,
          releaseCount,
          resumeCount>>

Init ==
  /\ MaxEvents \in Nat
  /\ MaxEvents > 0
  /\ resultCardinality \in ResultCardinalities
  /\ convergence \in ConvergenceModes
  /\ releaseCoupling \in ReleaseCouplings
  /\ executionState = "Idle"
  /\ physicalRequestCount = IF convergence = "Composite" THEN 2 ELSE 1
  /\ submittedRequests = 0
  /\ terminalRequests = 0
  /\ backendEventCount = 0
  /\ resultReady = FALSE
  /\ eventCount = 0
  /\ consumedEvents = 0
  /\ logicalTerminalCount = 0
  /\ cancelRequested = FALSE
  /\ releaseCount = 0
  /\ resumeCount = 0

Submit ==
  /\ executionState = "Idle"
  /\ submittedRequests = 0
  /\ executionState' = "Active"
  /\ submittedRequests' = physicalRequestCount
  /\ UNCHANGED <<resultCardinality,
                 convergence,
                 releaseCoupling,
                 physicalRequestCount,
                 terminalRequests,
                 backendEventCount,
                 resultReady,
                 eventCount,
                 consumedEvents,
                 logicalTerminalCount,
                 cancelRequested,
                 releaseCount,
                 resumeCount>>

(* Multiple-result executions may publish events before physical terminal. *)
ProduceEvent ==
  /\ resultCardinality = "Multiple"
  /\ executionState = "Active"
  /\ cancelRequested = FALSE
  /\ eventCount < MaxEvents
  /\ eventCount' = eventCount + 1
  /\ backendEventCount' = backendEventCount + 1
  /\ UNCHANGED <<resultCardinality,
                 convergence,
                 releaseCoupling,
                 executionState,
                 physicalRequestCount,
                 submittedRequests,
                 terminalRequests,
                 resultReady,
                 consumedEvents,
                 logicalTerminalCount,
                 cancelRequested,
                 releaseCount,
                 resumeCount>>

(* A single result may precede physical terminal only for split release. *)
RecordSingleResult ==
  /\ resultCardinality = "Single"
  /\ executionState \in {"Active", "Terminal", "Cancelled"}
  /\ resultReady = FALSE
  /\ releaseCoupling = "Split" \/ terminalRequests = physicalRequestCount
  /\ resultReady' = TRUE
  /\ backendEventCount' = backendEventCount + 1
  /\ UNCHANGED <<resultCardinality,
                 convergence,
                 releaseCoupling,
                 executionState,
                 physicalRequestCount,
                 submittedRequests,
                 terminalRequests,
                 eventCount,
                 consumedEvents,
                 logicalTerminalCount,
                 cancelRequested,
                 releaseCount,
                 resumeCount>>

RequestCancel ==
  /\ executionState = "Active"
  /\ cancelRequested = FALSE
  /\ cancelRequested' = TRUE
  /\ UNCHANGED <<resultCardinality,
                 convergence,
                 releaseCoupling,
                 executionState,
                 physicalRequestCount,
                 submittedRequests,
                 terminalRequests,
                 backendEventCount,
                 resultReady,
                 eventCount,
                 consumedEvents,
                 logicalTerminalCount,
                 releaseCount,
                 resumeCount>>

(* Each physical request reaches terminal independently. The last one ends *)
(* the backend execution but does not itself authorize release.             *)
CompletePhysicalRequest ==
  /\ executionState = "Active"
  /\ terminalRequests < physicalRequestCount
  /\ terminalRequests' = terminalRequests + 1
  /\ backendEventCount' = backendEventCount + 1
  /\ executionState' =
       IF terminalRequests + 1 = physicalRequestCount
       THEN IF cancelRequested THEN "Cancelled" ELSE "Terminal"
       ELSE "Active"
  /\ logicalTerminalCount' =
       IF terminalRequests + 1 = physicalRequestCount
       THEN 1
       ELSE logicalTerminalCount
  /\ resultReady' =
       IF terminalRequests + 1 = physicalRequestCount
          /\ resultCardinality = "Single"
          /\ releaseCoupling = "Coupled"
       THEN TRUE
       ELSE resultReady
  /\ UNCHANGED <<resultCardinality,
                 convergence,
                 releaseCoupling,
                 physicalRequestCount,
                 submittedRequests,
                 eventCount,
                 consumedEvents,
                 cancelRequested,
                 releaseCount,
                 resumeCount>>

(* Events already published by a source remain consumable after terminal. *)
ConsumeEvent ==
  /\ resultCardinality = "Multiple"
  /\ consumedEvents < eventCount
  /\ consumedEvents' = consumedEvents + 1
  /\ UNCHANGED <<resultCardinality,
                 convergence,
                 releaseCoupling,
                 executionState,
                 physicalRequestCount,
                 submittedRequests,
                 terminalRequests,
                 backendEventCount,
                 resultReady,
                 eventCount,
                 logicalTerminalCount,
                 cancelRequested,
                 releaseCount,
                 resumeCount>>

(* Release requires logical and physical convergence. Split means these    *)
(* facts may become true at different events, not that either can be lost. *)
AuthorizeRelease ==
  /\ executionState \in {"Terminal", "Cancelled"}
  /\ terminalRequests = physicalRequestCount
  /\ logicalTerminalCount = 1
  /\ IF resultCardinality = "Single"
        THEN resultReady
        ELSE consumedEvents = eventCount
  /\ releaseCount = 0
  /\ releaseCount' = 1
  /\ executionState' = "Released"
  /\ UNCHANGED <<resultCardinality,
                 convergence,
                 releaseCoupling,
                 physicalRequestCount,
                 submittedRequests,
                 terminalRequests,
                 backendEventCount,
                 resultReady,
                 eventCount,
                 consumedEvents,
                 logicalTerminalCount,
                 cancelRequested,
                 resumeCount>>

(* This model uses the borrowed-buffer-safe policy: a single-result waiter  *)
(* resumes only after release has been authorized. Source Next() resumption *)
(* remains covered by the dedicated source/lease models.                    *)
AuthorizeContinuation ==
  /\ resultCardinality = "Single"
  /\ resultReady
  /\ releaseCount = 1
  /\ resumeCount = 0
  /\ resumeCount' = 1
  /\ UNCHANGED <<resultCardinality,
                 convergence,
                 releaseCoupling,
                 executionState,
                 physicalRequestCount,
                 submittedRequests,
                 terminalRequests,
                 backendEventCount,
                 resultReady,
                 eventCount,
                 consumedEvents,
                 logicalTerminalCount,
                 cancelRequested,
                 releaseCount>>

Next ==
  \/ Submit
  \/ ProduceEvent
  \/ RecordSingleResult
  \/ RequestCancel
  \/ CompletePhysicalRequest
  \/ ConsumeEvent
  \/ AuthorizeRelease
  \/ AuthorizeContinuation

Spec == Init /\ [][Next]_vars

TypeOK ==
  /\ resultCardinality \in ResultCardinalities
  /\ convergence \in ConvergenceModes
  /\ releaseCoupling \in ReleaseCouplings
  /\ executionState \in ExecutionStates
  /\ physicalRequestCount \in {1, 2}
  /\ submittedRequests \in Nat
  /\ terminalRequests \in Nat
  /\ backendEventCount \in Nat
  /\ resultReady \in BOOLEAN
  /\ eventCount \in Nat
  /\ consumedEvents \in Nat
  /\ logicalTerminalCount \in Nat
  /\ cancelRequested \in BOOLEAN
  /\ releaseCount \in Nat
  /\ resumeCount \in Nat
  /\ eventCount <= MaxEvents
  /\ backendEventCount <= MaxEvents + physicalRequestCount + 1

ShapeIsOrthogonal ==
  /\ (convergence = "Single" <=> physicalRequestCount = 1)
  /\ (convergence = "Composite" <=> physicalRequestCount = 2)
  /\ (resultCardinality = "Single" => eventCount = 0)
  /\ (resultCardinality = "Multiple" => ~resultReady)

PhysicalRequestBounds ==
  /\ submittedRequests <= physicalRequestCount
  /\ terminalRequests <= submittedRequests

LogicalTerminalOnce == logicalTerminalCount <= 1
ConsumedEventsBound == consumedEvents <= eventCount

ReleaseAuthorization ==
  releaseCount = 1
    => /\ executionState = "Released"
       /\ terminalRequests = physicalRequestCount
       /\ logicalTerminalCount = 1
       /\ IF resultCardinality = "Single"
             THEN resultReady
             ELSE consumedEvents = eventCount

ContinuationAuthorization ==
  resumeCount = 1
    => /\ resultCardinality = "Single"
       /\ resultReady
       /\ releaseCount = 1

OnceOnlyAuthorization ==
  /\ releaseCount <= 1
  /\ resumeCount <= 1

TerminalState ==
  logicalTerminalCount = 1
    => executionState \in {"Terminal", "Cancelled", "Released"}

=============================================================================
