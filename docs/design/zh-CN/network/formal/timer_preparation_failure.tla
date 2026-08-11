----------------------- MODULE timer_preparation_failure -----------------------

EXTENDS Naturals

(***************************************************************************)
(* LUringTimerQueue 的逻辑 timer 接受与 physical preparation 模型。     *)
(*                                                                         *)
(* RunAfter() 不能因为 callback 已插入用户态 tree 就报告成功。初次       *)
(* driver 或更早 deadline 的 update 在本地 SQE preparation 失败时，      *)
(* 对应的新 logical timer 必须回滚。已收到 driver CQE 后，若为下一个    *)
(* timer 重臂失败，则没有同步调用者可接收 error；loop 必须进入          *)
(* Stopping，而不是静默遗失 timer。                                      *)
(***************************************************************************)

Timers == {"First", "Earlier", "Later"}

LoopStates        == {"Running", "Stopping", "Stopped"}
TimerStates       == {"Absent", "Accepted", "Fired", "Discarded"}
DriverStates      == {"Idle", "PendingSubmit", "InFlight"}
PreparationStates == {"Idle", "Preparing"}
FailureModes      == {"Initial", "Update", "Rearm", "None"}

VARIABLES loopState,
          failureMode,
          timerState,
          driverState,
          driverTarget,
          initialPreparation,
          updatePreparation,
          rearmPreparation,
          initialAttempted,
          updateAttempted,
          rearmAttempted,
          initialFailureObserved,
          updateFailureObserved,
          rearmFailureObserved,
          driverPreparationCount,
          updatePreparationCount,
          timerAcceptanceCount

vars == <<loopState,
          failureMode,
          timerState,
          driverState,
          driverTarget,
          initialPreparation,
          updatePreparation,
          rearmPreparation,
          initialAttempted,
          updateAttempted,
          rearmAttempted,
          initialFailureObserved,
          updateFailureObserved,
          rearmFailureObserved,
          driverPreparationCount,
          updatePreparationCount,
          timerAcceptanceCount>>

Init ==
  /\ loopState = "Running"
  /\ failureMode \in FailureModes
  /\ timerState = [timer \in Timers |-> "Absent"]
  /\ driverState = "Idle"
  /\ driverTarget = "None"
  /\ initialPreparation = "Idle"
  /\ updatePreparation = "Idle"
  /\ rearmPreparation = "Idle"
  /\ initialAttempted = FALSE
  /\ updateAttempted = FALSE
  /\ rearmAttempted = FALSE
  /\ initialFailureObserved = FALSE
  /\ updateFailureObserved = FALSE
  /\ rearmFailureObserved = FALSE
  /\ driverPreparationCount = 0
  /\ updatePreparationCount = 0
  /\ timerAcceptanceCount = [timer \in Timers |-> 0]

(* First logical timer is only provisional while driver SQE preparation runs. *)
BeginInitialPreparation ==
  /\ loopState = "Running"
  /\ ~initialAttempted
  /\ initialPreparation = "Idle"
  /\ timerState["First"] = "Absent"
  /\ driverState = "Idle"
  /\ initialPreparation' = "Preparing"
  /\ UNCHANGED <<loopState,
                 failureMode,
                 timerState,
                 driverState,
                 driverTarget,
                 updatePreparation,
                 rearmPreparation,
                 initialAttempted,
                 updateAttempted,
                 rearmAttempted,
                 initialFailureObserved,
                 updateFailureObserved,
                 rearmFailureObserved,
                 driverPreparationCount,
                 updatePreparationCount,
                 timerAcceptanceCount>>

InitialPreparationFails ==
  /\ initialPreparation = "Preparing"
  /\ failureMode = "Initial"
  /\ initialPreparation' = "Idle"
  /\ initialAttempted' = TRUE
  /\ initialFailureObserved' = TRUE
  /\ UNCHANGED <<loopState,
                 failureMode,
                 timerState,
                 driverState,
                 driverTarget,
                 updatePreparation,
                 rearmPreparation,
                 updateAttempted,
                 rearmAttempted,
                 updateFailureObserved,
                 rearmFailureObserved,
                 driverPreparationCount,
                 updatePreparationCount,
                 timerAcceptanceCount>>

InitialPreparationSucceeds ==
  /\ initialPreparation = "Preparing"
  /\ failureMode # "Initial"
  /\ initialPreparation' = "Idle"
  /\ initialAttempted' = TRUE
  /\ timerState' = [timerState EXCEPT !["First"] = "Accepted"]
  /\ driverState' = "PendingSubmit"
  /\ driverTarget' = "First"
  /\ driverPreparationCount' = driverPreparationCount + 1
  /\ timerAcceptanceCount' = [timerAcceptanceCount EXCEPT !["First"] = @ + 1]
  /\ UNCHANGED <<loopState,
                 failureMode,
                 updatePreparation,
                 rearmPreparation,
                 updateAttempted,
                 rearmAttempted,
                 initialFailureObserved,
                 updateFailureObserved,
                 rearmFailureObserved,
                 updatePreparationCount>>

FlushDriver ==
  /\ driverState = "PendingSubmit"
  /\ driverState' = "InFlight"
  /\ UNCHANGED <<loopState,
                 failureMode,
                 timerState,
                 driverTarget,
                 initialPreparation,
                 updatePreparation,
                 rearmPreparation,
                 initialAttempted,
                 updateAttempted,
                 rearmAttempted,
                 initialFailureObserved,
                 updateFailureObserved,
                 rearmFailureObserved,
                 driverPreparationCount,
                 updatePreparationCount,
                 timerAcceptanceCount>>

(* A new earlier deadline needs an update physical request.  The old driver
   remains valid while this preparation is provisional. *)
BeginEarlierUpdate ==
  /\ loopState = "Running"
  /\ ~updateAttempted
  /\ updatePreparation = "Idle"
  /\ timerState["First"] = "Accepted"
  /\ timerState["Earlier"] = "Absent"
  /\ timerState["Later"] = "Absent"
  /\ driverState = "InFlight"
  /\ driverTarget = "First"
  /\ updatePreparation' = "Preparing"
  /\ UNCHANGED <<loopState,
                 failureMode,
                 timerState,
                 driverState,
                 driverTarget,
                 initialPreparation,
                 rearmPreparation,
                 initialAttempted,
                 updateAttempted,
                 rearmAttempted,
                 initialFailureObserved,
                 updateFailureObserved,
                 rearmFailureObserved,
                 driverPreparationCount,
                 updatePreparationCount,
                 timerAcceptanceCount>>

EarlierUpdateFails ==
  /\ updatePreparation = "Preparing"
  /\ failureMode = "Update"
  /\ updatePreparation' = "Idle"
  /\ updateAttempted' = TRUE
  /\ updateFailureObserved' = TRUE
  /\ UNCHANGED <<loopState,
                 failureMode,
                 timerState,
                 driverState,
                 driverTarget,
                 initialPreparation,
                 rearmPreparation,
                 initialAttempted,
                 rearmAttempted,
                 initialFailureObserved,
                 rearmFailureObserved,
                 driverPreparationCount,
                 updatePreparationCount,
                 timerAcceptanceCount>>

EarlierUpdateSucceeds ==
  /\ updatePreparation = "Preparing"
  /\ failureMode # "Update"
  /\ updatePreparation' = "Idle"
  /\ updateAttempted' = TRUE
  /\ timerState' = [timerState EXCEPT !["Earlier"] = "Accepted"]
  /\ updatePreparationCount' = updatePreparationCount + 1
  /\ timerAcceptanceCount' = [timerAcceptanceCount EXCEPT !["Earlier"] = @ + 1]
  /\ UNCHANGED <<loopState,
                 failureMode,
                 driverState,
                 driverTarget,
                 initialPreparation,
                 rearmPreparation,
                 initialAttempted,
                 rearmAttempted,
                 initialFailureObserved,
                 updateFailureObserved,
                 rearmFailureObserved,
                 driverPreparationCount>>

(* A later timer is logically accepted without touching the active driver. *)
AcceptLaterTimer ==
  /\ loopState = "Running"
  /\ ~updateAttempted
  /\ timerState["First"] = "Accepted"
  /\ timerState["Earlier"] = "Absent"
  /\ timerState["Later"] = "Absent"
  /\ driverState = "InFlight"
  /\ driverTarget = "First"
  /\ timerState' = [timerState EXCEPT !["Later"] = "Accepted"]
  /\ timerAcceptanceCount' = [timerAcceptanceCount EXCEPT !["Later"] = @ + 1]
  /\ UNCHANGED <<loopState,
                 failureMode,
                 driverState,
                 driverTarget,
                 initialPreparation,
                 updatePreparation,
                 rearmPreparation,
                 initialAttempted,
                 updateAttempted,
                 rearmAttempted,
                 initialFailureObserved,
                 updateFailureObserved,
                 rearmFailureObserved,
                 driverPreparationCount,
                 updatePreparationCount>>

DriverTerminalForFirst ==
  /\ driverState = "InFlight"
  /\ driverTarget = "First"
  /\ timerState["First"] = "Accepted"
  /\ timerState["Earlier"] = "Absent"
  /\ timerState' = [timerState EXCEPT !["First"] = "Fired"]
  /\ driverState' = "Idle"
  /\ driverTarget' = "None"
  /\ UNCHANGED <<loopState,
                 failureMode,
                 initialPreparation,
                 updatePreparation,
                 rearmPreparation,
                 initialAttempted,
                 updateAttempted,
                 rearmAttempted,
                 initialFailureObserved,
                 updateFailureObserved,
                 rearmFailureObserved,
                 driverPreparationCount,
                 updatePreparationCount,
                 timerAcceptanceCount>>

BeginRearm ==
  /\ loopState = "Running"
  /\ ~rearmAttempted
  /\ rearmPreparation = "Idle"
  /\ timerState["First"] = "Fired"
  /\ timerState["Later"] = "Accepted"
  /\ driverState = "Idle"
  /\ rearmPreparation' = "Preparing"
  /\ UNCHANGED <<loopState,
                 failureMode,
                 timerState,
                 driverState,
                 driverTarget,
                 initialPreparation,
                 updatePreparation,
                 initialAttempted,
                 updateAttempted,
                 rearmAttempted,
                 initialFailureObserved,
                 updateFailureObserved,
                 rearmFailureObserved,
                 driverPreparationCount,
                 updatePreparationCount,
                 timerAcceptanceCount>>

RearmPreparationFails ==
  /\ rearmPreparation = "Preparing"
  /\ failureMode = "Rearm"
  /\ loopState' = "Stopping"
  /\ rearmPreparation' = "Idle"
  /\ rearmAttempted' = TRUE
  /\ rearmFailureObserved' = TRUE
  /\ UNCHANGED <<failureMode,
                 timerState,
                 driverState,
                 driverTarget,
                 initialPreparation,
                 updatePreparation,
                 initialAttempted,
                 updateAttempted,
                 initialFailureObserved,
                 updateFailureObserved,
                 driverPreparationCount,
                 updatePreparationCount,
                 timerAcceptanceCount>>

RearmPreparationSucceeds ==
  /\ rearmPreparation = "Preparing"
  /\ failureMode # "Rearm"
  /\ rearmPreparation' = "Idle"
  /\ rearmAttempted' = TRUE
  /\ driverState' = "PendingSubmit"
  /\ driverTarget' = "Later"
  /\ driverPreparationCount' = driverPreparationCount + 1
  /\ UNCHANGED <<loopState,
                 failureMode,
                 timerState,
                 initialPreparation,
                 updatePreparation,
                 initialAttempted,
                 updateAttempted,
                 initialFailureObserved,
                 updateFailureObserved,
                 rearmFailureObserved,
                 updatePreparationCount,
                 timerAcceptanceCount>>

(* This branch models LUringLoop::Run() after the failed re-arm has put it in
   Stopping.  There is no live driver SQE to cancel on this local-failure path;
   logical timers are explicitly discarded by loop shutdown. *)
DiscardTimersAfterRearmFailure ==
  /\ loopState = "Stopping"
  /\ rearmFailureObserved
  /\ driverState = "Idle"
  /\ timerState' = [timer \in Timers |->
                       IF timerState[timer] = "Accepted" THEN "Discarded" ELSE timerState[timer]]
  /\ loopState' = "Stopped"
  /\ UNCHANGED <<failureMode,
                 driverState,
                 driverTarget,
                 initialPreparation,
                 updatePreparation,
                 rearmPreparation,
                 initialAttempted,
                 updateAttempted,
                 rearmAttempted,
                 initialFailureObserved,
                 updateFailureObserved,
                 rearmFailureObserved,
                 driverPreparationCount,
                 updatePreparationCount,
                 timerAcceptanceCount>>

Next ==
  \/ BeginInitialPreparation
  \/ InitialPreparationFails
  \/ InitialPreparationSucceeds
  \/ FlushDriver
  \/ BeginEarlierUpdate
  \/ EarlierUpdateFails
  \/ EarlierUpdateSucceeds
  \/ AcceptLaterTimer
  \/ DriverTerminalForFirst
  \/ BeginRearm
  \/ RearmPreparationFails
  \/ RearmPreparationSucceeds
  \/ DiscardTimersAfterRearmFailure

Spec == Init /\ [][Next]_vars

TypeOK ==
  /\ loopState \in LoopStates
  /\ failureMode \in FailureModes
  /\ timerState \in [Timers -> TimerStates]
  /\ driverState \in DriverStates
  /\ driverTarget \in (Timers \cup {"None"})
  /\ initialPreparation \in PreparationStates
  /\ updatePreparation \in PreparationStates
  /\ rearmPreparation \in PreparationStates
  /\ initialAttempted \in BOOLEAN
  /\ updateAttempted \in BOOLEAN
  /\ rearmAttempted \in BOOLEAN
  /\ initialFailureObserved \in BOOLEAN
  /\ updateFailureObserved \in BOOLEAN
  /\ rearmFailureObserved \in BOOLEAN
  /\ driverPreparationCount \in Nat
  /\ updatePreparationCount \in Nat
  /\ timerAcceptanceCount \in [Timers -> Nat]

OneInitialAndRearmDriverPreparation == driverPreparationCount <= 2
OneEarlierUpdatePreparation == updatePreparationCount <= 1
OneLogicalAcceptancePerTimer == \A timer \in Timers: timerAcceptanceCount[timer] <= 1

InitialFailureRollsBack ==
  initialFailureObserved
    => /\ timerState["First"] = "Absent"
       /\ driverState = "Idle"
       /\ driverPreparationCount = 0
       /\ timerAcceptanceCount["First"] = 0

UpdateFailureRollsBack ==
  updateFailureObserved
    => /\ timerState["First"] \in {"Accepted", "Fired"}
       /\ timerState["Earlier"] = "Absent"
       /\ updatePreparationCount = 0
       /\ timerAcceptanceCount["Earlier"] = 0

AcceptedFirstHasPreparedDriver ==
  timerState["First"] \in {"Accepted", "Fired"} => driverPreparationCount >= 1

AcceptedEarlierHasPreparedUpdate ==
  timerState["Earlier"] = "Accepted" => updatePreparationCount = 1

RearmFailureStopsInsteadOfLosingTimer ==
  rearmFailureObserved => loopState \in {"Stopping", "Stopped"}

RearmFailureRetainsTimerUntilShutdown ==
  /\ rearmFailureObserved
  /\ loopState = "Stopping"
  => timerState["Later"] = "Accepted"

StoppedExplicitlyDiscardsUnfiredTimer ==
  loopState = "Stopped"
    => /\ timerState["Later"] = "Discarded"
       /\ driverState = "Idle"

=======================================================================================
