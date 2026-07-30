---------------- MODULE async_stream_multiop_backend_refinement ----------------

EXTENDS Naturals, Sequences, FiniteSets

(***************************************************************************)
(* Reactor / io_uring 到并发 AsyncStream operation 的有界 trace refinement。 *)
(*                                                                         *)
(* 一个 backend 在 Init 时固定。它可以执行 readiness 或 SQE/CQE 等内部    *)
(* stuttering 步；只有 Submit、Complete/Cancel、Resume 和 Close 写入       *)
(* application-visible trace。                                             *)
(*                                                                         *)
(* 范围是一个 stream 的一个 Read 和一个 Write。它验证两个后端均保持同一    *)
(* operation owner、Close 和可观察 trace 协议，并不要求物理状态相同。      *)
(***************************************************************************)

Backends == {"Reactor", "LUring"}
Operations == {"Read", "Write"}
Coroutines == {"Reader", "Writer"}
Owner(op) == IF op = "Read" THEN "Reader" ELSE "Writer"

ResourceStates == {"Open", "Closing", "Closed"}
OperationStates == {"None", "Pending", "Completed", "Cancelled"}
CoroutineStates == {"Running", "Waiting", "Ready"}
ReactorStates == {"Idle", "ChannelWaiting", "Ready"}
UringStates == {"Idle", "SQEQueued", "Submitted", "CQEReady"}

VARIABLES backend,
          resourceState,
          operationState,
          coroutineState,
          submitCount,
          completionCount,
          resumeCount,
          reactorState,
          uringState,
          trace

vars == <<backend,
          resourceState,
          operationState,
          coroutineState,
          submitCount,
          completionCount,
          resumeCount,
          reactorState,
          uringState,
          trace>>

TraceEvents ==
  (Operations \X {"Submit", "Complete", "Cancel", "Resume"})
    \cup {<<"Stream", "Close">>}

TerminalEvents(op) == {<<op, "Complete">>, <<op, "Cancel">>}
Occurrences(event) == Cardinality({i \in 1..Len(trace): trace[i] = event})
AllSettled == \A op \in Operations: operationState[op] # "Pending"

Init ==
  /\ backend \in Backends
  /\ resourceState = "Open"
  /\ operationState = [op \in Operations |-> "None"]
  /\ coroutineState = [coroutine \in Coroutines |-> "Running"]
  /\ submitCount = [op \in Operations |-> 0]
  /\ completionCount = [op \in Operations |-> 0]
  /\ resumeCount = [op \in Operations |-> 0]
  /\ reactorState = [op \in Operations |-> "Idle"]
  /\ uringState = [op \in Operations |-> "Idle"]
  /\ trace = <<>>

(***************************************************************************)
(* Reactor physical interpretation.                                        *)
(***************************************************************************)

ReactorSubmitPending(op) ==
  /\ backend = "Reactor"
  /\ op \in Operations
  /\ resourceState = "Open"
  /\ operationState[op] = "None"
  /\ coroutineState[Owner(op)] = "Running"
  /\ reactorState[op] = "Idle"
  /\ operationState' = [operationState EXCEPT ![op] = "Pending"]
  /\ coroutineState' = [coroutineState EXCEPT ![Owner(op)] = "Waiting"]
  /\ submitCount' = [submitCount EXCEPT ![op] = @ + 1]
  /\ reactorState' = [reactorState EXCEPT ![op] = "ChannelWaiting"]
  /\ trace' = Append(trace, <<op, "Submit">>)
  /\ UNCHANGED <<backend, resourceState, completionCount, resumeCount, uringState>>

ReactorImmediateComplete(op) ==
  /\ backend = "Reactor"
  /\ op \in Operations
  /\ resourceState = "Open"
  /\ operationState[op] = "None"
  /\ coroutineState[Owner(op)] = "Running"
  /\ reactorState[op] = "Idle"
  /\ operationState' = [operationState EXCEPT ![op] = "Completed"]
  /\ submitCount' = [submitCount EXCEPT ![op] = @ + 1]
  /\ completionCount' = [completionCount EXCEPT ![op] = @ + 1]
  /\ trace' = Append(Append(trace, <<op, "Submit">>), <<op, "Complete">>)
  /\ UNCHANGED <<backend, resourceState, coroutineState, resumeCount, reactorState, uringState>>

ReactorReady(op) ==
  /\ backend = "Reactor"
  /\ op \in Operations
  /\ reactorState[op] = "ChannelWaiting"
  /\ reactorState' = [reactorState EXCEPT ![op] = "Ready"]
  /\ UNCHANGED <<backend,
                 resourceState,
                 operationState,
                 coroutineState,
                 submitCount,
                 completionCount,
                 resumeCount,
                 uringState,
                 trace>>

ReactorComplete(op) ==
  /\ backend = "Reactor"
  /\ op \in Operations
  /\ resourceState \in {"Open", "Closing"}
  /\ operationState[op] = "Pending"
  /\ coroutineState[Owner(op)] = "Waiting"
  /\ reactorState[op] = "Ready"
  /\ operationState' = [operationState EXCEPT ![op] = "Completed"]
  /\ completionCount' = [completionCount EXCEPT ![op] = @ + 1]
  /\ reactorState' = [reactorState EXCEPT ![op] = "Idle"]
  /\ trace' = Append(trace, <<op, "Complete">>)
  /\ UNCHANGED <<backend, resourceState, coroutineState, submitCount, resumeCount, uringState>>

ReactorCancel(op) ==
  /\ backend = "Reactor"
  /\ op \in Operations
  /\ resourceState = "Closing"
  /\ operationState[op] = "Pending"
  /\ coroutineState[Owner(op)] = "Waiting"
  /\ reactorState[op] \in {"ChannelWaiting", "Ready"}
  /\ operationState' = [operationState EXCEPT ![op] = "Cancelled"]
  /\ completionCount' = [completionCount EXCEPT ![op] = @ + 1]
  /\ reactorState' = [reactorState EXCEPT ![op] = "Idle"]
  /\ trace' = Append(trace, <<op, "Cancel">>)
  /\ UNCHANGED <<backend, resourceState, coroutineState, submitCount, resumeCount, uringState>>

(***************************************************************************)
(* io_uring physical interpretation.                                       *)
(***************************************************************************)

UringPrepareSQE(op) ==
  /\ backend = "LUring"
  /\ op \in Operations
  /\ resourceState = "Open"
  /\ operationState[op] = "None"
  /\ coroutineState[Owner(op)] = "Running"
  /\ uringState[op] = "Idle"
  /\ operationState' = [operationState EXCEPT ![op] = "Pending"]
  /\ coroutineState' = [coroutineState EXCEPT ![Owner(op)] = "Waiting"]
  /\ submitCount' = [submitCount EXCEPT ![op] = @ + 1]
  /\ uringState' = [uringState EXCEPT ![op] = "SQEQueued"]
  /\ trace' = Append(trace, <<op, "Submit">>)
  /\ UNCHANGED <<backend, resourceState, completionCount, resumeCount, reactorState>>

UringImmediateComplete(op) ==
  /\ backend = "LUring"
  /\ op \in Operations
  /\ resourceState = "Open"
  /\ operationState[op] = "None"
  /\ coroutineState[Owner(op)] = "Running"
  /\ uringState[op] = "Idle"
  /\ operationState' = [operationState EXCEPT ![op] = "Completed"]
  /\ submitCount' = [submitCount EXCEPT ![op] = @ + 1]
  /\ completionCount' = [completionCount EXCEPT ![op] = @ + 1]
  /\ trace' = Append(Append(trace, <<op, "Submit">>), <<op, "Complete">>)
  /\ UNCHANGED <<backend, resourceState, coroutineState, resumeCount, reactorState, uringState>>

UringSubmit(op) ==
  /\ backend = "LUring"
  /\ op \in Operations
  /\ uringState[op] = "SQEQueued"
  /\ uringState' = [uringState EXCEPT ![op] = "Submitted"]
  /\ UNCHANGED <<backend,
                 resourceState,
                 operationState,
                 coroutineState,
                 submitCount,
                 completionCount,
                 resumeCount,
                 reactorState,
                 trace>>

UringCQE(op) ==
  /\ backend = "LUring"
  /\ op \in Operations
  /\ uringState[op] = "Submitted"
  /\ uringState' = [uringState EXCEPT ![op] = "CQEReady"]
  /\ UNCHANGED <<backend,
                 resourceState,
                 operationState,
                 coroutineState,
                 submitCount,
                 completionCount,
                 resumeCount,
                 reactorState,
                 trace>>

UringComplete(op) ==
  /\ backend = "LUring"
  /\ op \in Operations
  /\ resourceState \in {"Open", "Closing"}
  /\ operationState[op] = "Pending"
  /\ coroutineState[Owner(op)] = "Waiting"
  /\ uringState[op] = "CQEReady"
  /\ operationState' = [operationState EXCEPT ![op] = "Completed"]
  /\ completionCount' = [completionCount EXCEPT ![op] = @ + 1]
  /\ uringState' = [uringState EXCEPT ![op] = "Idle"]
  /\ trace' = Append(trace, <<op, "Complete">>)
  /\ UNCHANGED <<backend, resourceState, coroutineState, submitCount, resumeCount, reactorState>>

UringCancel(op) ==
  /\ backend = "LUring"
  /\ op \in Operations
  /\ resourceState = "Closing"
  /\ operationState[op] = "Pending"
  /\ coroutineState[Owner(op)] = "Waiting"
  /\ uringState[op] \in {"SQEQueued", "Submitted", "CQEReady"}
  /\ operationState' = [operationState EXCEPT ![op] = "Cancelled"]
  /\ completionCount' = [completionCount EXCEPT ![op] = @ + 1]
  /\ uringState' = [uringState EXCEPT ![op] = "Idle"]
  /\ trace' = Append(trace, <<op, "Cancel">>)
  /\ UNCHANGED <<backend, resourceState, coroutineState, submitCount, resumeCount, reactorState>>

(***************************************************************************)
(* Shared logical transitions.                                             *)
(***************************************************************************)

Resume(op) ==
  /\ op \in Operations
  /\ coroutineState[Owner(op)] = "Waiting"
  /\ operationState[op] \in {"Completed", "Cancelled"}
  /\ resumeCount[op] = 0
  /\ coroutineState' = [coroutineState EXCEPT ![Owner(op)] = "Ready"]
  /\ resumeCount' = [resumeCount EXCEPT ![op] = @ + 1]
  /\ trace' = Append(trace, <<op, "Resume">>)
  /\ UNCHANGED <<backend,
                 resourceState,
                 operationState,
                 submitCount,
                 completionCount,
                 reactorState,
                 uringState>>

Close ==
  /\ resourceState = "Open"
  /\ resourceState' = IF AllSettled THEN "Closed" ELSE "Closing"
  /\ trace' = Append(trace, <<"Stream", "Close">>)
  /\ UNCHANGED <<backend,
                 operationState,
                 coroutineState,
                 submitCount,
                 completionCount,
                 resumeCount,
                 reactorState,
                 uringState>>

FinalizeClose ==
  /\ resourceState = "Closing"
  /\ AllSettled
  /\ resourceState' = "Closed"
  /\ UNCHANGED <<backend,
                 operationState,
                 coroutineState,
                 submitCount,
                 completionCount,
                 resumeCount,
                 reactorState,
                 uringState,
                 trace>>

Next ==
  \/ \E op \in Operations: ReactorSubmitPending(op)
  \/ \E op \in Operations: ReactorImmediateComplete(op)
  \/ \E op \in Operations: ReactorReady(op)
  \/ \E op \in Operations: ReactorComplete(op)
  \/ \E op \in Operations: ReactorCancel(op)
  \/ \E op \in Operations: UringPrepareSQE(op)
  \/ \E op \in Operations: UringImmediateComplete(op)
  \/ \E op \in Operations: UringSubmit(op)
  \/ \E op \in Operations: UringCQE(op)
  \/ \E op \in Operations: UringComplete(op)
  \/ \E op \in Operations: UringCancel(op)
  \/ \E op \in Operations: Resume(op)
  \/ Close
  \/ FinalizeClose

Spec == Init /\ [][Next]_vars

TypeOK ==
  /\ backend \in Backends
  /\ resourceState \in ResourceStates
  /\ operationState \in [Operations -> OperationStates]
  /\ coroutineState \in [Coroutines -> CoroutineStates]
  /\ submitCount \in [Operations -> Nat]
  /\ completionCount \in [Operations -> Nat]
  /\ resumeCount \in [Operations -> Nat]
  /\ reactorState \in [Operations -> ReactorStates]
  /\ uringState \in [Operations -> UringStates]
  /\ trace \in Seq(TraceEvents)

BackendStateShape ==
  /\ backend = "Reactor" => \A op \in Operations: uringState[op] = "Idle"
  /\ backend = "LUring" => \A op \in Operations: reactorState[op] = "Idle"

RefinementInvariant ==
  /\ backend = "Reactor"
       => \A op \in Operations:
            operationState[op] = "Pending"
              <=> reactorState[op] \in {"ChannelWaiting", "Ready"}
  /\ backend = "LUring"
       => \A op \in Operations:
            operationState[op] = "Pending"
              <=> uringState[op] \in {"SQEQueued", "Submitted", "CQEReady"}

SingleSubmission == \A op \in Operations: submitCount[op] <= 1
UniqueCompletion == \A op \in Operations: completionCount[op] <= 1
ExactlyOnceResume == \A op \in Operations: resumeCount[op] <= 1
ClosedHasNoPending == resourceState = "Closed" => AllSettled

TraceCountMatchesState ==
  /\ \A op \in Operations:
       Occurrences(<<op, "Submit">>) = submitCount[op]
  /\ \A op \in Operations:
       Occurrences(<<op, "Complete">>) + Occurrences(<<op, "Cancel">>) = completionCount[op]
  /\ \A op \in Operations:
       Occurrences(<<op, "Resume">>) = resumeCount[op]

TraceResumeAfterTerminal ==
  \A op \in Operations:
    \A i \in 1..Len(trace):
      trace[i] = <<op, "Resume">>
        => \E j \in 1..(i - 1): trace[j] \in TerminalEvents(op)

TraceCloseRejectsLaterSubmit ==
  \A i \in 1..Len(trace):
    trace[i] = <<"Stream", "Close">>
      => \A j \in (i + 1)..Len(trace):
           \A op \in Operations: trace[j] # <<op, "Submit">>

========================================================================================
