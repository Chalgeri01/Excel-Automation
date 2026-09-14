# Architecture Decision Records

This file records meaningful architectural, design, and technical decisions for the project. Add a new ADR, or update the status of an existing ADR, whenever a significant change, dependency choice, pattern shift, or engineering trade-off is proposed or accepted. ADR IDs are sequential and are not reused.

## ADR-001 — Isolate Excel process ownership and limit machine-wide concurrency

### ID / Date

ADR-001 / 2026-08-30

### Status

Accepted

### Context

The automation refreshes more than 450 Excel workbooks in its largest daily batch. Each file is processed through a separate hidden Excel COM instance. Historically, approximately four to six Excel processes could remain in a zombie state during a large run. Cleanup occurred only after the entire batch and used a bulk force-kill against every `EXCEL.EXE` process on the machine.

That approach created two related problems:

- Zombie processes accumulated for several hours, consuming memory, COM resources, file handles, and Power Query capacity.
- Bulk cleanup could terminate Excel instances owned by an overlapping scheduled batch or by an interactive user. This failure mode was observed when Batch 5 cleanup coincided with Batch 6 refresh failures.

Concurrency was also controlled only inside each batch. Independently scheduled batches could therefore exceed the intended host capacity when their schedules overlapped. Batch 1 averaged approximately 476 files and 10.2 hours with two workers, while historical timing data indicated that three workers could reduce its ideal processing time to approximately 6.3 hours.

### Decision

Use per-worker Excel process ownership together with a machine-wide concurrency gate:

1. Every call to `Start-Excel` acquires one of three machine-wide named mutex slots before creating Excel.
2. The worker resolves and records the operating-system PID associated with the Excel COM application's window handle.
3. `Stop-Excel` first requests a normal Excel shutdown, releases the COM object, and waits up to five seconds.
4. If that specific process remains alive, only the recorded worker-owned PID is force-terminated.
5. The previous scheduler-level bulk termination of all Excel processes is removed.
6. Named mutexes use the Windows `Global` namespace so independently launched Task Scheduler and PowerShell processes share the same three-slot limit. An abandoned mutex is recoverable when its owning worker process dies.
7. Batch-level worker counts are configured centrally in `$BatchThrottleMap`. Batch 1 launches three workers; Batches 2–9 currently launch two. A batch missing from the map defaults to two workers.
8. The combined number of automation-owned Excel instances remains capped by `MachineExcelLimit`, currently three, even when multiple batches overlap.

### Rationale

PID-scoped cleanup was selected instead of bulk process termination because it preserves failure isolation: one batch may clean up only the Excel instance it created. Performing cleanup after every file prevents zombie processes from accumulating until the end of a long batch.

A machine-wide gate was selected instead of relying only on PowerShell's per-batch `ThrottleLimit` because scheduled batches run in separate processes and can overlap. Named mutex slots provide operating-system-level coordination across those processes and recover more safely from worker termination than a process-local counter.

The global limit is set to three as a measured compromise. It allows Batch 1 to use an additional worker and materially reduce elapsed time without allowing every overlapping batch to independently create three or more Excel instances. The trade-off is that workers from other batches may wait for a slot when the machine is at capacity.

### Consequences

Positive impacts:

- Batch 1 can process up to three files concurrently, with an expected practical runtime of approximately 6.5–7.5 hours instead of 10.2 hours.
- Overlapping batches cannot exceed three concurrent automation-owned Excel instances in total.
- Zombie Excel processes that reach normal worker cleanup are removed close to the file that created them.
- Interactive Excel sessions and Excel instances belonging to other batches are not targeted by cleanup.
- Batch concurrency can be adjusted in one configuration map without changing runner logic.

Negative impacts and limitations:

- Workers may remain queued while all machine-wide slots are occupied, so a lower-priority long batch can affect the start time of another batch unless scheduling priorities are introduced later.
- Increasing the machine-wide limit requires capacity testing of RAM, CPU, network shares, Excel COM stability, and upstream data sources.
- A worker permanently blocked inside a COM call may not reach `Stop-Excel`. A future hard per-file watchdog is still required to terminate that worker and its owned Excel process reliably.
- The reviewed repository is located under `D:\Data`, while the current Task Scheduler configuration executes scripts from `C:\Users\kapl\Desktop\Project-Reporting-Automation`. Changes must be deployed to the live path before they affect scheduled runs.

## ADR-002 — Maintain an execution-flow map as an integration contract

### ID / Date

ADR-002 / 2026-08-30

### Status

Accepted

### Context

The automation crosses Windows Task Scheduler, PowerShell parent and child processes, Excel COM instances, network-hosted workbooks, MySQL, Python email workers, SMTP, FTP, and maintenance jobs. A change that is locally correct in one script can still fail at a boundary because parameters, dates, exit codes, concurrency limits, database state, or deployment paths are handed off between independently executed files.

The repository previously had no single maintained description of those runtime handoffs. Troubleshooting therefore depended on reconstructing the path from source code and logs in every session, increasing the risk of overlooking failures between modules.

### Decision

Maintain `Flow.md` in the project root as the authoritative repository-level execution map.

Each critical trace path records its entry point, exact ordered file/function handoffs, state changes, external dependencies, error behavior, and exit point. Any feature or fix that changes how modules interact must update the relevant flow in the same change. Debugging should begin by following the documented trace and identifying the first boundary where observed state differs from expected state.

`Decisions.md` remains the record of why significant technical choices were made; `Flow.md` records how the accepted implementation executes. The two documents are updated together when an architectural decision changes runtime flow.

### Rationale

A code-adjacent flow map was selected because this project has no application framework that makes control flow visible through routes, controllers, or service registration. The important behavior is distributed across scheduled processes and external systems, and several downstream exit codes are not currently propagated to the scheduler.

Keeping the map in Markdown makes it reviewable with code, deployable with the repository, and usable without specialized tooling. The accepted trade-off is ongoing maintenance: an inaccurate flow map can be more misleading than no map, so interaction changes are not complete until the document is updated and checked against the implementation.

### Consequences

Positive impacts:

- Cross-file and cross-process handoffs have a stable troubleshooting reference.
- Date, state, concurrency, database, and exit-code boundaries become explicit review points.
- Future sessions can distinguish the production path from legacy/direct launchers and support scripts.
- Architectural rationale and runtime behavior remain connected through `Decisions.md` and `Flow.md`.

Negative impacts and limitations:

- Every interaction change now includes documentation work and verification.
- Task Scheduler configuration and other external state can drift from the repository unless their actual definitions are periodically compared with `Flow.md`.
- The flow map describes observed code behavior; it does not by itself fix the known propagation, deployment, or observability gaps it identifies.

## ADR-003 — Maintain a living handover for AI session continuity

### ID / Date

ADR-003 / 2026-08-30

### Status

Accepted

### Context

Work on the automation spans multiple AI sessions and includes uncommitted code, external Task Scheduler configuration, production deployment paths, generated operational artifacts, and unresolved integration risks. Chat history is not a reliable long-term project record, and reconstructing the current state at the start of every session wastes time and can cause completed work, known hazards, or deployment boundaries to be missed.

`Decisions.md` explains why architectural choices were accepted, and `Flow.md` explains how execution travels through the system, but neither is intended to answer the immediate operational questions: what is done, what is active, what is broken, and what must not be disturbed.

### Decision

Maintain `Handover.md` in the project root as the living session-continuity record.

Every AI session must begin by reading `Handover.md`, followed by the relevant sections of `Decisions.md` and `Flow.md`. At the end of a session, or immediately after a significant completed task, the handover date and affected sections must be updated. Stale statements must be removed or moved to the correct status so the file describes the present state rather than accumulating an unfiltered activity log.

The handover will contain the project's current status, recently completed work, active work, known failures and unresolved risks, and explicit cautions for future changes. It must not contain plaintext secrets.

### Rationale

A single short, repository-local handover was selected because it can be read before code changes without depending on chat memory or an external project-management system. Separating current state from the permanent ADR history and detailed flow map keeps the initial context useful and scannable.

The accepted trade-off is maintenance discipline. The handover can become harmful if it is stale, overly historical, or treated as proof of production state without checking external systems. Each session must therefore update existing facts instead of only appending new notes.

### Consequences

Positive impacts:

- New sessions have a consistent starting point and can preserve prior work and cautions.
- Active work, completed work, known failures, and deployment status are distinguished explicitly.
- Dirty-worktree ownership and production/runtime boundaries are visible before edits begin.
- `Handover.md`, `Decisions.md`, and `Flow.md` provide complementary current-state, rationale, and execution records.

Negative impacts and limitations:

- Significant tasks now require a handover update before completion.
- The file does not automatically verify Task Scheduler, MySQL, SMB, SMTP, FTP, or deployment state; time-sensitive claims still require live checks.
- The convention depends on future sessions discovering and following the repository instruction. If stronger automatic enforcement is required, a repository-level agent instruction file should be considered separately.

## ADR-004 — Stabilize the refresh pipeline before validating three-worker Batch 1

### ID / Date

ADR-004 / 2026-08-31

### Status

Proposed

Partial implementation on 2026-08-31: the repository now contains the durable dispatcher, per-file phase, worker PID, and owned Excel PID trace described in Decision 4. This instrumentation is not yet proven deployed to `TEST-BI`. Refresh-semantics reconciliation, enforced timeout behavior, the external watchdog, and the two-worker canary remain proposed.

### Context

Batch 1 historically completed approximately 471-479 workbook events in about 9.4-11.9 hours with two workers. Warning signs appeared before the new concurrency deployment: the 2026-08-27 run contained five failures, including Excel RPC failures lasting 55-78 minutes, and the 2026-08-29 run contained three failures, including recurring failures in `PD_KA_3011.xlsb` and `PD_KA_3014.xlsb`. The 2026-08-30 run did not produce a final CSV under the older scheduler behavior.

The first log entries proving deployment of the new batch throttle and machine-wide limit appear on 2026-08-31. That Batch 1 run used three workers, remained inside `Run-Parallel.ps1`, and was ultimately interrupted by the daily machine reboot without a final CSV or email stage. Batch 2 also remained incomplete, while later small batches finished, indicating individual blocked workers rather than a complete mutex deadlock.

The reviewed repository also shows an unintended refresh-pipeline difference alongside the intended concurrency changes. The active `Refresh-WorkbookSmart` in `Shared-Excel-Helpers.ps1` now matches the function previously named `Old_Refresh-WorkbookSmart`. A retained earlier helper copy has a different active implementation that temporarily changes Excel from manual to automatic calculation during refresh and restores the previous mode afterward. The current `Start-Excel` sets calculation to manual, while the current active refresh function does not switch it back during Power Query processing.

Several Excel COM calls remain capable of blocking outside an enforceable timeout, including workbook open, synchronous `RefreshAll`, `CalculateUntilAsyncQueriesDone`, save, close, and quit. `Wait-Connections` returns `false` after its timeout, but that result is ignored. Per-file events are written only after completion, so a blocked workbook has no durable START/phase record.

### Decision

Before treating three-worker Batch 1 as production-ready, perform a controlled stabilization and canary validation:

1. Compare the helper actually deployed on `TEST-BI` with the repository and the last known working refresh implementation.
2. Preserve the accepted machine-wide mutex and PID-owned cleanup, but restore the intended calculation-mode and refresh semantics rather than using the older refresh implementation accidentally.
3. Treat a connection-wait timeout as a file failure instead of continuing silently to save.
4. Add durable per-file START, current phase, END, worker PID, and owned Excel PID diagnostics without logging the database connection string.
5. Add an external per-file watchdog around the complete `Refresh-One.ps1` child process so a blocked COM call cannot hold a slot indefinitely. The watchdog may terminate only the timed-out worker and its proven owned Excel PID.
6. Run the first full Batch 1 canary at two workers, with no overlapping manual/scheduled duplicate. Return to three workers only after the canary completes and resource/error evidence supports the increase.

Only the diagnostic file-tracing portion is implemented in the repository. The overall stabilization proposal is not yet accepted or completed.

### Rationale

The observed failure cannot be attributed solely to the increase from two to three workers because instability and an incomplete run were already visible beforehand. At the same time, raising concurrency increases Excel, Power Query, SMB, memory, and upstream-source contention, and the refresh-function difference makes the first three-worker result an invalid clean performance comparison.

A two-worker canary provides a known historical baseline while retaining the safer process-ownership cleanup. Phase logging identifies the exact blocking workbook and COM stage. An external watchdog is necessary because an in-process timer cannot interrupt a thread already blocked inside an Excel COM call.

### Consequences

Positive impacts:

- Separates pre-existing workbook/data-source instability from concurrency-related regressions.
- Produces evidence for the exact workbook and phase instead of relying on a missing final CSV.
- Prevents one hard COM hang from holding an Excel slot until the daily reboot.
- Retains the safety benefit of targeted PID cleanup and avoids global Excel termination.

Negative impacts and limitations:

- The first stabilized Batch 1 run will use two workers and may again take roughly 9-12 hours.
- File instrumentation has been implemented and concurrency-tested locally, but still requires deployment and production validation; watchdog behavior remains to be implemented and tested.
- A timeout threshold must account for legitimate 20-minute files while still stopping genuine hangs.
- Production validation still requires access to `TEST-BI`, MySQL events, and the network-hosted workbooks/data sources.
