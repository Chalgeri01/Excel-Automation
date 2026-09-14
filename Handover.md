# Project Handover

Last updated: 2026-08-31

## Session Protocol

At the start of every AI session:

1. Read this file before inspecting or changing code.
2. Read `Decisions.md` for accepted technical choices and their rationale.
3. Read the relevant trace path in `Flow.md` before changing or debugging interactions between scripts or external systems.
4. Confirm whether the request applies to the repository copy under `D:\Data\Project-Reporting-Automation`, the live scheduled copy under `C:\Users\kapl\Desktop\Project-Reporting-Automation`, or both.

At the end of every session, or immediately after a significant task, update the date and the relevant sections below. Remove stale statements, move completed work out of "What's In Progress," and leave concrete evidence such as the verification performed and any deployment still required.

## Current Status / Where Things Stand

This is a Windows scheduled Excel reporting automation. Its primary production path is expected to be:

`Windows Task Scheduler -> Scheduled-Runner.ps1 -> Run-Parallel.ps1 -> Refresh-One.ps1 -> Excel COM / MySQL -> CSV export -> email or Batch 8 FTP`

The largest batch processes more than 450 workbooks. The current repository implementation uses batch-specific PowerShell throttles together with a three-slot machine-wide Excel limit. Batch 1 launches up to three workers; Batches 2-9 launch up to two; an unmapped batch defaults to two. Overlapping batches can still own no more than three automation Excel instances in total when they use the same `MachineExcelLimit` and mutex names.

Excel shutdown is now worker-owned. Each worker records the PID belonging to its Excel COM instance, attempts a normal shutdown, waits five seconds, and force-terminates only that PID if necessary. There should be no scheduler-level or batch-level command that kills every Excel process on the machine.

Refresh and email outcomes are primarily recorded in the MySQL `events` table and exported to per-run CSV files. Email sending uses Python worker threads and Google Workspace SMTP Relay. Batch 8 performs a separate FTP synchronization after refresh export.

The repository now also contains a concurrency-safe per-run diagnostic trace. `Scheduled-Runner.ps1` derives `Logginfo\refresh-trace_YYYY-MM-DD_Batch-N.log`; the dispatcher and child workers append records containing timestamp, run ID, batch, report path, worker PID, owned Excel PID, elapsed time, phase, level, and sanitized details. The trace brackets skip checks, worker launch/exit, Excel slot/create/quit/cleanup, workbook open/refresh/query wait/table/pivot/model/calculate/save/close, retries, and the final database write. Connection polling emits a progress record every 60 seconds while Excel still reports refreshing. The trace deliberately excludes the database connection string. This is implemented only in the `D:\Data` repository copy and has not been proven deployed to `TEST-BI`.

The repository working copy is under `D:\Data\Project-Reporting-Automation`, but hard-coded scheduler defaults point to `C:\Users\kapl\Desktop\Project-Reporting-Automation`. Repository changes have not been proven deployed to the live scheduled path. The worktree is heavily dirty and includes user-owned workbook changes, generated logs, deleted historical files, and modified scripts; do not assume unrelated changes are disposable.

The current documentation set is:

- `Handover.md`: current operational and development state; read first and update last.
- `Decisions.md`: ADR history explaining why significant decisions were made.
- `Flow.md`: exact execution paths, state handoffs, external boundaries, and troubleshooting checkpoints.

Active operational incident as of the synchronized log timestamp 2026-08-31 13:30 IST:

- Batch 1 started on `TEST-BI` at 00:01:02 with batch throttle 3 and machine limit 3, entered `Run-Parallel.ps1`, and has no return, CSV export, email start, or batch summary. No `run-log_2026-08-31_Batch-1.csv` exists in the synchronized logs.
- Batch 2 started at 05:46:46, also entered `Run-Parallel.ps1`, and has no return or CSV export.
- Batches 9, 3, and 4 subsequently completed. This means the machine-wide gate was not fully deadlocked; the leading diagnosis is one or more individual workers blocked inside or before per-file cleanup while other slot capacity remained available.
- Recent completed Batch 1 runs from 2026-08-23 through 2026-08-29 reached their last refresh event in approximately 9.4 to 11.9 hours. The 2026-08-31 run is outside that recent range.
- The review workstation can ping `TEST-BI` but is denied remote Task Scheduler, admin-share, WinRM, and DCOM process access. It also lacks the production `REPORTLOGS_CONN`, so the exact active workbook/PID cannot be identified from this workstation. The synchronized orchestration log records no per-file start event; per-file results remain in MySQL until final CSV export.
- The user confirmed that the scheduled daily reboot occurred before live process/intermediate evidence could be captured. The synchronized copy still ends at the Batch 5 start at 13:30, so it contains no post-reboot task result or final Batch 1/2 event.

Investigation conclusions:

- The slowdown was developing before the new concurrency deployment. Batch 1 had five failures on 2026-08-27, including RPC failures lasting approximately 55 and 78 minutes; it had three failures on 2026-08-29; and the 2026-08-30 run produced no final Batch 1 CSV under the older scheduler behavior.
- The log first shows the new batch-throttle/machine-limit deployment on 2026-08-31. Therefore three-worker operation may have increased resource contention, but it is not the sole origin of the problem.
- The reviewed helper changed beyond PID cleanup and mutex coordination. Current `Refresh-WorkbookSmart` matches the previously named older implementation and does not temporarily enable automatic calculation during refresh, although `Start-Excel` sets calculation to manual. `Scripts/Shared-Excel-Helpers-1.ps1` contains a different retained active implementation that enables automatic calculation and restores the prior mode. The exact helper deployed on `TEST-BI` still must be compared directly.
- No current timeout can interrupt workbook open, synchronous refresh, asynchronous-query wait, save, close, or quit after a COM call blocks. The connection-wait timeout result is also ignored. Targeted PID cleanup helps only after control reaches `Stop-Excel`.
- Do not launch a full manual Batch 1 at 21:36 on 2026-08-31. The next scheduled Batch 1 is near 00:01, and the after-18:00 date rule would give both runs `run-log_2026-09-01_Batch-1`, risking duplicate workbook updates, database identity collision, and contention over the same local master copy.

## What's Done

- Removed global/bulk Excel termination from the primary scheduled path.
- Added per-worker Excel process ownership by mapping the Excel COM window handle to its operating-system PID.
- Added normal Excel shutdown, COM release, a five-second exit wait, and targeted force-kill of only the owned PID.
- Added a machine-wide concurrency gate using three named global Windows mutex slots. Abandoned slots can be recovered after a worker process dies.
- Passed `MachineExcelLimit=3` and `ExcelSlotWaitTimeoutSec=21600` through `Scheduled-Runner.ps1`, `Run-Parallel.ps1`, `Refresh-One.ps1`, and `Shared-Excel-Helpers.ps1`.
- Added `$BatchThrottleMap` in `Scheduled-Runner.ps1`: Batch 1 uses three workers; Batches 2-9 use two; missing batch entries default to two.
- Verified the concurrency gate with five independent PowerShell workers; observed concurrency did not exceed three slots.
- Verified the owned-PID Excel lifecycle with a real Excel process while preserving pre-existing Excel processes.
- Established `Decisions.md` and recorded:
  - ADR-001: owned Excel cleanup and machine-wide concurrency.
  - ADR-002: maintain `Flow.md` as an integration contract.
  - ADR-003: maintain this handover as the session-continuity record.
  - ADR-004 (Proposed): stabilize refresh semantics, add phase diagnostics/watchdog, and canary Batch 1 at two workers before revalidating three.
- Established `Flow.md` with eight traced paths covering scheduling, fan-out, Excel lifecycle, workbook refresh, email, FTP, error propagation, and maintenance jobs.
- Documented the current known integration gaps, alternate/legacy launchers, deployment boundary, and troubleshooting order.
- The current email script contains parallel SMTP sending, per-thread reusable connections, health checks, bounded retries, attachment-size protection, database validation, event logging, and CSV export. These behaviors were present during the current-state review and are mapped in `Flow.md`.
- Diagnosed the 2026-08-31 Batch 1 incident from synchronized logs without changing or terminating runtime state. Confirmed the stop boundary is inside `Run-Parallel.ps1`, not CSV export or email, and confirmed Batch 2 is independently incomplete at the same boundary.
- Added `Scripts/Shared-Trace-Helpers.ps1` and propagated one per-run trace path through `Scheduled-Runner.ps1`, `Run-Parallel.ps1`, and `Refresh-One.ps1`.
- Added durable per-item and per-phase diagnostics across dispatcher skip checks, child process boundaries, refresh attempts/retries, Excel mutex/PID lifecycle, workbook operations, and MySQL result writes. Logging failures are isolated and cannot fail a refresh; database credentials are not logged.
- Verified all five affected PowerShell scripts parse successfully from UTF-8 source. Verified helper contracts expose the new `Trace` parameters. Stress-tested five separate PowerShell jobs writing 100 records to one trace: all jobs completed, all 100 lines were present, and no line was malformed/interleaved.
- Updated ADR-004 and `Flow.md` to record the partial diagnostics implementation and the new trace path.

## What's In Progress

The diagnostic logging code is complete in the `D:\Data` repository copy. Deployment and a monitored runtime validation remain in progress; the Batch 1/Batch 2 production incident remains open.

Operational follow-up is still pending:

- On `TEST-BI`, inspect the active `Scheduled-Runner`, `Run-Parallel`, `Refresh-One`, and Excel process tree and extract the `-Path` value from active refresh workers without exposing the `-DbConn` argument.
- Query MySQL `events` for `run-log_2026-08-31_Batch-1` and `run-log_2026-08-31_Batch-2` to count completed rows, find the last event, and compare active worker paths with completed paths.
- Deploy/reconcile `Shared-Trace-Helpers.ps1` and the four modified PowerShell scripts as one atomic set. A partial deployment will break the new parameter/helper handoff.
- Decide whether to accept the remaining portions of ADR-004. If accepted, reconcile the active refresh implementation, add an external child-process watchdog, enforce timeout behavior, then perform a two-worker canary before trying three workers again.
- Do not run a full manual Batch 1 close to the 00:01 schedule. A diagnostic rerun must first prevent scheduled overlap, use an unambiguous run identity, suppress downstream email, and preferably use a small copied worklist containing known slow/failing files.
- Do not terminate any process until each Excel PID is mapped to its owning `Refresh-One` process. Never use a bulk Excel kill during incident recovery.
- Deploy or reconcile the reviewed `D:\Data` scripts with the live `C:\Users\kapl\Desktop` scheduler path.
- Run a monitored production validation of Batch 1 with three workers and confirm actual elapsed time, peak RAM/CPU, Excel process count, workbook failure count, and overlap behavior.
- Confirm today's Task Scheduler state and result on the production host. The previously supplied result `267014` decodes locally to `0x41306`: "The last run of the task was terminated by the user."
- Export and version the real Task Scheduler definitions so action paths, arguments, triggers, task account, environment, overlap policy, and time labels can be compared with the code.

## What's Broken / Known Issues

- **End-to-end status is unreliable:** `Run-Parallel.ps1` does not aggregate `Refresh-One.ps1` exit codes. Email, refresh-CSV export, and FTP failures are logged but are not consistently promoted to `Scheduled-Runner.ps1`'s final exit code. Task Scheduler result `0` therefore does not guarantee that every downstream action succeeded.
- **Power Query timeout is not enforced:** `Wait-Connections` returns `false` on timeout, but `Refresh-WorkbookSmart` ignores the result and continues toward calculation and save.
- **Hard COM hangs remain possible:** a worker blocked permanently inside a COM call may never reach `Stop-Excel`; there is no external per-file watchdog yet.
- **Current production incident:** Batch 1 and Batch 2 on 2026-08-31 have not returned from `Run-Parallel.ps1`. Because later batches completed, this is not a total mutex deadlock. The exact active workbook is unconfirmed until the `TEST-BI` process tree or MySQL can be inspected.
- **Database refresh logging still has gaps:** failures before the main refresh result insert can exit without an `events` row. The new file trace records those earlier phases once deployed, but a successful refresh whose DB insert fails still warns and exits `0`, which can later make email validation fail.
- **Trace is diagnostic, not a watchdog:** it reveals the last phase, file, worker PID, and Excel PID, but it does not interrupt a blocked COM call. A hard hang can still hold a mutex slot until an external watchdog or reboot ends the worker.
- **Refresh/email date mismatch:** Batches 1 and 2 can use the scheduler's next-day logical refresh date after 18:00, while the email script independently defaults to the current local date because `Scheduled-Runner.ps1` does not pass `--email-date`.
- **Email audit metadata is inaccurate:** email events use a hard-coded email `MASTER_PATH` rather than the selected batch master workbook.
- **Temporary directories accumulate:** every refresh creates a unique directory under `C:\Temp\ExcelTmp`, but the active worker does not remove it.
- **FTP failures can look successful:** the FTP script catches top-level exceptions without returning a nonzero result, and directory-creation exceptions are silently ignored.
- **Log archive deletion risk:** `sync_and_cleanup.py` proceeds to delete old source logs after individual destination copy failures.
- **Scheduler configuration drift:** complete task definitions are not in the repository. Supplied task-name times differ from some `$BatchNameMap` values for Batches 1, 2, 5, and 8.
- **Production deployment is unverified:** code under `D:\Data` may differ from the scripts actually run from `C:\Users\kapl\Desktop`.
- **Credentials are embedded in source:** the older launcher and FTP script contain plaintext connection credentials. They have not yet been moved to secure configuration or rotated.
- **Legacy entry points are inconsistent:** direct batch files and `ReportRunner/Launch-RunParallel.ps1` bypass parts of central scheduling, run-ID, email/FTP, and throttle logic. `Run-Reports.bat` also passes an invalid value as `SheetName` for the current contract.

## What to Avoid / Watch Out For

- Never restore `Stop-Process -Name EXCEL`, `taskkill /IM EXCEL.EXE`, or any other global Excel cleanup. Only terminate a PID proven to be owned by the current worker.
- Do not increase `MachineExcelLimit` above three without monitored capacity testing of RAM, CPU, SMB/network load, Excel COM stability, and upstream data sources.
- Keep every scheduled route on the same mutex prefix and machine limit. A bypassing launcher or inconsistent setting weakens the machine-wide guarantee.
- Treat `Scripts/Scheduled-Runner.ps1` as the primary orchestration entry point. Do not use the old `.bat` or `ReportRunner` launchers as production equivalents without reconciling their parameters and behavior.
- Edit `Scripts/Shared-Excel-Helpers.ps1`, not the unreferenced `Scripts/Shared-Excel-Helpers-1.ps1` duplicate.
- Do not change code in `D:\Data` and assume Task Scheduler is using it. Verify/deploy to the registered live action path explicitly.
- Do not deploy only `Refresh-One.ps1` or only the new trace helper. `Scheduled-Runner.ps1`, `Run-Parallel.ps1`, `Refresh-One.ps1`, `Shared-Excel-Helpers.ps1`, and `Shared-Trace-Helpers.ps1` form one trace contract and must be deployed together.
- Do not delete, reset, or overwrite unrelated dirty-worktree changes. Workbooks, logs, historical deletions, and untracked files may belong to the user or running automation.
- Do not commit or repeat plaintext credentials in documentation, logs, chat output, or new source files. Any credential remediation should be handled as a separate, documented decision with a rotation plan.
- Do not rely only on the parent process exit code while diagnosing. Follow `Flow.md` and correlate Task Scheduler, `scheduled-runner.log`, the run ID, MySQL Refresh events, refresh CSV, Email events, `email-runner.log`, and `ftp-runner.log` as applicable.
- Do not change the logical run-date rule casually. Refresh skipping, email eligibility, duplicate suppression, database rows, and exported filenames depend on it.
- Do not change cross-file parameters, exit-code behavior, scheduling handoffs, concurrency, or external integrations without updating `Flow.md`. Record significant trade-offs in `Decisions.md` and update this handover before ending the session.
