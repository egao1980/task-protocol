# task-protocol

Lispy **CLOS** durable-task journal for [cl-stack](https://github.com/egao1980/cl-stack) (Temporal-shaped, not a workflow DSL). System name `task-protocol`, nickname `stack-task`.

| System | Role | Repo |
|--------|------|------|
| `task-protocol` (`stack-task`) | Protocol + in-memory journal | this repo |
| `task-backend-sql` | SQL journal + worker leases | [`egao1980/task-backend-sql`](https://github.com/egao1980/task-backend-sql) |

Core `:depends-on ()`. Events serialize as a **versioned envelope** (`:schema-version` + `:codec`). The default codec is **`:sexp-plist`**. Consumers **register decoders** (`register-event-codec`); `decode-event` never guesses a vector as a plist. Built-in `:json-object` maps hash-table → object, vector → array (json-protocol Lisp shape). `serdes-protocol` / `json-protocol` / `datetime-protocol` are soft-used when already loaded — not hard deps.

```lisp
(asdf:load-system "task-protocol")

(let ((journal (stack-task:make-in-memory-journal))
      (task (stack-task:make-durable-task :id "demo")))
  (stack-task:with-durable-task (task journal)
    (stack-task:with-durable-step ("fetch" :idempotency-key "url-1"
                                  :run-id (stack-task:make-run-id "run-1")
                                  :activation-id (stack-task:make-activation-id "act-1"))
      '(:ok t))
    (stack-task:schedule-wake task (+ (get-universal-time) 60))
    (stack-task:complete-task task :done)))
```

| Role | API |
|------|-----|
| Task | `durable-task` — `id`, parent, `status` (`:new :running :waiting :completed :failed :canceled`), `retry-policy` |
| Journal | `append-event` / `replay-journal` / `journal-events` / `journal-hash` |
| Steps | `with-durable-step` — live exec + journal; resume returns the recorded result. Optional `:run-id` / `:activation-id` scope the recorded-step key (absent = 0.1.0 name+idempotency-key) |
| Identity | `run-id` / `activation-id` CLOS objects — `make-run-id` / `make-activation-id`; also slots on `durable-task` and journaled events |
| Effects | `effect-receipt` — `record-effect-receipt` journals side effects separately from the step return value |
| Versions | `schema-version` / `code-version` / `config-version` stamps on events (`*schema-version*` default `"0.2.1"`); mismatch on replay signals `task-replay-divergence` (`continue` to accept) |
| Codec | `encode-event` / `decode-event` / `register-event-codec` — `:sexp-plist` (default) and `:json-object`; arrays stay arrays (`#("a" "b")` is not a plist) |
| Timers | `schedule-wake` / `schedule-recurring` / `fire-due-timers` |
| Trees | `spawn-child-task` / `join-children` (`:all` `:any` `:quorum`) |
| Governance | `compact-journal`, `retention-policy`, `redact-event` / `make-redaction-policy` (A7 reuses the policy) |
| Runtime | `runtime-transition` / `journal-runtime-transition` — execution-plane phases; does not mutate durable-task status. Secret refs only. |

In-memory backend: `in-memory-journal`, hash table of task-id → event list.

Conditions: `task-error`, `task-replay-divergence` (`journal-hash` slot), `task-serialization-error`, `task-timeout`, `task-unknown-codec`. Restarts: `retry-step`, `skip-step`, `abort-task`, plus `use-value` on serialization and unknown codecs.

`ai-agent-protocol` `:durability` is a later consumer — not implemented here.

## 0.3.0 (contract): job queue, resource classes, progress

A queue on the task model for a web service that runs a few expensive
batch jobs (each needing a multi-GB heap, say) behind a live progress
page. Issues [#5](https://github.com/egao1980/task-protocol/issues/5)
(queue API), [#6](https://github.com/egao1980/task-protocol/issues/6)
(resource classes and admission), [#7](https://github.com/egao1980/task-protocol/issues/7)
(progress events and subscription).

This section is a contract: the protocol is written down and tested, the
in-memory backend is stubbed (every method signals `task-error` "… is not
implemented yet"). The generic-function docstrings in `src/jobs.lisp` are
the spec; the SQL backend (`task-backend-sql` 0.2) implements the same
generic functions later.

```lisp
(let ((q (stack-task:make-in-memory-job-queue
          :resource-classes '((:heap-4g 3)))))
  ;; web handler
  (let ((id (stack-task:submit-job q :render :payload '(:input "a.dat")
                                   :priority 1 :resource-class :heap-4g)))
    ;; live page: replay from the client's last seen seq, then live
    (stack-task:subscribe-job-events q id #'send-to-page :after-seq 0)
    ;; worker loop
    (let ((job (stack-task:claim-job q "worker-1")))
      (when job
        (stack-task:report-job-progress q (stack-task:job-id job) "worker-1"
                                        :phase "load" :fraction 1/4)
        (if (stack-task:cancel-requested-p q (stack-task:job-id job))
            (stack-task:acknowledge-cancel q (stack-task:job-id job) "worker-1")
            (stack-task:complete-job q (stack-task:job-id job) "worker-1"
                                     :result '(:ok t)))))))
```

| Role | API |
|------|-----|
| Job | `job` snapshot: `job-id` `job-kind` `job-payload` `job-priority` `job-resource-class` `job-state` (`:queued :running :completed :failed :canceled`) `job-submitted-at` `job-started-at` `job-finished-at` `job-updated-at` `job-result` `job-failure` `job-worker-id` `job-lease-until` `job-attempts` `job-cancel-requested` |
| Client | `submit-job` → id · `job-status` · `list-jobs` (`:state` `:kind` `:resource-class`) · `cancel-job` (`:reason` `:force`) · `cancel-requested-p` |
| Worker | `claim-job` (`:kinds` `:lease-seconds`) · `heartbeat-job` · `complete-job` · `fail-job` · `acknowledge-cancel` · `expire-job-leases` |
| Admission | `define-resource-class` (name, limit ≥ 0) · `find-resource-class` · `list-resource-classes` · `resource-class-usage` |
| Progress | `report-job-progress` (`:phase` `:fraction` 0–1 `:message`) · `append-job-event` · `job-events` (`:after-seq`) · `subscribe-job-events` (`:after-seq`) · `unsubscribe-job-events` |
| Events | `job-transition` (from, to, worker, reason) and `job-progress`, journaled under the job id, round-trip through `event-plist` / `encode-event` like every other event; `*schema-version*` is `"0.3.0"` |
| State machine | `job-transition-allowed-p`, `job-terminal-state-p` (one table, `+job-transitions+`) |

Semantics in short (full text in `src/jobs.lisp`):

- **Order.** `claim-job` takes the highest priority (larger first, default
  0), FIFO within a priority. A job whose resource class is full is
  skipped and blocks nothing behind it. A job re-queued by lease expiry
  keeps its place.
- **Admission.** A claim is admitted only while the class has fewer
  running jobs than its limit, atomically across threads. The slot is
  released when the job leaves `:running`: complete, fail, acknowledged
  or forced cancel, lease expiry. Lowering a limit preempts nothing.
- **Cancel.** A queued job is canceled at once. A running job gets the
  cooperative flag (`cancel-requested-p`) and stays running until the
  worker calls `acknowledge-cancel`; `:force t` cancels it at once (a
  backend that killed the process).
- **Leases.** Same vocabulary as `task-backend-sql`: `worker-id`,
  `lease-seconds` (default 30), `heartbeat-job`; expired when
  `lease-until < now`. `claim-job` expires leases first. Every operation
  takes `:now` (universal time) for deterministic tests.
- **Journal.** Every state change appends one `job-transition`; progress
  appends `job-progress`; seqs are the journal's (1, 2, …).
- **Subscription.** Replay of seq > `after-seq`, then live, in seq order,
  each event once, no gaps, also under concurrent appends. A reconnecting
  client passes the last seq it saw (SSE `Last-Event-ID`).
- **Conditions.** `job-error` (a `task-error`) → `job-not-found`
  (`use-value`), `job-state-error` (`continue`) → `job-lease-lost`,
  `unknown-resource-class` (`use-value`, re-checked), `job-progress-error`
  (`use-value`, re-checked). Payloads and results go through
  `canonicalize-value` (`task-serialization-error`, `use-value`). A
  failing subscriber callback signals in the appender inside a `continue`
  restart that skips that delivery.

Core still has `:depends-on ()`. The in-memory queue locks with the
implementation's own recursive mutex (`src/lock.lisp`, SBCL / ECL / CCL);
the tests use `bordeaux-threads`, already in the test graph through Rove.

| File | Status |
|------|--------|
| `src/conditions.lisp` (job conditions) | given |
| `src/events.lisp` (`job-transition`, `job-progress`, codec) | given |
| `src/lock.lisp` | given |
| `src/jobs.lisp` (model, state table, generic functions, spec) | given |
| `src/queue-memory.lisp` (`in-memory-job-queue`, constructor) | given |
| `src/queue-memory.lisp` (every method) | stubbed |
| `tests/job-support.lisp` | given (helpers) |
| `tests/job-queue-test.lisp` | contract |
| `tests/job-admission-test.lisp` | contract |
| `tests/job-progress-test.lisp` | contract |
| `tests/job-conditions-test.lisp` | contract |

The job state machine is one data table behind
`job-transition-allowed-p`, kept minimal so it can move to the logic
family (`logic-protocol`) later.

## License

MIT — see [LICENSE](LICENSE).
