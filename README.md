# task-protocol

Lispy **CLOS** durable-task journal for [cl-stack](https://github.com/egao1980/cl-stack) (Temporal-shaped, not a workflow DSL). System name `task-protocol`, nickname `stack-task`.

| System | Role | Repo |
|--------|------|------|
| `task-protocol` (`stack-task`) | Protocol + in-memory journal | this repo |
| `task-backend-sql` | SQL journal + worker leases | [`egao1980/task-backend-sql`](https://github.com/egao1980/task-backend-sql) |

Core `:depends-on ()`. Events serialize as **plists (sexp)**. `serdes-protocol` / `json-protocol` / `datetime-protocol` are soft-used when already loaded — not hard deps.

```lisp
(asdf:load-system "task-protocol")

(let ((journal (stack-task:make-in-memory-journal))
      (task (stack-task:make-durable-task :id "demo")))
  (stack-task:with-durable-task (task journal)
    (stack-task:with-durable-step ("fetch" :idempotency-key "url-1")
      '(:ok t))
    (stack-task:schedule-wake task (+ (get-universal-time) 60))
    (stack-task:complete-task task :done)))
```

| Role | API |
|------|-----|
| Task | `durable-task` — `id`, parent, `status` (`:new :running :waiting :completed :failed :canceled`), `retry-policy` |
| Journal | `append-event` / `replay-journal` / `journal-events` / `journal-hash` |
| Steps | `with-durable-step` — live exec + journal; resume returns the recorded result |
| Timers | `schedule-wake` / `schedule-recurring` / `fire-due-timers` |
| Trees | `spawn-child-task` / `join-children` (`:all` `:any` `:quorum`) |
| Governance | `compact-journal`, `retention-policy`, `redact-event` / `make-redaction-policy` (A7 reuses the policy) |

In-memory backend: `in-memory-journal`, hash table of task-id → event list.

Conditions: `task-error`, `task-replay-divergence` (`journal-hash` slot), `task-serialization-error`, `task-timeout`. Restarts: `retry-step`, `skip-step`, `abort-task`, plus `use-value` on serialization.

`ai-agent-protocol` `:durability` is a later consumer — not implemented here.

## License

MIT — see [LICENSE](LICENSE).
