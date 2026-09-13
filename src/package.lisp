(defpackage #:task-protocol
  (:use #:cl)
  (:nicknames #:stack-task)
  (:export #:task-error
           #:task-error-message
           #:task-error-task
           #:task-replay-divergence
           #:task-replay-divergence-journal-hash
           #:task-replay-divergence-expected
           #:task-replay-divergence-actual
           #:task-serialization-error
           #:task-serialization-error-value
           #:task-timeout
           #:task-timeout-deadline
           #:retry-step
           #:skip-step
           #:abort-task

           #:durable-task
           #:durable-task-p
           #:make-durable-task
           #:durable-task-id
           #:durable-task-parent
           #:durable-task-status
           #:durable-task-retry-policy
           #:durable-task-result
           #:durable-task-input
           #:durable-task-journal
           #:durable-task-children
           #:durable-task-quorum
           #:durable-task-error

           #:retry-policy
           #:make-retry-policy
           #:retry-policy-max-attempts
           #:retry-policy-backoff-seconds
           #:retry-policy-timeout

           #:*task*
           #:*journal*

           #:task-event
           #:event-task-id
           #:event-timestamp
           #:event-seq
           #:event-plist
           #:event-from-plist
           #:event-type-keyword
           #:copy-event
           #:encode-payload
           #:decode-payload

           #:step-completed
           #:step-name
           #:step-result
           #:step-idempotency-key
           #:timer-set
           #:timer-time
           #:timer-spec
           #:timer-recurring-p
           #:timer-fired
           #:child-spawned
           #:event-child-id
           #:event-child-input
           #:children-joined
           #:join-policy
           #:join-result
           #:wait-input
           #:wait-prompt
           #:task-started
           #:task-completed
           #:event-result
           #:task-failed
           #:event-reason
           #:journal-snapshot
           #:snapshot-status
           #:snapshot-result
           #:snapshot-steps
           #:snapshot-event-count

           #:append-event
           #:replay-journal
           #:journal-events
           #:journal-task-ids
           #:import-events
           #:journal-hash
           #:apply-event
           #:compact-journal

           #:with-durable-task
           #:with-durable-step
           #:serializable-p
           #:canonicalize-value
           #:complete-task
           #:fail-task
           #:cancel-task
           #:request-input
           #:check-task-timeout

           #:schedule-wake
           #:schedule-recurring
           #:fire-due-timers
           #:compute-next-fire

           #:spawn-child-task
           #:join-children

           #:in-memory-journal
           #:make-in-memory-journal
           #:copy-in-memory-journal
           #:in-memory-journal-p

           #:retention-policy
           #:make-retention-policy
           #:retention-policy-max-age
           #:retention-policy-max-count
           #:retention-policy-per-class
           #:journal-retention-policy
           #:journal-redaction-policy

           #:redaction-policy
           #:make-redaction-policy
           #:redact-event
           #:*redaction-policy*
           #:+default-sensitive-keys+))

(in-package #:task-protocol)
