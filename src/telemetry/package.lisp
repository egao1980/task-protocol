(defpackage #:task-protocol/telemetry
  (:use #:cl #:task-protocol)
  (:local-nicknames (#:tel #:telemetry-protocol))
  (:export #:instrument-task-step
           #:*task-span-link-trace-id*
           #:*task-span-link-span-id*)
  (:documentation
   "Optional span events for durable-task journal appends.
    Core stays dep-free. Load this system to emit step/timer events."))

(in-package #:task-protocol/telemetry)
