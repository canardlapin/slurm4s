# ADR 0009: Scheduler telemetry is bounded and non-authoritative

- Status: accepted
- Date: 2026-07-22
- Mote: `bd-01KY6FDWZ12VTWWBGZF0Y9K3X3`

## Decision

`slurm4s-observability` provides an optional `TelemetryScheduler[F]` decorator and
`SchedulerTelemetrySink[F]` algebra. The decorator records one completed operation summary for
capability discovery, submission, observation, accounting, and cancellation. A log, metrics, or
trace adapter can consume that stable summary without becoming a dependency of the scheduler
modules.

Events contain only operation kind, a minimal correlation subject, completion time, bounded
duration, typed outcome, and a configured maximum number of diagnostic codes. They cannot contain
raw evidence, stdout/stderr, script bytes, arguments, environment values, diagnostic messages or
fields, SSH configuration, or credentials.

Domain results remain exact. Rejected submission, acceptance uncertainty, invocation failure,
parse failure, empty query, and cancellation uncertainty have distinct telemetry outcomes. If
the underlying scheduler effect fails, telemetry records only its exception class and then
re-raises the original effect failure.

## Failure and ownership boundary

Sink delivery is best effort and has an explicit timeout. Sink failure or timeout is swallowed
after bounded delivery, so it cannot replace a scheduler result. The decorator owns no queue,
fiber, retry loop, durable state, or batching policy. A sink implementation that wants buffering
must supply and document its own bounded ownership.

Synchronous delivery can add at most the configured sink deadline after the scheduler effect.
Applications that require lower latency should use a bounded nonblocking adapter. Telemetry is
never consulted by managed recovery, result attachment, failure diagnosis, or scheduler parsing.
The durable control journal and raw bounded scheduler evidence remain authoritative.

## Adapters

The module deliberately does not make log4cats or OpenTelemetry runtime dependencies. An adapter
may translate `SchedulerOperationEvent` into:

- one structured operational log record;
- a counter by operation and typed outcome plus a duration histogram; or
- one completed span/event with the same redacted attributes.

Adapter-specific naming and exporters can evolve outside the scheduler contract. Raw workload
logs remain available only through explicit bounded log APIs.

## Executable evidence

The observability suite proves:

- rejection and parse failure remain their original domain values;
- diagnostic-code count is capped and messages/fields have no telemetry representation;
- failing and nonresponsive sinks cannot fail or indefinitely delay a successful call;
- underlying effect failure is recorded by class and re-raised unchanged; and
- cancellation uncertainty remains distinct and job-correlated.
