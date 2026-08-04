# Pre-deadline termination notices

`JobRequest.terminationNotice` requests one typed Slurm advance signal. The signal, target scope, and
lead time are validated before submission and lowered to one `sbatch --signal=...` argument. An
absent notice remains absent from canonical request JSON for compatibility with existing managed
journals and older agents.

SSH clients negotiate `termination-notices` during the agent handshake. A request that contains a
notice is refused locally when the target agent did not advertise that feature; it is never sent to
an older peer that could ignore the field and submit different semantics.

```scala
val notice = TerminationNotice(
  TerminationNoticeSignal.Usr1,
  TerminationNoticeScope.BatchShell,
  SignalLeadSeconds.unsafeFrom(120)
)
val request = existingRequest.copy(terminationNotice = Some(notice))
```

This is advance notice, not a graceful-shutdown guarantee. Slurm may deliver the signal earlier than
the requested lead time, and its later terminal SIGTERM/SIGKILL sequence is separate. Applications
must therefore make drain handling idempotent and tolerate early delivery.

The worker module exposes `DrainNoticeSource` as an adapter seam. A signal-aware supervising
launcher can publish a notice through an injected effect or a marker file; `TaskContext.drainNotice`
exposes that optional capability to application code. slurm4s does not install a JVM signal
handler, stop admission, cancel work, or choose when a partial result is publishable. Pilot and
drain policy remain in the application layer.
