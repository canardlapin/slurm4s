# ADR 0003: Bounded agent protocol over system OpenSSH

- Status: accepted
- Date: 2026-07-22
- Mote: `bd-01KY5W5BT115F7RYAE2YFZFMQR`

## Decision

Remote control uses the user's system OpenSSH client and a small Scala agent on the HPC login
host. The local process argv ends in the invariant command
`TARGET scala-slurm-agent serve --stdio`. It includes `-T -o RequestTTY=no`; workload names,
scripts, arguments, resource requests, job identifiers, log locators, and cursors travel only in
protocol frames. Connection configuration is limited to typed OpenSSH options before the target.
A target cannot begin with `-` or contain whitespace or control characters.

The library does not implement SSH, read private keys, persist credentials, force `BatchMode`, or
replace host-key policy. Normal OpenSSH configuration remains authoritative for identities,
agents, known hosts, proxy jumps, control sockets, and interactive authentication. The
[OpenBSD ssh manual](https://man.openbsd.org/OpenBSD-7.7/ssh.1) specifies that `-T` disables
pseudo-terminal allocation and that standard input/output are forwarded to a remote command. The
[OpenBSD ssh_config manual](https://man.openbsd.org/OpenBSD-current/man/ssh_config) documents the
configuration surface that remains under user/site control.

The initial client uses one SSH process per request. This is less efficient than multiplexing but
gives every operation a bounded resource lifetime and makes reconnection semantics unambiguous.
OpenSSH's own configured connection sharing may still amortize transport setup. Protocol-level
multiplexing can be added behind the same algebra after it has cancellation and correlation laws.

## Wire contract

Each frame is a four-byte, network-order, positive payload length followed by canonical UTF-8
JSON. A peer rejects zero, negative, oversized, truncated, malformed, or extra frames. The
decoder accepts arbitrarily fragmented input and multiple coalesced frames without allocating an
advertised oversized payload. Envelopes carry a request ID, `{major, minor}` protocol version,
message kind, method or response status, body, and retained additive extensions.

Handshake negotiates the smaller frame limit and an intersection of features. Major-version
mismatch is a distinct value. The P2 method set is capabilities, opaque ExitOnly script
submission, observation, accounting, cancellation, and bounded log reads. Registered typed tasks
and typed result envelopes remain a P4 protocol addition; they are rejected rather than silently
treated as opaque scripts.

The agent executable requires `SCALA_SLURM_WORKSPACE`, assembles the same `SlurmCliScheduler` used
locally, confines script and log access to that private workspace, and emits protocol bytes only
on stdout while serving. It invokes the standard Slurm commands by argv, never through a shell.
The stdio server can accept fragmented or coalesced frames and drains its response stream through
FS2.

## Failures and reconnects

The remote API keeps the following diagnostic planes disjoint:

| Failure | Evidence and meaning |
| --- | --- |
| `ProtocolMismatch` | A valid handshake named an incompatible major version |
| `AgentUnavailable` | The SSH executable could not start, or the fixed remote agent command was absent |
| `AuthenticationFailed` | OpenSSH exit 255 plus a recognized authentication diagnostic |
| `TransportDisconnected` | No valid response arrived; records whether the complete request frame was written |
| `RemoteCliFailure` | A valid agent response classified a remote operation failure |
| `ProtocolViolation` | Framing, JSON, correlation, schema, or response-shape failure |

OpenSSH stderr is always retained separately and bounded. Authentication recognition is
deliberately conservative: an unrecognized exit 255 remains a transport disconnection instead of
being mislabeled. A disconnect never calls `scancel`. If submission acceptance is not available
to the client, existing core semantics preserve it as unknown rather than authorizing a retry.

Logs remain page based. Each independent connection supplies the caller's byte cursor and file
identity; reconnecting therefore resumes from durable caller state and does not depend on an SSH
stream remaining alive. FS2 may expose repeated page reads as a live stream, but that stream is
not durable state.

## Degraded direct SSH

Direct SSH compatibility is an explicit mode, never an automatic semantic substitute for the
agent. Its capability value states that protocol frames, agent-paged logs, and durable control are
unavailable and carries degradation reasons. Callers must opt into those weaker guarantees.

## Executable evidence

- The framing suite covers every two-part split, coalesced frames, pre-allocation size rejection,
  zero lengths, and truncated EOF.
- The stdio suite drives a handshake through fragmented frames and checks the exact fixed command.
- One scheduler scenario suite runs capabilities, submission, observation, accounting, and
  cancellation directly and through encoded agent frames, then compares the complete domain
  results.
- A reconnect test reads consecutive log pages through three independent SSH exchanges
  (handshake plus two page calls).
- Failure tests distinguish authentication failure, missing agent, disconnection, remote CLI
  failure, and protocol-major mismatch.
- A disconnect-after-acceptance test proves that losing a client session does not invoke
  cancellation.

