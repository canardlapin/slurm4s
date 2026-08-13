# Plan: local coding-agent control of a remote HPC workspace

Status: proposed

## Outcome

Build a separate local application, working name `hpc`, that gives humans and coding agents one
bounded view of a remote Slurm site:

- Slurm queue discovery and known-job control through slurm4s;
- a rooted, typed remote-workspace API for files;
- a human CLI and an MCP server over the same application service.

This is an application above slurm4s, not another slurm4s module. Filesystem browsing, editing,
configuration, and agent-tool presentation are not Slurm semantics and should not enter the
slurm4s dependency graph or release contract.

## Language and repository

Use a separate Scala 3/JVM sbt repository named `hpc` (final product name can change). Scala keeps
the Slurm path type-safe: the application consumes `slurm4s-core`, `slurm4s-protocol`, and
`slurm4s-ssh` directly instead of translating their ADTs through a subprocess API. Cats Effect and
FS2 remain the single resource-lifecycle model.

Implement MCP in the JVM application through the official
[`modelcontextprotocol/java-sdk`](https://github.com/modelcontextprotocol/java-sdk). Keep Reactor
and SDK DTOs inside `hpc-mcp`; adapt once to the Cats Effect application algebra. There is no need
for a TypeScript sidecar in v1. A web UI may later be a separate TypeScript client without moving
control semantics out of Scala.

Suggested repository layout:

```text
hpc/
  modules/core/                 product model and HpcService algebra
  modules/workspace-protocol/   bounded, versioned filesystem wire values
  modules/workspace-agent/      fixed remote stdio service
  modules/ssh/                  system-OpenSSH workspace transport
  modules/cli/                  hpc command and canonical JSON output
  modules/mcp/                  local MCP stdio adapter
  modules/testkit/              fake site, workspace, transport, and fault programs
```

`remote-exec-kernel` supplies existing provider-neutral bounds, diagnostics, freshness, and atomic
file mechanics. The first workspace protocol stays in `hpc`; extract a smaller neutral contract
only after a second real consumer proves that its meaning is provider-independent. Sojourn is an
optional later adapter for site/store/artifact workflows, not a v1 dependency.

## Runtime shape

```text
coding agent -> hpc mcp --stdio --+
human shell  -> hpc ... ----------+-> HpcService (local Scala process)
                                      |-> slurm4s SSH -> slurm4s-agent -> Slurm CLI
                                      +-> workspace SSH -> hpc-workspace-agent -> rooted files
```

Both remote channels use the user's system OpenSSH configuration. They do not read keys, replace
host-key policy, or interpolate a shell command. One fixed remote command is allowed on each
channel. OpenSSH connection sharing may amortize startup.

Do not mount the cluster with SSHFS and call that the agent contract. A POSIX-looking mount hides
disconnection, symlink escape, stale metadata, partial writes, and remote authority. The product
should expose those conditions explicitly.

## Application algebra

`HpcService[F]` composes capabilities; it does not expose `exec(command: String)`:

```scala
trait HpcService[F[_]]:
  def jobs: QueueReader[F]
  def scheduler: Scheduler[F]
  def workspace: RemoteWorkspace[F]

trait RemoteWorkspace[F[_]]:
  def roots: F[WorkspaceResult[Vector[WorkspaceRoot]]]
  def list(root: RootId, path: RelativePath, page: DirectoryPage):
    F[WorkspaceResult[DirectoryListing]]
  def stat(root: RootId, path: RelativePath): F[WorkspaceResult[RemoteEntry]]
  def read(root: RootId, path: RelativePath, cursor: FileCursor, maximum: ByteLimit):
    F[WorkspaceResult[FilePage]]
  def write(request: AtomicWriteRequest): F[WorkspaceResult[RemoteFileVersion]]
```

Directory pages and file reads have independent bounds. A directory continuation token names only
an ordering position and directory version; if the directory changes, continuation reports a
stale listing instead of composing two states silently. File pages carry a version identity so a
resumed read cannot splice bytes from two versions.

Writes are explicit tools, never a side effect of reading. They require either an expected file
version or an explicit create-only intent, stage to a private temporary file, force content, and
atomically replace within the same root. Multi-page upload is a begin/chunk/commit protocol with a
declared size and digest; it is not one oversized frame.

## Workspace authority

The remote agent starts with a private configuration mapping opaque root IDs to absolute paths,
for example `project` and `scratch`. Callers transmit a `RootId` plus a validated relative path,
never an arbitrary absolute path.

For every operation the remote interpreter must:

1. reject empty components, `.`/`..`, NUL, control characters, and absolute paths;
2. resolve beneath the configured root and refuse symlinks by default;
3. refuse sockets, devices, and other special files;
4. apply entry-count, byte, time, and frame limits before allocation;
5. return permission, missing, changed, truncated, and unavailable as distinct data;
6. retain bounded evidence without returning secrets or whole file contents as diagnostics.

An optional root may later allow symlinks, but every hop must still resolve beneath that root. The
agent runs with the SSH user's ordinary permissions and offers no privilege escalation.

## CLI and MCP surfaces

The CLI is a stable automation interface as well as a human tool. Every read command supports
canonical JSON on stdout; diagnostics go to stderr and use documented exit categories.

Initial commands:

```text
hpc roots
hpc fs list ROOT PATH --limit N
hpc fs stat ROOT PATH
hpc fs read ROOT PATH --offset N --max-bytes N
hpc fs write ROOT PATH --if-version V --from FILE
hpc jobs list --name NAME --partition PARTITION --state STATE --limit N
hpc jobs show JOB
hpc jobs cancel JOB
hpc logs read JOB STREAM --offset N --max-bytes N
hpc mcp --stdio
```

MCP registers the same operations as narrowly described tools. Read-only tools and mutating tools
remain visibly separate. Result schemas reuse the application model; the MCP adapter does not
parse human CLI text or invent a second error model. MCP resources can present root and file URIs,
but all reads still pass through `RemoteWorkspace` bounds and authority checks.

Local configuration belongs under the platform configuration directory and names SSH targets,
root aliases, default limits, and feature policy. It contains no private keys or copied tokens.

## Delivery order and acceptance

1. **slurm4s queue discovery.** Land `QueueReader.listJobs`, fixed `squeue` argv, parser, negotiated
   protocol feature, frame-aware page maximum, and local/remote conformance.
2. **hpc skeleton.** Create the separate repo, pure application algebra, deterministic testkit,
   config model, and canonical JSON golden files. No real filesystem mutation yet.
3. **workspace read path.** Implement roots, list, stat, and paged reads in a local interpreter,
   fixed remote agent, loopback SSH transport, and reconnect/version tests.
4. **workspace write path.** Add compare-and-swap atomic writes and chunked upload with crash-edge,
   symlink-race, size, digest, and permission tests.
5. **product adapters.** Add CLI commands, then MCP stdio over the same service. Run the official MCP
   conformance suite and prove stdout contains protocol bytes only.
6. **real-site evidence.** Exercise an unprivileged account through its ordinary SSH path and record
   exact Slurm, Java, OpenSSH, filesystem, configuration, feature, and limitation evidence.

The first usable milestone is read-only: queue listing plus rooted file list/stat/read. Mutation is
not admitted until atomicity and path-confinement tests pass. A release claim requires unit and
loopback evidence plus one real-site run; local green tests alone do not establish site support.

## Deferred non-goals

- arbitrary remote shell execution;
- a transparent remote filesystem mount;
- cluster-wide or other-user queue enumeration;
- privileged file access;
- snapshot queue pagination that Slurm cannot guarantee;
- IDE UI, file watching, and collaborative editing;
- automatic artifact-store or Sojourn integration.
