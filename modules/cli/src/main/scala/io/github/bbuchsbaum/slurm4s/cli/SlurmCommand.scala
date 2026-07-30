package io.github.bbuchsbaum.slurm4s.cli

enum SlurmExecutable(val fileName: String) derives CanEqual:
  case Sbatch extends SlurmExecutable("sbatch")
  case Squeue extends SlurmExecutable("squeue")
  case Sacct extends SlurmExecutable("sacct")
  case Scontrol extends SlurmExecutable("scontrol")
  case Scancel extends SlurmExecutable("scancel")

/** A process argument vector. It is never rendered as a shell command. */
final case class SlurmCommand(
    executable: SlurmExecutable,
    arguments: Vector[String],
    environment: Map[String, String] = Map.empty,
    workingDirectory: Option[String] = None
) derives CanEqual

/** Bounds applied to one Slurm command invocation.
  *
  * `captureLimit` and `evidenceLimit` are deliberately distinct. `captureLimit` is how much output
  * the interpreter reads, and therefore how much a parser can see: structured `squeue` output runs
  * to several kilobytes per job, so a capture bound sized for diagnostics silently truncates the
  * JSON and turns a healthy query into a parse failure. `evidenceLimit` is how much of that output
  * is retained afterwards for diagnosis, applied only once parsing is complete.
  */
final case class CommandPolicy(
    timeout: io.github.bbuchsbaum.slurm4s.core.DurationMillis,
    captureLimit: io.github.bbuchsbaum.slurm4s.core.ByteLimit,
    evidenceLimit: io.github.bbuchsbaum.slurm4s.core.ByteLimit =
      io.github.bbuchsbaum.slurm4s.core.ByteLimit.defaultEvidence
) derives CanEqual

trait CommandExecutor[F[_]]:
  def execute(
      command: SlurmCommand,
      policy: CommandPolicy
  ): F[io.github.bbuchsbaum.slurm4s.core.InvocationResult]
