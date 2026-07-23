package io.github.bbuchsbaum.scalaslurm.cli

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

final case class CommandPolicy(
    timeout: io.github.bbuchsbaum.scalaslurm.core.DurationMillis,
    captureLimit: io.github.bbuchsbaum.scalaslurm.core.ByteLimit
) derives CanEqual

trait CommandExecutor[F[_]]:
  def execute(
      command: SlurmCommand,
      policy: CommandPolicy
  ): F[io.github.bbuchsbaum.scalaslurm.core.InvocationResult]
