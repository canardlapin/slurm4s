package io.github.bbuchsbaum.scalaslurm.testkit

import cats.effect.Ref
import cats.effect.Sync
import cats.syntax.all.*
import io.github.bbuchsbaum.scalaslurm.cli.CommandExecutor
import io.github.bbuchsbaum.scalaslurm.cli.CommandPolicy
import io.github.bbuchsbaum.scalaslurm.cli.SlurmCommand
import io.github.bbuchsbaum.scalaslurm.core.InvocationResult

final case class ExpectedCommand(
    description: String,
    accepts: SlurmCommand => Boolean,
    result: InvocationResult
)

object ExpectedCommand:
  def exact(command: SlurmCommand, result: InvocationResult): ExpectedCommand =
    ExpectedCommand(command.toString, _ == command, result)

final class ScriptedCommandExecutor[F[_]: Sync] private (
    remainingRef: Ref[F, Vector[ExpectedCommand]],
    observedRef: Ref[F, Vector[SlurmCommand]]
) extends CommandExecutor[F]:
  def execute(command: SlurmCommand, policy: CommandPolicy): F[InvocationResult] =
    observedRef.update(_ :+ command) *>
      remainingRef
        .modify {
          case head +: tail => tail -> Right(head)
          case empty        => empty -> Left(new AssertionError(s"unexpected command: $command"))
        }
        .flatMap {
          case Left(error)                                  => Sync[F].raiseError(error)
          case Right(expected) if expected.accepts(command) => expected.result.pure[F]
          case Right(expected)                              =>
            Sync[F].raiseError(
              new AssertionError(s"command did not match ${expected.description}: $command")
            )
        }

  def remaining: F[Vector[ExpectedCommand]] = remainingRef.get
  def observed: F[Vector[SlurmCommand]] = observedRef.get

object ScriptedCommandExecutor:
  def create[F[_]: Sync](expected: Vector[ExpectedCommand]): F[ScriptedCommandExecutor[F]] =
    (Ref.of[F, Vector[ExpectedCommand]](expected), Ref.of[F, Vector[SlurmCommand]](Vector.empty))
      .mapN(new ScriptedCommandExecutor(_, _))
