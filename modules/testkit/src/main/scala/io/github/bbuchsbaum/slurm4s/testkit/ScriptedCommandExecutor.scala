package io.github.bbuchsbaum.slurm4s.testkit

import cats.effect.Ref
import cats.effect.Sync
import cats.syntax.all.*
import io.github.bbuchsbaum.slurm4s.cli.CommandExecutor
import io.github.bbuchsbaum.slurm4s.cli.CommandPolicy
import io.github.bbuchsbaum.slurm4s.cli.SlurmCommand
import io.github.bbuchsbaum.slurm4s.core.InvocationResult

final case class ExpectedCommand(
    description: String,
    accepts: SlurmCommand => Boolean,
    acceptsPolicy: CommandPolicy => Boolean,
    result: InvocationResult
)

object ExpectedCommand:
  def exact(command: SlurmCommand, result: InvocationResult): ExpectedCommand =
    ExpectedCommand(command.toString, _ == command, _ => true, result)

  def exact(
      command: SlurmCommand,
      policy: CommandPolicy,
      result: InvocationResult
  ): ExpectedCommand =
    ExpectedCommand(s"$command with policy $policy", _ == command, _ == policy, result)

final case class ObservedCommand(command: SlurmCommand, policy: CommandPolicy) derives CanEqual

final class ScriptedCommandExecutor[F[_]: Sync] private (
    remainingRef: Ref[F, Vector[ExpectedCommand]],
    observedRef: Ref[F, Vector[ObservedCommand]]
) extends CommandExecutor[F]:
  def execute(command: SlurmCommand, policy: CommandPolicy): F[InvocationResult] =
    observedRef.update(_ :+ ObservedCommand(command, policy)) *>
      remainingRef
        .modify {
          case head +: tail if head.accepts(command) && head.acceptsPolicy(policy) =>
            tail -> Right(head.result)
          case steps @ (head +: _) =>
            steps -> Left(
              new AssertionError(
                s"command or policy did not match ${head.description}: $command with policy $policy"
              )
            )
          case empty =>
            empty -> Left(new AssertionError(s"unexpected command: $command"))
        }
        .flatMap(_.liftTo[F])

  def remaining: F[Vector[ExpectedCommand]] = remainingRef.get
  def observed: F[Vector[ObservedCommand]] = observedRef.get

object ScriptedCommandExecutor:
  def create[F[_]: Sync](expected: Vector[ExpectedCommand]): F[ScriptedCommandExecutor[F]] =
    (
      Ref.of[F, Vector[ExpectedCommand]](expected),
      Ref.of[F, Vector[ObservedCommand]](Vector.empty)
    )
      .mapN(new ScriptedCommandExecutor(_, _))
