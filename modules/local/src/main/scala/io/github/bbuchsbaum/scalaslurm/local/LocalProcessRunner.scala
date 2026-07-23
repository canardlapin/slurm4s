package io.github.bbuchsbaum.scalaslurm.local

import cats.effect.Resource
import fs2.Stream
import io.github.bbuchsbaum.scalaslurm.cli.SlurmCommand
import io.github.bbuchsbaum.scalaslurm.core.InvocationResult

/** A scoped local OS process. This resource scope never represents the lifetime of a Slurm job. */
trait RunningCommand[F[_]]:
  def stdout: Stream[F, Byte]
  def stderr: Stream[F, Byte]
  def result: F[InvocationResult]

trait LocalProcessRunner[F[_]]:
  def start(command: SlurmCommand): Resource[F, RunningCommand[F]]
