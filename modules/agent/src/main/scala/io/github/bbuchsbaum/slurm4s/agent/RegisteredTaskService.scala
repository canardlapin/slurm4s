package io.github.bbuchsbaum.slurm4s.agent

import cats.effect.IO
import io.github.bbuchsbaum.slurm4s.core.*
import io.github.bbuchsbaum.slurm4s.protocol.*
import io.github.bbuchsbaum.slurm4s.worker.RegisteredTaskLauncher

final class WorkerRegisteredTaskService(
    launcher: RegisteredTaskLauncher,
    scheduler: Scheduler[IO]
) extends AgentRegisteredTaskService[IO]:
  def submit(
      request: RemoteRegisteredTaskRequest
  ): IO[AgentCall[RemoteRegisteredSubmission]] =
    launcher.prepareRemote(request).flatMap {
      case Left(diagnostics) =>
        IO.pure(
          AgentCall.Failed(
            AgentFailure.RemoteAgentFailure(
              diagnostics.toVector.map(_.code).mkString(",").take(512),
              None
            )
          )
        )
      case Right(prepared) =>
        scheduler.submitLowered(prepared.schedulerRequest).map { submission =>
          AgentCall.Succeeded(
            RemoteRegisteredSubmission(
              prepared.resultRef,
              prepared.resultHandle,
              submission
            )
          )
        }
    }

  def submitBatch(
      request: RemoteRegisteredBatchRequest
  ): IO[AgentCall[RemoteRegisteredBatchSubmission]] =
    launcher.prepareRemoteBatch(request).flatMap {
      case Left(diagnostics) =>
        IO.pure(
          AgentCall.Failed(
            AgentFailure.RemoteAgentFailure(
              diagnostics.toVector.map(_.code).mkString(",").take(512),
              None
            )
          )
        )
      case Right(prepared) =>
        scheduler.submitLowered(prepared.schedulerRequest).map { submission =>
          AgentCall.Succeeded(
            RemoteRegisteredBatchSubmission(
              prepared.topology,
              prepared.elements.map { case (index, element) =>
                RemoteRegisteredBatchElementSubmission(
                  index,
                  element.resultRef,
                  element.resultHandle,
                  io.github.bbuchsbaum.slurm4s.core.LogRef(
                    element.invocation.attemptId,
                    element.invocation.attemptEpoch,
                    io.github.bbuchsbaum.slurm4s.core.LogStream.Stdout,
                    element.invocationPath.getParent.resolve("stdout.log").toString
                  ),
                  io.github.bbuchsbaum.slurm4s.core.LogRef(
                    element.invocation.attemptId,
                    element.invocation.attemptEpoch,
                    io.github.bbuchsbaum.slurm4s.core.LogStream.Stderr,
                    element.invocationPath.getParent.resolve("stderr.log").toString
                  )
                )
              },
              submission
            )
          )
        }
    }

  def submitScriptBatch(
      request: RemoteScriptBatchRequest
  ): IO[AgentCall[RemoteScriptBatchSubmission]] =
    launcher.prepareRemoteScriptBatch(request).flatMap {
      case Left(diagnostics) =>
        IO.pure(
          AgentCall.Failed(
            AgentFailure.RemoteAgentFailure(
              diagnostics.toVector.map(_.code).mkString(",").take(512),
              None
            )
          )
        )
      case Right(prepared) =>
        scheduler.submitLowered(prepared.schedulerRequest).map { submission =>
          AgentCall.Succeeded(
            RemoteScriptBatchSubmission(
              prepared.topology,
              prepared.elements.map(element =>
                RemoteScriptBatchElementSubmission(
                  element.index,
                  element.exitRef,
                  element.stdout,
                  element.stderr
                )
              ),
              submission
            )
          )
        }
    }

  def readResult(
      ref: RemoteResultRef,
      maximumBytes: ByteLimit
  ): IO[AgentCall[RemoteResultRead]] =
    launcher.readRemoteResult(ref, maximumBytes).map(AgentCall.Succeeded.apply)

  def readScriptExit(
      ref: RemoteScriptExitRef
  ): IO[AgentCall[RemoteScriptExitRead]] =
    launcher.readRemoteScriptExit(ref).map(AgentCall.Succeeded.apply)
