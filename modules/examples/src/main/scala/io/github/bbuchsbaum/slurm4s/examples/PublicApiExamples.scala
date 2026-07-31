package io.github.bbuchsbaum.slurm4s.examples

import cats.data.NonEmptyVector
import cats.data.ValidatedNec
import cats.effect.IO
import cats.effect.Resource
import cats.syntax.all.*
import fs2.io.process.Processes
import io.github.bbuchsbaum.slurm4s.protocol.AgentApi
import io.github.bbuchsbaum.slurm4s.batch.*
import io.github.bbuchsbaum.slurm4s.core.*
import io.github.bbuchsbaum.slurm4s.local.*
import io.github.bbuchsbaum.slurm4s.managed.*
import io.github.bbuchsbaum.slurm4s.protocol.AgentCall
import io.github.bbuchsbaum.slurm4s.protocol.FrameLimits
import io.github.bbuchsbaum.slurm4s.ssh.*
import io.github.bbuchsbaum.slurm4s.worker.*

import java.nio.charset.StandardCharsets
import java.nio.file.Path
import java.time.Instant
import scala.util.Try

object JobRequests:
  def resources(
      cpusPerTask: Int,
      tasks: Int,
      nodes: Option[Int] = None,
      memory: Option[MemoryRequest] = None,
      wallTime: Option[WallTimeMinutes] = None
  ): ValidatedNec[ValidationFailure, ResourceRequest] =
    ResourceRequest.validate(cpusPerTask, tasks, nodes, memory, wallTime)

  def exitOnly(
      submissionKey: String,
      name: String,
      source: ScriptSource,
      arguments: Vector[String],
      resources: ResourceRequest,
      environment: Map[String, String] = Map.empty
  ): ValidatedNec[ValidationFailure, JobRequest[NoResult]] =
    (
      SubmissionKey.from(submissionKey).toValidatedNec,
      JobName.from(name).toValidatedNec,
      environmentVariables(environment)
    ).mapN { (key, jobName, variables) =>
      JobRequest(
        key,
        jobName,
        Payload.Script(source, arguments, ResultContract.ExitOnly),
        resources,
        variables
      )
    }

  def declaredOutputs(
      submissionKey: String,
      name: String,
      source: ScriptSource,
      arguments: Vector[String],
      outputPaths: Vector[String],
      maximumManifestBytes: Int,
      resources: ResourceRequest,
      environment: Map[String, String] = Map.empty
  ): ValidatedNec[ValidationFailure, JobRequest[OutputManifest]] =
    val contract = (
      outputPaths.traverse(path => RelativeOutputPath.from(path).toValidatedNec),
      ByteLimit.from(maximumManifestBytes).toValidatedNec
    ).tupled.andThen { case (paths, maximum) =>
      ResultContract.DeclaredOutputs.from(paths, maximum).toValidatedNec
    }

    (
      SubmissionKey.from(submissionKey).toValidatedNec,
      JobName.from(name).toValidatedNec,
      contract,
      environmentVariables(environment)
    ).mapN { (key, jobName, resultContract, variables) =>
      JobRequest(
        key,
        jobName,
        Payload.Script(source, arguments, resultContract),
        resources,
        variables
      )
    }

  private def environmentVariables(
      values: Map[String, String]
  ): ValidatedNec[ValidationFailure, Map[EnvName, String]] =
    values.toVector
      .traverse { case (name, value) =>
        EnvName.from(name).toValidatedNec.map(_ -> value)
      }
      .map(_.toMap)

object LocalOpaque:
  def runtime(
      config: SlurmLocalConfig
  )(using Processes[IO]): Resource[IO, SlurmLocal[IO]] =
    SlurmLocal.default[IO](config)

  def submit(
      runtime: SlurmLocal[IO],
      request: JobRequest[NoResult]
  ): IO[SubmissionAttempt] =
    runtime.submitLowered(request)

object RemoteOpaque:
  def wire(
      connection: SshConnection,
      runner: SshProcessRunner[IO],
      frameLimits: FrameLimits,
      exchangePolicy: SshExchangePolicy
  ): Either[ValidationFailure, SshAgentWireClient[IO]] =
    SshCommand
      .agent(connection)
      .map(launch => SshAgentWireClient(launch, runner, frameLimits, exchangePolicy))

  def systemWire(
      connection: SshConnection,
      frameLimits: FrameLimits,
      exchangePolicy: SshExchangePolicy
  )(using Processes[IO]): Either[ValidationFailure, SshAgentWireClient[IO]] =
    wire(connection, SystemSshProcessRunner[IO], frameLimits, exchangePolicy)

  def connect(
      config: SlurmSshConfig
  )(using
      Processes[IO]
  ): Either[ValidationFailure, Resource[IO, RemoteSlurm[IO]]] =
    Slurm.overSsh[IO](config)

  def submit(
      api: AgentApi[IO],
      request: JobRequest[NoResult]
  ): IO[AgentCall[SubmissionAttempt]] =
    LaunchSpec
      .fromRequest(request)
      .fold(
        diagnostics =>
          IO.pure(
            AgentCall.Failed(
              io.github.bbuchsbaum.slurm4s.protocol.AgentFailure
                .ProtocolViolation(diagnostics.values.head.message, None)
            )
          ),
        api.submitOpaque
      )

object LogMonitoring:
  def readLocal(
      reader: LocalLogReader[IO],
      ref: LogRef,
      cursor: LogCursor,
      pageSize: ByteLimit
  ): IO[LogReadResult] =
    reader.read(ref, cursor, pageSize)

  def readRemote(
      api: AgentApi[IO],
      ref: LogRef,
      cursor: LogCursor,
      pageSize: ByteLimit
  ): IO[AgentCall[LogReadResult]] =
    api.readLog(ref, cursor, pageSize)

final case class RecoveryBounds(
    inFlight: PositiveInt,
    acceptanceUnknown: PositiveInt,
    pendingOutbox: PositiveInt
)

final case class RecoveryPass(
    recoveredClaims: Vector[Either[ControlFailure, ManagedAttempt]],
    acceptanceSearches: Vector[(SubmissionKey, AcceptanceSearchResult)],
    dispatchedOutbox: Vector[Either[ControlFailure, ManagedAttempt]]
)

object ManagedRecovery:
  def durableController(
      journal: Path,
      scheduler: Scheduler[IO],
      limits: JournalLimits = JournalLimits.default
  ): Resource[IO, ManagedController[IO]] =
    Managed.durable[IO](journal, scheduler, ManagedConfig(journalLimits = limits))

  def afterRestart(
      controller: ManagedController[IO],
      acceptanceSearch: AcceptanceSearch[IO],
      bounds: RecoveryBounds
  ): IO[RecoveryPass] =
    for
      recovered <- controller.recoverInFlight(bounds.inFlight.toInt)
      reconciled <- controller.reconcileUnknown(
        acceptanceSearch,
        bounds.acceptanceUnknown.toInt
      )
      dispatched <- controller.dispatchPending(bounds.pendingOutbox.toInt)
    yield RecoveryPass(recovered, reconciled, dispatched)

final class IncrementTask private (
    val operation: OperationRef[Int, Int]
) extends SlurmTask[Int, Int]:
  val inputCodec: InputCodec[Int] = new InputCodec[Int]:
    val schemaId: SchemaId = operation.inputSchema
    def encode(value: Int): Either[ResultCodecFailure, Vector[Byte]] =
      Right(value.toString.getBytes(StandardCharsets.UTF_8).toVector)
    def decode(bytes: Vector[Byte]): Either[ResultCodecFailure, Int] =
      IncrementTask.decodeInt(bytes)

  val outputCodec: ResultCodec[Int] = new ResultCodec[Int]:
    val schemaId: ResultSchemaId = operation.outputSchema
    def encode(value: Int): Either[ResultCodecFailure, Vector[Byte]] =
      Right(value.toString.getBytes(StandardCharsets.UTF_8).toVector)
    def decode(bytes: Vector[Byte]): Either[ResultCodecFailure, Int] =
      IncrementTask.decodeInt(bytes)

  override val retrySafety: RetrySafety = RetrySafety.SafeForAutomaticRetry

  def run(input: Int, context: TaskContext[IO]): IO[Int] =
    context.progress(ProgressEvent("incrementing")).as(input + 1)

object IncrementTask:
  def create(
      operationId: String = "example.increment",
      operationVersion: String = "1",
      inputSchema: String = "example.int-input.v1",
      outputSchema: String = "example.int-result.v1"
  ): ValidatedNec[ValidationFailure, IncrementTask] =
    (
      OperationId.from(operationId).toValidatedNec,
      OperationVersion.from(operationVersion).toValidatedNec,
      SchemaId.from(inputSchema).toValidatedNec,
      ResultSchemaId.from(outputSchema).toValidatedNec
    ).mapN { (id, version, input, output) =>
      IncrementTask(OperationRef(id, version, input, output))
    }

  private def decodeInt(bytes: Vector[Byte]): Either[ResultCodecFailure, Int] =
    Try(new String(bytes.toArray, StandardCharsets.UTF_8).toInt).toEither.leftMap { error =>
      ResultCodecFailure("invalid-int", Option(error.getMessage).getOrElse("invalid integer"))
    }

object TypedResults:
  def request(
      submissionKey: String,
      name: String,
      input: Int,
      task: IncrementTask,
      maximumResultBytes: ByteLimit,
      resources: ResourceRequest
  ): ValidatedNec[ValidationFailure, JobRequest[Int]] =
    (
      SubmissionKey.from(submissionKey).toValidatedNec,
      JobName.from(name).toValidatedNec
    ).mapN { (key, jobName) =>
      task(input).request(key, jobName, resources, maximumResultBytes)
    }

  def registry(task: IncrementTask): Either[TaskFailure, TaskRegistry] =
    TaskRegistry.from(Vector(TaskRegistration(task)))

  def submit(
      launcher: RegisteredTaskLauncher,
      scheduler: Scheduler[IO],
      request: JobRequest[Int]
  ): IO[RegisteredSubmissionResult[Int]] =
    RegisteredTaskSubmitter(launcher, scheduler).submit(request)

  def attach(
      attempt: ManagedAttempt,
      handle: DurableResultHandle,
      contract: ResultContract.Structured[Int],
      envelopePath: Path,
      observedOutputs: Vector[OutputEntry],
      observedAt: Instant
  ): IO[ExecutionResult[Int]] =
    ResultAttachment.attachFile[IO, Int](
      attempt,
      handle,
      contract,
      envelopePath,
      observedOutputs,
      observedAt
    )

object RemoteTypedResults:
  def submitAndAwait(
      remote: RemoteSlurm[IO],
      task: IncrementTask,
      input: Int,
      options: RemoteTaskOptions
  ): IO[Int] =
    remote.submitOrRaise(task(input), options).flatMap(_.awaitValue)

  def reattachAndAwait(
      remote: RemoteSlurm[IO],
      task: IncrementTask,
      descriptor: RemoteTaskDescriptor,
      policy: RemoteAwaitPolicy = RemoteAwaitPolicy.default
  ): IO[Int] =
    IO.fromEither(
      remote
        .attach(descriptor, task.outputCodec, policy)
        .leftMap(RemoteSubmitException.apply)
    ).flatMap(_.awaitValue)

object RemoteTypedBatches:
  def submitAndAwait[I, O](
      remote: RemoteSlurm[IO],
      grid: Grid[I],
      task: SlurmTask[I, O],
      execution: BatchExecutionPlan,
      options: RemoteBatchOptions
  ): IO[NonEmptyVector[O]] =
    remote
      .submitBatchOrRaise(
        SlurmBatch.registered(grid, task),
        execution,
        options
      )
      .flatMap(_.awaitValues)
