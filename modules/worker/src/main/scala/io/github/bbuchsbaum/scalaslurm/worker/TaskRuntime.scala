package io.github.bbuchsbaum.scalaslurm.worker

import cats.effect.Clock
import cats.effect.IO
import cats.effect.Ref
import io.github.bbuchsbaum.scalaslurm.core.*

enum RetrySafety derives CanEqual:
  case Unknown
  case NoAutomaticRetry
  case SafeForAutomaticRetry

trait ScalaTask[I, O]:
  def operation: OperationRef[I, O]
  def inputCodec: InputCodec[I]
  def outputCodec: ResultCodec[O]
  def retrySafety: RetrySafety = RetrySafety.Unknown
  def run(input: I, context: TaskContext[IO]): IO[O]

object TaskInvocations:
  def encode[I, O](
      task: ScalaTask[I, O],
      input: I,
      submissionKey: SubmissionKey,
      attemptId: AttemptId,
      attemptEpoch: AttemptEpoch,
      job: Option[JobRef],
      declaredOutputs: Vector[RelativeOutputPath],
      maximumInputBytes: ByteLimit,
      maximumResultBytes: ByteLimit,
      maximumEnvelopeBytes: ByteLimit,
      maximumOutputBytes: ByteLimit,
      workerRelease: WorkerRelease
  ): Either[TaskFailure, TaskInvocation] =
    encodeWith(
      task.operation,
      task.inputCodec,
      task.outputCodec.schemaId,
      input,
      submissionKey,
      attemptId,
      attemptEpoch,
      job,
      declaredOutputs,
      maximumInputBytes,
      maximumResultBytes,
      maximumEnvelopeBytes,
      maximumOutputBytes,
      workerRelease
    )

  def encodeRegistered[I, O](
      task: Payload.RegisteredTask[I, O],
      submissionKey: SubmissionKey,
      attemptId: AttemptId,
      attemptEpoch: AttemptEpoch,
      job: Option[JobRef],
      maximumInputBytes: ByteLimit,
      maximumEnvelopeBytes: ByteLimit,
      maximumOutputBytes: ByteLimit,
      workerRelease: WorkerRelease
  ): Either[TaskFailure, TaskInvocation] =
    task.resultContract match
      case contract: ResultContract.Structured[O] @unchecked =>
        encodeWith(
          task.operation,
          task.inputCodec,
          contract.codec.schemaId,
          task.input,
          submissionKey,
          attemptId,
          attemptEpoch,
          job,
          contract.outputs,
          maximumInputBytes,
          contract.maxResultBytes,
          maximumEnvelopeBytes,
          maximumOutputBytes,
          workerRelease
        )
      case _ =>
        Left(
          TaskFailure.InvalidInvocation(
            "registered tasks require a structured result contract"
          )
        )

  private def encodeWith[I, O](
      operation: OperationRef[I, O],
      inputCodec: InputCodec[I],
      outputSchema: ResultSchemaId,
      input: I,
      submissionKey: SubmissionKey,
      attemptId: AttemptId,
      attemptEpoch: AttemptEpoch,
      job: Option[JobRef],
      declaredOutputs: Vector[RelativeOutputPath],
      maximumInputBytes: ByteLimit,
      maximumResultBytes: ByteLimit,
      maximumEnvelopeBytes: ByteLimit,
      maximumOutputBytes: ByteLimit,
      workerRelease: WorkerRelease
  ): Either[TaskFailure, TaskInvocation] =
    for
      _ <- Either.cond(
        operation.inputSchema == inputCodec.schemaId && operation.outputSchema == outputSchema,
        (),
        TaskFailure.InvalidInvocation("operation schemas do not match the registered codecs")
      )
      bytes <- inputCodec.encode(input).left.map(TaskFailure.InputCodec.apply)
      _ <- Either.cond(
        bytes.size <= maximumInputBytes.value,
        (),
        TaskFailure.InputTooLarge(bytes.size.toLong, maximumInputBytes.value)
      )
      _ <- Either.cond(
        declaredOutputs.distinct.size == declaredOutputs.size,
        (),
        TaskFailure.InvalidInvocation("declared outputs must be distinct")
      )
    yield TaskInvocation(
      submissionKey,
      attemptId,
      attemptEpoch,
      job,
      operation.descriptor,
      bytes,
      declaredOutputs,
      maximumInputBytes,
      maximumResultBytes,
      maximumEnvelopeBytes,
      maximumOutputBytes,
      workerRelease
    )

enum TaskFailure derives CanEqual:
  case UnknownOperation(operation: RegisteredOperation)
  case DuplicateOperation(operation: RegisteredOperation)
  case WorkerReleaseMismatch(expected: WorkerRelease, actual: WorkerRelease)
  case SchemaMismatch(expected: String, received: String)
  case InputTooLarge(actualBytes: Long, maximumBytes: Int)
  case ResultTooLarge(actualBytes: Long, maximumBytes: Int)
  case InputCodec(failure: ResultCodecFailure)
  case OutputCodec(failure: ResultCodecFailure)
  case TaskRaised(className: String, message: String)
  case Outputs(failure: TaskIoFailure)
  case Publication(failure: ResultPublicationFailure)
  case InvalidInvocation(message: String)

sealed trait TaskRegistration:
  def operation: RegisteredOperation
  private[worker] def execute(
      invocation: TaskInvocation,
      context: TaskContext[IO]
  ): IO[Either[TaskFailure, Vector[Byte]]]

object TaskRegistration:
  def apply[I, O](task: ScalaTask[I, O]): TaskRegistration = new TaskRegistration:
    val operation: RegisteredOperation = task.operation.descriptor

    def execute(
        invocation: TaskInvocation,
        context: TaskContext[IO]
    ): IO[Either[TaskFailure, Vector[Byte]]] =
      if invocation.operation.inputSchema != task.inputCodec.schemaId then
        IO.pure(
          Left(
            TaskFailure.SchemaMismatch(
              task.inputCodec.schemaId.value,
              invocation.operation.inputSchema.value
            )
          )
        )
      else if invocation.operation.outputSchema != task.outputCodec.schemaId then
        IO.pure(
          Left(
            TaskFailure.SchemaMismatch(
              task.outputCodec.schemaId.value,
              invocation.operation.outputSchema.value
            )
          )
        )
      else if invocation.inputBytes.size > invocation.maximumInputBytes.value then
        IO.pure(
          Left(
            TaskFailure.InputTooLarge(
              invocation.inputBytes.size.toLong,
              invocation.maximumInputBytes.value
            )
          )
        )
      else
        task.inputCodec.decode(invocation.inputBytes) match
          case Left(failure) => IO.pure(Left(TaskFailure.InputCodec(failure)))
          case Right(input)  =>
            task.run(input, context).attempt.map {
              case Left(error) =>
                Left(
                  TaskFailure.TaskRaised(
                    error.getClass.getName,
                    Option(error.getMessage).getOrElse(error.getClass.getSimpleName)
                  )
                )
              case Right(output) =>
                task.outputCodec.encode(output).left.map(TaskFailure.OutputCodec.apply).flatMap {
                  bytes =>
                    Either.cond(
                      bytes.size <= invocation.maximumResultBytes.value,
                      bytes,
                      TaskFailure.ResultTooLarge(
                        bytes.size.toLong,
                        invocation.maximumResultBytes.value
                      )
                    )
                }
            }

final class TaskRegistry private (entries: Map[(OperationId, OperationVersion), TaskRegistration]):
  private[worker] def execute(
      invocation: TaskInvocation,
      context: TaskContext[IO]
  ): IO[Either[TaskFailure, Vector[Byte]]] =
    entries.get(invocation.operation.id -> invocation.operation.version) match
      case None               => IO.pure(Left(TaskFailure.UnknownOperation(invocation.operation)))
      case Some(registration) => registration.execute(invocation, context)

object TaskRegistry:
  def from(registrations: Vector[TaskRegistration]): Either[TaskFailure, TaskRegistry] =
    val duplicate = registrations
      .groupBy(value => value.operation.id -> value.operation.version)
      .collectFirst { case (_, values) if values.size > 1 => values.head.operation }
    duplicate match
      case Some(operation) => Left(TaskFailure.DuplicateOperation(operation))
      case None            =>
        Right(
          TaskRegistry(
            registrations
              .map(value => (value.operation.id -> value.operation.version) -> value)
              .toMap
          )
        )

trait WorkerEventSink[F[_]]:
  def append(event: WorkerEvent): F[Unit]

object WorkerEventSink:
  val noop: WorkerEventSink[IO] = new WorkerEventSink[IO]:
    def append(event: WorkerEvent): IO[Unit] = IO.unit

enum WorkerRunResult derives CanEqual:
  case Succeeded(envelope: ResultEnvelope, publication: ResultPublication)
  case Failed(
      failure: TaskFailure,
      envelope: Option[ResultEnvelope],
      publication: Option[ResultPublication]
  )

final class WorkerRuntime private (
    release: WorkerRelease,
    registry: TaskRegistry,
    eventSink: WorkerEventSink[IO],
    sequence: Ref[IO, Long]
):
  def reportProgress(invocation: TaskInvocation)(event: ProgressEvent): IO[Unit] =
    emit(invocation, WorkerEventPayload.Progress(event))

  def run(
      invocation: TaskInvocation,
      context: TaskContext[IO],
      publisher: ResultPublisher[IO]
  ): IO[WorkerRunResult] =
    emit(invocation, WorkerEventPayload.Started) *>
      execute(invocation, context, publisher)

  private def execute(
      invocation: TaskInvocation,
      context: TaskContext[IO],
      publisher: ResultPublisher[IO]
  ): IO[WorkerRunResult] =
    val result =
      if invocation.workerRelease != release then
        IO.pure(Left(TaskFailure.WorkerReleaseMismatch(invocation.workerRelease, release)))
      else registry.execute(invocation, context)

    result.flatMap {
      case Left(failure) => publishFailure(invocation, failure, publisher)
      case Right(bytes)  =>
        context.outputs
          .seal(invocation.declaredOutputs, invocation.maximumOutputBytes)
          .flatMap {
            case Left(failure) =>
              publishFailure(invocation, TaskFailure.Outputs(failure), publisher)
            case Right(outputs) =>
              Clock[IO].realTimeInstant.flatMap { now =>
                val envelope = ResultEnvelope.succeeded(
                  invocation.submissionKey,
                  invocation.attemptId,
                  invocation.attemptEpoch,
                  invocation.job,
                  WorkloadOperation.Registered(
                    invocation.operation.id,
                    invocation.operation.version
                  ),
                  invocation.operation.outputSchema,
                  bytes,
                  outputs,
                  release,
                  now
                )
                publisher.publish(envelope).flatMap {
                  case Left(failure) =>
                    val taskFailure = TaskFailure.Publication(failure)
                    emit(invocation, WorkerEventPayload.Failed("publication", failure.toString)) *>
                      IO.pure(WorkerRunResult.Failed(taskFailure, Some(envelope), None))
                  case Right(publication) =>
                    emit(
                      invocation,
                      WorkerEventPayload.ResultPublished(publication.digest)
                    ) *> IO.pure(WorkerRunResult.Succeeded(envelope, publication))
                }
              }
          }
    }

  private def publishFailure(
      invocation: TaskInvocation,
      failure: TaskFailure,
      publisher: ResultPublisher[IO]
  ): IO[WorkerRunResult] =
    Clock[IO].realTimeInstant.flatMap { now =>
      val envelope = ResultEnvelope.failed(
        invocation.submissionKey,
        invocation.attemptId,
        invocation.attemptEpoch,
        invocation.job,
        WorkloadOperation.Registered(invocation.operation.id, invocation.operation.version),
        invocation.operation.outputSchema,
        failure.productPrefix,
        failure.toString,
        OutputManifest.empty,
        release,
        now
      )
      emit(invocation, WorkerEventPayload.Failed(failure.productPrefix, failure.toString)) *>
        publisher.publish(envelope).map {
          case Right(publication) =>
            WorkerRunResult.Failed(failure, Some(envelope), Some(publication))
          case Left(problem) =>
            WorkerRunResult.Failed(TaskFailure.Publication(problem), Some(envelope), None)
        }
    }

  private def emit(invocation: TaskInvocation, payload: WorkerEventPayload): IO[Unit] =
    for
      next <- sequence.getAndUpdate(_ + 1L)
      now <- Clock[IO].realTimeInstant
      _ <- eventSink.append(
        WorkerEvent(
          next,
          invocation.submissionKey,
          invocation.attemptId,
          invocation.attemptEpoch,
          WorkloadOperation.Registered(invocation.operation.id, invocation.operation.version),
          release,
          now,
          payload
        )
      )
    yield ()

object WorkerRuntime:
  def create(
      release: WorkerRelease,
      registry: TaskRegistry,
      eventSink: WorkerEventSink[IO] = WorkerEventSink.noop
  ): IO[WorkerRuntime] =
    Ref.of[IO, Long](0L).map(new WorkerRuntime(release, registry, eventSink, _))
