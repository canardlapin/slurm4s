package io.github.bbuchsbaum.slurm4s.worker

import cats.effect.Clock
import cats.effect.IO
import cats.effect.Ref
import cats.effect.kernel.Outcome
import cats.syntax.all.*
import io.github.bbuchsbaum.slurm4s.core.*

import scala.concurrent.duration.*

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
      workerRelease,
      task.retrySafety
    )

  def encodeRegistered[I, O](
      task: Payload.RegisteredTask[I, O],
      retrySafety: RetrySafety,
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
          workerRelease,
          retrySafety
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
      workerRelease: WorkerRelease,
      retrySafety: RetrySafety
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
      workerRelease,
      retrySafety
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

  def stableCode: String = this match
    case UnknownOperation(_)         => "unknown-operation"
    case DuplicateOperation(_)       => "duplicate-operation"
    case WorkerReleaseMismatch(_, _) => "worker-release-mismatch"
    case SchemaMismatch(_, _)        => "schema-mismatch"
    case InputTooLarge(_, _)         => "input-too-large"
    case ResultTooLarge(_, _)        => "result-too-large"
    case InputCodec(_)               => "input-codec"
    case OutputCodec(_)              => "output-codec"
    case TaskRaised(_, _)            => "task-raised"
    case Outputs(_)                  => "outputs"
    case Publication(_)              => "publication"
    case InvalidInvocation(_)        => "invalid-invocation"

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

final case class WorkerEventPolicy(deliveryTimeout: DurationMillis) derives CanEqual

object WorkerEventPolicy:
  val default: WorkerEventPolicy = WorkerEventPolicy(
    DurationMillis.unsafeFrom(1000L)
  )

final case class WorkerEventDeliveryDiagnostic(
    eventKind: String,
    code: String,
    causeClass: Option[String]
) derives CanEqual

enum WorkerRunResult derives CanEqual:
  case Succeeded(
      envelope: ResultEnvelope,
      publication: ResultPublication,
      eventDeliveryDiagnostics: Vector[WorkerEventDeliveryDiagnostic]
  )
  case Failed(
      failure: TaskFailure,
      envelope: Option[ResultEnvelope],
      publication: Option[ResultPublication],
      eventDeliveryDiagnostics: Vector[WorkerEventDeliveryDiagnostic]
  )

final class WorkerRuntime private (
    release: WorkerRelease,
    registry: TaskRegistry,
    eventSink: WorkerEventSink[IO],
    eventPolicy: WorkerEventPolicy,
    sequence: Ref[IO, Long],
    deliveryDiagnostics: Ref[
      IO,
      Map[(AttemptId, AttemptEpoch), Vector[WorkerEventDeliveryDiagnostic]]
    ]
):
  def reportProgress(invocation: TaskInvocation)(event: ProgressEvent): IO[Unit] =
    emit(invocation, WorkerEventPayload.Progress(event))

  def run(
      invocation: TaskInvocation,
      context: TaskContext[IO],
      publisher: ResultPublisher[IO]
  ): IO[WorkerRunResult] =
    clearDeliveryDiagnostics(invocation) *>
      emit(invocation, WorkerEventPayload.Started) *>
      execute(invocation, context, publisher).flatMap(attachDeliveryDiagnostics(invocation, _))

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
                      IO.pure(
                        WorkerRunResult.Failed(taskFailure, Some(envelope), None, Vector.empty)
                      )
                  case Right(publication) =>
                    emit(
                      invocation,
                      WorkerEventPayload.ResultPublished(publication.digest)
                    ) *> IO.pure(WorkerRunResult.Succeeded(envelope, publication, Vector.empty))
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
        failure.stableCode,
        failure.toString,
        OutputManifest.empty,
        release,
        now
      )
      publisher.publish(envelope).flatMap {
        case Right(publication) =>
          emit(invocation, WorkerEventPayload.Failed(failure.stableCode, failure.toString)) *>
            IO.pure(
              WorkerRunResult.Failed(
                failure,
                Some(envelope),
                Some(publication),
                Vector.empty
              )
            )
        case Left(problem) =>
          emit(invocation, WorkerEventPayload.Failed("publication", problem.toString)) *>
            IO.pure(
              WorkerRunResult.Failed(
                TaskFailure.Publication(problem),
                Some(envelope),
                None,
                Vector.empty
              )
            )
      }
    }

  private def emit(invocation: TaskInvocation, payload: WorkerEventPayload): IO[Unit] =
    for
      next <- sequence.getAndUpdate(_ + 1L)
      now <- Clock[IO].realTimeInstant
      event = WorkerEvent(
        next,
        invocation.submissionKey,
        invocation.attemptId,
        invocation.attemptEpoch,
        WorkloadOperation.Registered(invocation.operation.id, invocation.operation.version),
        release,
        now,
        payload
      )
      diagnostic <- deliverEvent(event, eventKind(payload))
      _ <- diagnostic.traverse_(recordDeliveryDiagnostic(invocation, _))
    yield ()

  private def deliverEvent(
      event: WorkerEvent,
      kind: String
  ): IO[Option[WorkerEventDeliveryDiagnostic]] =
    eventSink.append(event).start.flatMap { fiber =>
      fiber.join
        .map(Right(_))
        .timeoutTo(
          eventPolicy.deliveryTimeout.value.millis,
          fiber.cancel.start.void.as(
            Left(
              WorkerEventDeliveryDiagnostic(
                kind,
                "event-delivery-timeout",
                None
              )
            )
          )
        )
        .flatMap {
          case Left(diagnostic)                    => IO.pure(Some(diagnostic))
          case Right(Outcome.Succeeded(completed)) =>
            completed.attempt.map {
              case Right(_)    => None
              case Left(error) => Some(raisedDiagnostic(kind, error))
            }
          case Right(Outcome.Errored(error)) =>
            IO.pure(Some(raisedDiagnostic(kind, error)))
          case Right(Outcome.Canceled()) =>
            IO.pure(
              Some(
                WorkerEventDeliveryDiagnostic(
                  kind,
                  "event-delivery-cancelled",
                  None
                )
              )
            )
        }
    }

  private def raisedDiagnostic(
      kind: String,
      error: Throwable
  ): WorkerEventDeliveryDiagnostic =
    WorkerEventDeliveryDiagnostic(
      kind,
      "event-delivery-raised",
      Some(
        Option(error.getClass.getSimpleName)
          .filter(_.nonEmpty)
          .getOrElse("Throwable")
          .take(128)
      )
    )

  private def eventKind(payload: WorkerEventPayload): String = payload match
    case WorkerEventPayload.Started            => "started"
    case WorkerEventPayload.Progress(_)        => "progress"
    case WorkerEventPayload.ProcessExited(_)   => "process-exited"
    case WorkerEventPayload.ResultPublished(_) => "result-published"
    case WorkerEventPayload.Failed(_, _)       => "failed"

  private def recordDeliveryDiagnostic(
      invocation: TaskInvocation,
      diagnostic: WorkerEventDeliveryDiagnostic
  ): IO[Unit] =
    val key = invocation.attemptId -> invocation.attemptEpoch
    deliveryDiagnostics.update { current =>
      val bounded = (current.getOrElse(key, Vector.empty) :+ diagnostic)
        .takeRight(WorkerRuntime.MaximumDeliveryDiagnostics)
      current.updated(key, bounded)
    }

  private def clearDeliveryDiagnostics(invocation: TaskInvocation): IO[Unit] =
    deliveryDiagnostics.update(_ - (invocation.attemptId -> invocation.attemptEpoch))

  private def attachDeliveryDiagnostics(
      invocation: TaskInvocation,
      result: WorkerRunResult
  ): IO[WorkerRunResult] =
    val key = invocation.attemptId -> invocation.attemptEpoch
    deliveryDiagnostics.modify { current =>
      val diagnostics = current.getOrElse(key, Vector.empty)
      val updated = result match
        case WorkerRunResult.Succeeded(envelope, publication, _) =>
          WorkerRunResult.Succeeded(envelope, publication, diagnostics)
        case WorkerRunResult.Failed(failure, envelope, publication, _) =>
          WorkerRunResult.Failed(failure, envelope, publication, diagnostics)
      current.removed(key) -> updated
    }

object WorkerRuntime:
  private val MaximumDeliveryDiagnostics = 16

  def create(
      release: WorkerRelease,
      registry: TaskRegistry,
      eventSink: WorkerEventSink[IO] = WorkerEventSink.noop,
      eventPolicy: WorkerEventPolicy = WorkerEventPolicy.default
  ): IO[WorkerRuntime] =
    (
      Ref.of[IO, Long](0L),
      Ref.of[
        IO,
        Map[(AttemptId, AttemptEpoch), Vector[WorkerEventDeliveryDiagnostic]]
      ](Map.empty)
    ).mapN(new WorkerRuntime(release, registry, eventSink, eventPolicy, _, _))
