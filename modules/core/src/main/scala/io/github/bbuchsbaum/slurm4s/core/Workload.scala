package io.github.bbuchsbaum.slurm4s.core

import cats.Order
import cats.Show
import cats.data.NonEmptyVector

object RelativeOutputPath:
  opaque type Type = String

  def from(raw: String): Either[ValidationFailure, Type] =
    IdentifierRules.text("relativeOutputPath", raw, 4096).flatMap { path =>
      val segments = path.split('/').toVector
      Either.cond(
        !path.startsWith("/") && segments.forall(segment => segment.nonEmpty && segment != ".."),
        path,
        ValidationFailure(
          "relativeOutputPath",
          "must be relative and must not contain empty or '..' segments"
        )
      )
    }

  def unsafeFrom(raw: String): Type =
    from(raw).fold(problem => throw new IllegalArgumentException(problem.reason), identity)

  extension (path: Type) def value: String = path
  given CanEqual[Type, Type] = CanEqual.derived
  given Order[Type] = Order.from((left, right) => left.compareTo(right))
  given Ordering[Type] = summon[Order[Type]].toOrdering
  given Show[Type] = Show.show(identity)
type RelativeOutputPath = RelativeOutputPath.Type

enum ScriptSource derives CanEqual:
  case Inline(name: String, bytes: Vector[Byte])
  case StagedLocal(path: String)
  case ExistingRemote(path: String)

enum NoResult derives CanEqual:
  case Value

final case class OutputEntry private (
    path: RelativeOutputPath,
    sizeBytes: Long,
    digest: ContentDigest
) derives CanEqual

object OutputEntry:
  def from(
      path: RelativeOutputPath,
      sizeBytes: Long,
      digest: ContentDigest
  ): Either[ValidationFailure, OutputEntry] =
    Either.cond(
      sizeBytes >= 0L,
      OutputEntry(path, sizeBytes, digest),
      ValidationFailure("outputSizeBytes", "must not be negative")
    )

final case class OutputManifest private (entries: Vector[OutputEntry]) derives CanEqual

object OutputManifest:
  val empty: OutputManifest = OutputManifest(Vector.empty)

  private[slurm4s] def verified(entries: Vector[OutputEntry]): OutputManifest =
    OutputManifest(entries.sortBy(_.path))

  def from(entries: Vector[OutputEntry]): Either[ValidationFailure, OutputManifest] =
    val duplicates =
      entries.groupBy(_.path).collect { case (path, values) if values.size > 1 => path }
    Either.cond(
      duplicates.isEmpty,
      OutputManifest(entries.sortBy(_.path)),
      ValidationFailure(
        "outputManifest",
        s"contains duplicate paths: ${duplicates.toVector.map(_.value).sorted.mkString(", ")}"
      )
    )

enum ResultMode derives CanEqual:
  case ExitOnly
  case DeclaredOutputs
  case Structured

final case class ResultContractDescriptor(
    mode: ResultMode,
    schema: Option[ResultSchemaId],
    maxBytes: ByteLimit,
    declaredOutputs: Vector[RelativeOutputPath]
) derives CanEqual

sealed trait ResultContract[A] derives CanEqual:
  def descriptor: ResultContractDescriptor

object ResultContract:
  case object ExitOnly extends ResultContract[NoResult]:
    val descriptor: ResultContractDescriptor = ResultContractDescriptor(
      mode = ResultMode.ExitOnly,
      schema = None,
      maxBytes = ByteLimit.defaultEvidence,
      declaredOutputs = Vector.empty
    )

  final case class DeclaredOutputs private (
      outputs: NonEmptyVector[RelativeOutputPath],
      maxManifestBytes: ByteLimit
  ) extends ResultContract[OutputManifest]:
    val descriptor: ResultContractDescriptor = ResultContractDescriptor(
      mode = ResultMode.DeclaredOutputs,
      schema = None,
      maxBytes = maxManifestBytes,
      declaredOutputs = outputs.toVector
    )

  object DeclaredOutputs:
    def from(
        outputs: Vector[RelativeOutputPath],
        maxManifestBytes: ByteLimit
    ): Either[ValidationFailure, DeclaredOutputs] =
      NonEmptyVector.fromVector(outputs) match
        case None => Left(ValidationFailure("declaredOutputs", "must contain at least one path"))
        case Some(_) if outputs.distinct.size != outputs.size =>
          Left(ValidationFailure("declaredOutputs", "must not contain duplicate paths"))
        case Some(values) => Right(DeclaredOutputs(values, maxManifestBytes))

  final case class Structured[A] private (
      codec: ResultCodec[A],
      maxResultBytes: ByteLimit,
      outputs: Vector[RelativeOutputPath]
  ) extends ResultContract[A]:
    val descriptor: ResultContractDescriptor = ResultContractDescriptor(
      mode = ResultMode.Structured,
      schema = Some(codec.schemaId),
      maxBytes = maxResultBytes,
      declaredOutputs = outputs
    )

  object Structured:
    def apply[A](
        codec: ResultCodec[A],
        maxResultBytes: ByteLimit
    ): Structured[A] = new Structured(codec, maxResultBytes, Vector.empty)

    def from[A](
        codec: ResultCodec[A],
        maxResultBytes: ByteLimit,
        outputs: Vector[RelativeOutputPath]
    ): Either[ValidationFailure, Structured[A]] =
      Either.cond(
        outputs.distinct.size == outputs.size,
        new Structured(codec, maxResultBytes, outputs),
        ValidationFailure("structuredOutputs", "must not contain duplicate paths")
      )

sealed trait Payload[A]:
  def resultContract: ResultContract[A]

object Payload:
  final case class Script[A](
      source: ScriptSource,
      arguments: Vector[String],
      resultContract: ResultContract[A]
  ) extends Payload[A]

  final case class RegisteredTask[I, O](
      operation: OperationRef[I, O],
      input: I,
      inputCodec: InputCodec[I],
      resultContract: ResultContract[O]
  ) extends Payload[O]

enum WorkloadOutcome derives CanEqual:
  case Completed(exitCode: Int)
  case Failed(exitCode: Option[Int], diagnostics: Diagnostics)
  case OutOfMemory
  case TimeLimitExceeded
  case Cancelled
  case NodeFailure
  case Unknown(raw: String)

enum ExecutionResult[+A]:
  case Succeeded(value: A, outputs: OutputManifest, evidence: EvidenceBundle)
  case WorkloadFailed(outcome: WorkloadOutcome, evidence: EvidenceBundle)
  case ResultInvalid(diagnostics: Diagnostics, evidence: EvidenceBundle)
  case Indeterminate(diagnostics: Diagnostics, evidence: EvidenceBundle)
