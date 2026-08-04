package io.github.bbuchsbaum.slurm4s.core

import cats.Order
import cats.Show
import cats.data.NonEmptyVector
import scodec.bits.ByteVector
import io.github.bbuchsbaum.remoteexec.kernel.TextIdentifier

object RelativeOutputPath:
  opaque type Type = String

  def from(raw: String): Either[ValidationFailure, Type] =
    TextIdentifier.validate("relativeOutputPath", raw, 4096).flatMap { path =>
      val segments = path.split('/').toVector
      // "." is rejected alongside ".." so that a/./b and a/b cannot be two distinct values naming
      // one file. Duplicate detection in OutputManifest and DeclaredOutputs compares these values
      // as strings, so two spellings of one path would pass the check and then collide on write.
      Either.cond(
        !path.startsWith("/") && segments
          .forall(segment => segment.nonEmpty && segment != ".." && segment != "."),
        path,
        ValidationFailure(
          "relativeOutputPath",
          "must be relative and must not contain empty, '.', or '..' segments"
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

/** How big an inline script may be is answered once, here, rather than at each boundary that
  * happens to look.
  *
  * It used to be answered three times and differently: the remote script-program decoder bounded
  * inline bytes at four mebibytes, the opaque submit decoder did not bound them at all and
  * inherited whatever frame size a deployment configured, and the worker refused them against its
  * own `maximumInvocationBytes`. So whether a given script was acceptable depended on which path
  * carried it, and a caller could build one that some paths would take and others would refuse.
  *
  * Bounding at construction is what makes the answer inherited rather than repeated: `Inline`
  * cannot be built without passing the check, so every decoder, planner and launcher downstream
  * holds a value that already satisfies it. A deployment may still be stricter — the worker's
  * configurable limit is a narrower operational bound, not a competing answer to what a valid
  * script is.
  */
enum ScriptSource private (valid: Boolean) derives CanEqual:
  if !valid then throw new IllegalArgumentException(ScriptSource.inlineScriptLimitReason)

  /** Source privacy keeps ordinary callers on the checked factory. The explicit parent construction
    * is also load-bearing: Scala emits public JVM constructor, `apply`, `copy`, and `fromProduct`
    * methods for a parameterized enum case even when its source constructor is private. Every one
    * of those generated paths evaluates `inlineWithinLimit`, so previously compiled bytecode and
    * reflection cannot manufacture an oversized value behind the source-level API.
    */
  case Inline private[ScriptSource] (name: String, bytes: ByteVector)
      extends ScriptSource(ScriptSource.inlineWithinLimit(bytes))
  case StagedLocal(path: String) extends ScriptSource(true)
  case ExistingRemote(path: String) extends ScriptSource(true)

object ScriptSource:
  private[ScriptSource] val inlineScriptLimitReason =
    s"must contain at most ${ByteLimit.maximumInlineScript.value} bytes"

  private[ScriptSource] def inlineWithinLimit(bytes: ByteVector): Boolean =
    bytes.size <= ByteLimit.maximumInlineScript.value.toLong

  def inlineScript(name: String, bytes: ByteVector): Either[ValidationFailure, Inline] =
    Either.cond(
      inlineWithinLimit(bytes),
      Inline(name, bytes),
      ValidationFailure("inlineScript", inlineScriptLimitReason)
    )

  /** Construct a trusted library or test constant, failing immediately if its source is invalid. */
  def unsafeInlineScript(name: String, bytes: ByteVector): Inline =
    inlineScript(name, bytes).fold(
      problem => throw new IllegalArgumentException(problem.reason),
      identity
    )

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

/** The mode-dependent shape of a result contract.
  *
  * Constructed only through [[ResultContractDescriptor.from]] or by the `ResultContract` cases in
  * this file. A public `apply`/`copy` allowed a descriptor claiming `Structured` with no schema, or
  * `ExitOnly` with declared outputs — states no `ResultContract` can produce and no consumer knows
  * how to honour.
  */
final case class ResultContractDescriptor private[core] (
    mode: ResultMode,
    schema: Option[ResultSchemaId],
    declaredOutputs: Vector[RelativeOutputPath]
) derives CanEqual

object ResultContractDescriptor:
  def from(
      mode: ResultMode,
      schema: Option[ResultSchemaId],
      declaredOutputs: Vector[RelativeOutputPath]
  ): Either[ValidationFailure, ResultContractDescriptor] =
    if declaredOutputs.distinct.size != declaredOutputs.size then
      Left(ValidationFailure("declaredOutputs", "must not contain duplicate paths"))
    else
      mode match
        case ResultMode.ExitOnly if schema.isDefined =>
          Left(ValidationFailure("resultContract", "exit-only results carry no schema"))
        case ResultMode.ExitOnly if declaredOutputs.nonEmpty =>
          Left(ValidationFailure("resultContract", "exit-only results declare no outputs"))
        case ResultMode.DeclaredOutputs if schema.isDefined =>
          Left(ValidationFailure("resultContract", "declared-output results carry no schema"))
        case ResultMode.DeclaredOutputs if declaredOutputs.isEmpty =>
          Left(ValidationFailure("declaredOutputs", "must contain at least one path"))
        case ResultMode.Structured if schema.isEmpty =>
          Left(ValidationFailure("resultContract", "structured results require a schema"))
        case _ =>
          Right(ResultContractDescriptor(mode, schema, declaredOutputs))

sealed trait ResultContract[A] derives CanEqual:
  def descriptor: ResultContractDescriptor

object ResultContract:
  case object ExitOnly extends ResultContract[NoResult]:
    val descriptor: ResultContractDescriptor = ResultContractDescriptor(
      mode = ResultMode.ExitOnly,
      schema = None,
      declaredOutputs = Vector.empty
    )

  final case class DeclaredOutputs private (
      outputs: NonEmptyVector[RelativeOutputPath]
  ) extends ResultContract[OutputManifest]:
    val descriptor: ResultContractDescriptor = ResultContractDescriptor(
      mode = ResultMode.DeclaredOutputs,
      schema = None,
      declaredOutputs = outputs.toVector
    )

  object DeclaredOutputs:
    def from(
        outputs: Vector[RelativeOutputPath]
    ): Either[ValidationFailure, DeclaredOutputs] =
      NonEmptyVector.fromVector(outputs) match
        case None => Left(ValidationFailure("declaredOutputs", "must contain at least one path"))
        case Some(_) if outputs.distinct.size != outputs.size =>
          Left(ValidationFailure("declaredOutputs", "must not contain duplicate paths"))
        case Some(values) => Right(DeclaredOutputs(values))

  final case class Structured[A] private (
      codec: ResultCodec[A],
      maxResultBytes: ByteLimit,
      outputs: Vector[RelativeOutputPath]
  ) extends ResultContract[A]:
    val descriptor: ResultContractDescriptor = ResultContractDescriptor(
      mode = ResultMode.Structured,
      schema = Some(codec.schemaId),
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

enum CompletionExitStatus derives CanEqual:
  case ReportedZero
  case Undisclosed

enum WorkloadOutcome derives CanEqual:
  /** Slurm reported successful completion, with an explicit zero or no disclosed process status. */
  case Completed(exitStatus: CompletionExitStatus)
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
