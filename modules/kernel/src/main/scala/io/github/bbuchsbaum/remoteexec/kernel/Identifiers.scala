package io.github.bbuchsbaum.remoteexec.kernel

import cats.Order
import cats.Show

/** A rejected provider-neutral value.
  *
  * `field` is a stable machine-facing field name; `reason` is a bounded human-facing explanation.
  */
final case class ValidationFailure(field: String, reason: String) derives CanEqual

private[kernel] object IdentifierRules:
  def text(field: String, raw: String, maxLength: Int): Either[ValidationFailure, String] =
    if raw == null then Left(ValidationFailure(field, "must not be null"))
    else if raw.isEmpty then Left(ValidationFailure(field, "must not be empty"))
    else if raw.length > maxLength then
      Left(ValidationFailure(field, s"must contain at most $maxLength characters"))
    else if raw.exists(character => character.isControl || character.isWhitespace) then
      Left(ValidationFailure(field, "must not contain whitespace or control characters"))
    else Right(raw)

/** Shared semantics for a validated, ordered textual identifier.
  *
  * Each extending singleton receives its own path-dependent opaque `Type`, so equal-looking values
  * from different identifier kinds remain non-interchangeable.
  */
abstract class TextIdentifier(field: String, maximumLength: Int):
  opaque type Type = String

  def from(raw: String): Either[ValidationFailure, Type] =
    IdentifierRules.text(field, raw, maximumLength)

  /** Construct a trusted library or test constant, failing immediately if its source is invalid. */
  def unsafeFrom(raw: String): Type =
    from(raw).fold(problem => throw new IllegalArgumentException(problem.reason), identity)

  /** Accept a literal the macro has already validated. Subclasses call this from `apply`. */
  protected inline def validated(inline value: String): Type = unsafeFrom(value)

  extension (id: Type) def value: String = id

  given CanEqual[Type, Type] = CanEqual.derived
  given Order[Type] = Order.from((left, right) => left.compareTo(right))
  given Ordering[Type] = summon[Order[Type]].toOrdering
  given Show[Type] = Show.show(identity)

object TextIdentifier:
  def validate(
      field: String,
      raw: String,
      maximumLength: Int
  ): Either[ValidationFailure, String] =
    IdentifierRules.text(field, raw, maximumLength)

object SubmissionKey extends TextIdentifier("submissionKey", 200):
  /** A literal, validated at compile time. Dynamic values use `from`. */
  inline def apply(inline raw: String): Type =
    validated(LiteralIdentifier.text("submissionKey", 200, raw))
type SubmissionKey = SubmissionKey.Type

object AttemptId extends TextIdentifier("attemptId", 200):
  /** A literal, validated at compile time. Dynamic values use `from`. */
  inline def apply(inline raw: String): Type =
    validated(LiteralIdentifier.text("attemptId", 200, raw))
type AttemptId = AttemptId.Type

object OperationId extends TextIdentifier("operationId", 255):
  /** A literal, validated at compile time. Dynamic values use `from`. */
  inline def apply(inline raw: String): Type =
    validated(LiteralIdentifier.text("operationId", 255, raw))
type OperationId = OperationId.Type

object OperationVersion extends TextIdentifier("operationVersion", 100):
  /** A literal, validated at compile time. Dynamic values use `from`. */
  inline def apply(inline raw: String): Type =
    validated(LiteralIdentifier.text("operationVersion", 100, raw))
type OperationVersion = OperationVersion.Type

object SchemaId extends TextIdentifier("schemaId", 255):
  /** A literal, validated at compile time. Dynamic values use `from`. */
  inline def apply(inline raw: String): Type =
    validated(LiteralIdentifier.text("schemaId", 255, raw))
type SchemaId = SchemaId.Type

object ResultSchemaId extends TextIdentifier("resultSchemaId", 255):
  /** A literal, validated at compile time. Dynamic values use `from`. */
  inline def apply(inline raw: String): Type =
    validated(LiteralIdentifier.text("resultSchemaId", 255, raw))
type ResultSchemaId = ResultSchemaId.Type

object WorkerReleaseId extends TextIdentifier("workerReleaseId", 255):
  /** A literal, validated at compile time. Dynamic values use `from`. */
  inline def apply(inline raw: String): Type =
    validated(LiteralIdentifier.text("workerReleaseId", 255, raw))
type WorkerReleaseId = WorkerReleaseId.Type

/** A content digest, structurally validated as `sha256:<64 lowercase hex>`.
  *
  * A generic bounded-text identifier accepted `"x"` as a digest, so nothing prevented an arbitrary
  * label from standing in for content identity — including on decode, where every value arriving
  * from the wire was taken at face value.
  */
object ContentDigest:
  opaque type Type = String

  private val Sha256 = "sha256:[0-9a-f]{64}".r

  def from(raw: String): Either[ValidationFailure, Type] =
    if raw == null then Left(ValidationFailure("contentDigest", "must not be null"))
    else if !Sha256.matches(raw) then
      Left(
        ValidationFailure("contentDigest", "must be sha256:<64 lowercase hex characters>")
      )
    else Right(raw)

  def unsafeFrom(raw: String): Type =
    from(raw).fold(problem => throw new IllegalArgumentException(problem.reason), identity)

  extension (id: Type) def value: String = id

  given CanEqual[Type, Type] = CanEqual.derived
  given Order[Type] = Order.from((left, right) => left.compareTo(right))
  given Ordering[Type] = summon[Order[Type]].toOrdering
  given Show[Type] = Show.show(identity)

type ContentDigest = ContentDigest.Type

object AttemptEpoch:
  opaque type Type = Long
  val initial: Type = 1L
  def from(raw: Long): Either[ValidationFailure, Type] =
    Either.cond(raw > 0L, raw, ValidationFailure("attemptEpoch", "must be positive"))
  def unsafeFrom(raw: Long): Type =
    from(raw).fold(problem => throw new IllegalArgumentException(problem.reason), identity)
  extension (epoch: Type)
    def value: Long = epoch
    def next: Either[ValidationFailure, Type] =
      Either.cond(
        epoch < Long.MaxValue,
        epoch + 1L,
        ValidationFailure("attemptEpoch", "cannot advance beyond Long.MaxValue")
      )
  given CanEqual[Type, Type] = CanEqual.derived
  given Order[Type] = Order.from((left, right) => java.lang.Long.compare(left, right))
  given Ordering[Type] = summon[Order[Type]].toOrdering
  given Show[Type] = Show.show(_.toString)
type AttemptEpoch = AttemptEpoch.Type

object ByteLimit:
  opaque type Type = Int
  val defaultEvidence: Type = 64 * 1024
  val maximumCommandCapture: Type = 4 * 1024 * 1024
  val maximumLogPage: Type = 4 * 1024 * 1024
  def from(raw: Int): Either[ValidationFailure, Type] =
    Either.cond(raw > 0, raw, ValidationFailure("byteLimit", "must be positive"))
  def unsafeFrom(raw: Int): Type =
    from(raw).fold(problem => throw new IllegalArgumentException(problem.reason), identity)
  extension (limit: Type) def value: Int = limit
  given CanEqual[Type, Type] = CanEqual.derived
  given Order[Type] = Order.from((left, right) => java.lang.Integer.compare(left, right))
  given Ordering[Type] = summon[Order[Type]].toOrdering
  given Show[Type] = Show.show(_.toString)
type ByteLimit = ByteLimit.Type

object DurationMillis:
  opaque type Type = Long
  def from(raw: Long): Either[ValidationFailure, Type] =
    Either.cond(raw >= 0L, raw, ValidationFailure("durationMillis", "must not be negative"))
  def unsafeFrom(raw: Long): Type =
    from(raw).fold(problem => throw new IllegalArgumentException(problem.reason), identity)
  extension (duration: Type) def value: Long = duration
  given CanEqual[Type, Type] = CanEqual.derived
  given Order[Type] = Order.from((left, right) => java.lang.Long.compare(left, right))
  given Ordering[Type] = summon[Order[Type]].toOrdering
  given Show[Type] = Show.show(_.toString)
type DurationMillis = DurationMillis.Type

object PositiveInt:
  opaque type Type = Int
  def from(field: String, raw: Int): Either[ValidationFailure, Type] =
    Either.cond(raw > 0, raw, ValidationFailure(field, "must be positive"))
  def unsafeFrom(raw: Int): Type =
    from("positiveInt", raw).fold(
      problem => throw new IllegalArgumentException(problem.reason),
      identity
    )
  extension (value: Type) def toInt: Int = value
  given CanEqual[Type, Type] = CanEqual.derived
  given Order[Type] = Order.from((left, right) => java.lang.Integer.compare(left, right))
  given Ordering[Type] = summon[Order[Type]].toOrdering
  given Show[Type] = Show.show(_.toString)
type PositiveInt = PositiveInt.Type

object WallTimeMinutes:
  opaque type Type = Long
  def from(raw: Long): Either[ValidationFailure, Type] =
    Either.cond(raw > 0L, raw, ValidationFailure("wallTimeMinutes", "must be positive"))
  def unsafeFrom(raw: Long): Type =
    from(raw).fold(problem => throw new IllegalArgumentException(problem.reason), identity)
  extension (value: Type) def toLong: Long = value
  given CanEqual[Type, Type] = CanEqual.derived
  given Order[Type] = Order.from((left, right) => java.lang.Long.compare(left, right))
  given Ordering[Type] = summon[Order[Type]].toOrdering
  given Show[Type] = Show.show(_.toString)
type WallTimeMinutes = WallTimeMinutes.Type
