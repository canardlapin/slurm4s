package io.github.bbuchsbaum.slurm4s.core

import cats.Order
import cats.Show
import io.github.bbuchsbaum.remoteexec.kernel.TextIdentifier

object JobId extends TextIdentifier("jobId", 200)
type JobId = JobId.Type

object ClusterName extends TextIdentifier("clusterName", 255)
type ClusterName = ClusterName.Type

object UserName extends TextIdentifier("userName", 255)
type UserName = UserName.Type

object InputName extends TextIdentifier("inputName", 255)
type InputName = InputName.Type

object EnvName:
  opaque type Type = String

  private val MaximumLength = 255
  private val Supported = "[A-Za-z_][A-Za-z0-9_]*".r
  private val ReservedExportTokens = Set("ALL", "NONE", "NIL")

  def from(raw: String): Either[ValidationFailure, Type] =
    if raw == null then Left(ValidationFailure("environmentName", "must not be null"))
    else if raw.length > MaximumLength then
      Left(
        ValidationFailure(
          "environmentName",
          s"must contain at most $MaximumLength characters"
        )
      )
    else if ReservedExportTokens.contains(raw.toUpperCase(java.util.Locale.ROOT)) then
      Left(
        ValidationFailure(
          "environmentName",
          "must not be a reserved Slurm export token"
        )
      )
    else if !Supported.matches(raw) then
      Left(
        ValidationFailure(
          "environmentName",
          "must use portable process-variable syntax [A-Za-z_][A-Za-z0-9_]*"
        )
      )
    else Right(raw)

  def unsafeFrom(raw: String): Type =
    from(raw).fold(problem => throw new IllegalArgumentException(problem.reason), identity)

  extension (name: Type) def value: String = name
  given CanEqual[Type, Type] = CanEqual.derived
  given Order[Type] = Order.from((left, right) => left.compareTo(right))
  given Ordering[Type] = summon[Order[Type]].toOrdering
  given Show[Type] = Show.show(identity)

type EnvName = EnvName.Type

object EventCursor:
  opaque type Type = Long
  val origin: Type = 0L
  def from(raw: Long): Either[ValidationFailure, Type] =
    Either.cond(raw >= 0L, raw, ValidationFailure("eventCursor", "must not be negative"))
  def unsafeFrom(raw: Long): Type =
    from(raw).fold(problem => throw new IllegalArgumentException(problem.reason), identity)
  extension (cursor: Type)
    def value: Long = cursor

    /** Refuses rather than wrapping: `+ 1L` past `Long.MaxValue` would produce a negative cursor,
      * violating this type's own `from` invariant and inverting its `Order`.
      */
    def next: Either[ValidationFailure, Type] =
      Either.cond(
        cursor < Long.MaxValue,
        cursor + 1L,
        ValidationFailure("eventCursor", "cannot advance beyond Long.MaxValue")
      )
  given CanEqual[Type, Type] = CanEqual.derived
  given Order[Type] = Order.from((left, right) => java.lang.Long.compare(left, right))
  given Ordering[Type] = summon[Order[Type]].toOrdering
  given Show[Type] = Show.show(_.toString)
type EventCursor = EventCursor.Type

object LogOffset:
  opaque type Type = Long
  val start: Type = 0L
  def from(raw: Long): Either[ValidationFailure, Type] =
    Either.cond(raw >= 0L, raw, ValidationFailure("logOffset", "must not be negative"))
  def unsafeFrom(raw: Long): Type =
    from(raw).fold(problem => throw new IllegalArgumentException(problem.reason), identity)
  extension (offset: Type) def value: Long = offset
  given CanEqual[Type, Type] = CanEqual.derived
  given Order[Type] = Order.from((left, right) => java.lang.Long.compare(left, right))
  given Ordering[Type] = summon[Order[Type]].toOrdering
  given Show[Type] = Show.show(_.toString)
type LogOffset = LogOffset.Type

object ArrayIndex:
  opaque type Type = Int
  def from(raw: Int): Either[ValidationFailure, Type] =
    Either.cond(raw >= 0, raw, ValidationFailure("arrayIndex", "must not be negative"))
  def unsafeFrom(raw: Int): Type =
    from(raw).fold(problem => throw new IllegalArgumentException(problem.reason), identity)
  extension (index: Type) def value: Int = index
  given CanEqual[Type, Type] = CanEqual.derived
  given Order[Type] = Order.from((left, right) => java.lang.Integer.compare(left, right))
  given Ordering[Type] = summon[Order[Type]].toOrdering
  given Show[Type] = Show.show(_.toString)
type ArrayIndex = ArrayIndex.Type

object Mebibytes:
  opaque type Type = Long
  def from(raw: Long): Either[ValidationFailure, Type] =
    Either.cond(raw > 0L, raw, ValidationFailure("memoryMebibytes", "must be positive"))
  extension (value: Type) def toLong: Long = value
  given CanEqual[Type, Type] = CanEqual.derived
  given Order[Type] = Order.from((left, right) => java.lang.Long.compare(left, right))
  given Ordering[Type] = summon[Order[Type]].toOrdering
  given Show[Type] = Show.show(_.toString)
type Mebibytes = Mebibytes.Type

object JobName extends TextIdentifier("jobName", 128)
type JobName = JobName.Type

object ProtocolVersion:
  opaque type Type = Long
  val v1: Type = pack(1, 0)

  def from(major: Int, minor: Int): Either[ValidationFailure, Type] =
    Either.cond(
      major >= 0 && minor >= 0,
      pack(major, minor),
      ValidationFailure("protocolVersion", "major and minor must not be negative")
    )

  extension (version: Type)
    def major: Int = (version >>> 32).toInt
    def minor: Int = version.toInt
  given CanEqual[Type, Type] = CanEqual.derived
  given Order[Type] = Order.from((left, right) => java.lang.Long.compare(left, right))
  given Ordering[Type] = summon[Order[Type]].toOrdering
  given Show[Type] = Show.show(version => s"${version.major}.${version.minor}")

  private def pack(major: Int, minor: Int): Type =
    (major.toLong << 32) | (minor.toLong & 0xffffffffL)
type ProtocolVersion = ProtocolVersion.Type
