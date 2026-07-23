package io.github.bbuchsbaum.scalaslurm.core

final case class ValidationFailure(field: String, reason: String) derives CanEqual

private[core] object IdentifierRules:
  def text(field: String, raw: String, maxLength: Int): Either[ValidationFailure, String] =
    if raw == null then Left(ValidationFailure(field, "must not be null"))
    else if raw.isEmpty then Left(ValidationFailure(field, "must not be empty"))
    else if raw.length > maxLength then
      Left(ValidationFailure(field, s"must contain at most $maxLength characters"))
    else if raw.exists(character => character.isControl || character.isWhitespace) then
      Left(ValidationFailure(field, "must not contain whitespace or control characters"))
    else Right(raw)

object SubmissionKey:
  opaque type Type = String
  def from(raw: String): Either[ValidationFailure, Type] =
    IdentifierRules.text("submissionKey", raw, 200)
  extension (id: Type) def value: String = id
type SubmissionKey = SubmissionKey.Type

object AttemptId:
  opaque type Type = String
  def from(raw: String): Either[ValidationFailure, Type] =
    IdentifierRules.text("attemptId", raw, 200)
  extension (id: Type) def value: String = id
type AttemptId = AttemptId.Type

object JobId:
  opaque type Type = String
  def from(raw: String): Either[ValidationFailure, Type] =
    IdentifierRules.text("jobId", raw, 200)
  extension (id: Type) def value: String = id
type JobId = JobId.Type

object ClusterName:
  opaque type Type = String
  def from(raw: String): Either[ValidationFailure, Type] =
    IdentifierRules.text("clusterName", raw, 255)
  extension (id: Type) def value: String = id
type ClusterName = ClusterName.Type

object OperationId:
  opaque type Type = String
  def from(raw: String): Either[ValidationFailure, Type] =
    IdentifierRules.text("operationId", raw, 255)
  extension (id: Type) def value: String = id
type OperationId = OperationId.Type

object OperationVersion:
  opaque type Type = String
  def from(raw: String): Either[ValidationFailure, Type] =
    IdentifierRules.text("operationVersion", raw, 100)
  extension (id: Type) def value: String = id
type OperationVersion = OperationVersion.Type

object SchemaId:
  opaque type Type = String
  def from(raw: String): Either[ValidationFailure, Type] =
    IdentifierRules.text("schemaId", raw, 255)
  extension (id: Type) def value: String = id
type SchemaId = SchemaId.Type

object ResultSchemaId:
  opaque type Type = String
  def from(raw: String): Either[ValidationFailure, Type] =
    IdentifierRules.text("resultSchemaId", raw, 255)
  extension (id: Type) def value: String = id
type ResultSchemaId = ResultSchemaId.Type

object InputName:
  opaque type Type = String
  def from(raw: String): Either[ValidationFailure, Type] =
    IdentifierRules.text("inputName", raw, 255)
  extension (id: Type) def value: String = id
type InputName = InputName.Type

object WorkerReleaseId:
  opaque type Type = String
  def from(raw: String): Either[ValidationFailure, Type] =
    IdentifierRules.text("workerReleaseId", raw, 255)
  extension (id: Type) def value: String = id
type WorkerReleaseId = WorkerReleaseId.Type

object ContentDigest:
  opaque type Type = String
  def from(raw: String): Either[ValidationFailure, Type] =
    IdentifierRules.text("contentDigest", raw, 200)
  extension (id: Type) def value: String = id
type ContentDigest = ContentDigest.Type

object AttemptEpoch:
  opaque type Type = Long
  val initial: Type = 1L
  def from(raw: Long): Either[ValidationFailure, Type] =
    Either.cond(raw > 0L, raw, ValidationFailure("attemptEpoch", "must be positive"))
  extension (epoch: Type) def value: Long = epoch
type AttemptEpoch = AttemptEpoch.Type

object EventCursor:
  opaque type Type = Long
  val origin: Type = 0L
  def from(raw: Long): Either[ValidationFailure, Type] =
    Either.cond(raw >= 0L, raw, ValidationFailure("eventCursor", "must not be negative"))
  extension (cursor: Type)
    def value: Long = cursor
    def next: Type = cursor + 1L
type EventCursor = EventCursor.Type

object LogOffset:
  opaque type Type = Long
  val start: Type = 0L
  def from(raw: Long): Either[ValidationFailure, Type] =
    Either.cond(raw >= 0L, raw, ValidationFailure("logOffset", "must not be negative"))
  extension (offset: Type) def value: Long = offset
type LogOffset = LogOffset.Type

object ArrayIndex:
  opaque type Type = Int
  def from(raw: Int): Either[ValidationFailure, Type] =
    Either.cond(raw >= 0, raw, ValidationFailure("arrayIndex", "must not be negative"))
  extension (index: Type) def value: Int = index
type ArrayIndex = ArrayIndex.Type

object ByteLimit:
  opaque type Type = Int
  val defaultEvidence: Type = 64 * 1024
  val maximumCommandCapture: Type = 4 * 1024 * 1024
  val maximumLogPage: Type = 4 * 1024 * 1024
  def from(raw: Int): Either[ValidationFailure, Type] =
    Either.cond(raw > 0, raw, ValidationFailure("byteLimit", "must be positive"))
  extension (limit: Type) def value: Int = limit
type ByteLimit = ByteLimit.Type

object DurationMillis:
  opaque type Type = Long
  def from(raw: Long): Either[ValidationFailure, Type] =
    Either.cond(raw >= 0L, raw, ValidationFailure("durationMillis", "must not be negative"))
  extension (duration: Type) def value: Long = duration
type DurationMillis = DurationMillis.Type

object PositiveInt:
  opaque type Type = Int
  def from(field: String, raw: Int): Either[ValidationFailure, Type] =
    Either.cond(raw > 0, raw, ValidationFailure(field, "must be positive"))
  extension (value: Type) def toInt: Int = value
type PositiveInt = PositiveInt.Type

object Mebibytes:
  opaque type Type = Long
  def from(raw: Long): Either[ValidationFailure, Type] =
    Either.cond(raw > 0L, raw, ValidationFailure("memoryMebibytes", "must be positive"))
  extension (value: Type) def toLong: Long = value
type Mebibytes = Mebibytes.Type

object WallTimeMinutes:
  opaque type Type = Long
  def from(raw: Long): Either[ValidationFailure, Type] =
    Either.cond(raw > 0L, raw, ValidationFailure("wallTimeMinutes", "must be positive"))
  extension (value: Type) def toLong: Long = value
type WallTimeMinutes = WallTimeMinutes.Type

object JobName:
  opaque type Type = String
  def from(raw: String): Either[ValidationFailure, Type] =
    IdentifierRules.text("jobName", raw, 128)
  extension (name: Type) def value: String = name
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

  private def pack(major: Int, minor: Int): Type =
    (major.toLong << 32) | (minor.toLong & 0xffffffffL)
type ProtocolVersion = ProtocolVersion.Type
