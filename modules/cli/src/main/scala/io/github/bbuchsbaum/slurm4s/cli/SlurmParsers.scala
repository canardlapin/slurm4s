package io.github.bbuchsbaum.slurm4s.cli

import cats.data.NonEmptyVector
import cats.syntax.all.*
import io.circe.Json
import io.circe.JsonObject
import io.circe.parser
import io.github.bbuchsbaum.slurm4s.core.*

import java.nio.ByteBuffer
import java.nio.charset.CharacterCodingException
import java.nio.charset.CodingErrorAction
import java.nio.charset.StandardCharsets
import java.time.Instant
import java.time.LocalDateTime
import java.time.OffsetDateTime
import java.util.Locale
import scala.util.Try

object EvidenceText:
  def decode(bytes: Vector[Byte]): Either[Diagnostics, String] =
    val decoder = StandardCharsets.UTF_8
      .newDecoder()
      .onMalformedInput(CodingErrorAction.REPORT)
      .onUnmappableCharacter(CodingErrorAction.REPORT)
    try Right(decoder.decode(ByteBuffer.wrap(bytes.toArray)).toString)
    catch
      case error: CharacterCodingException =>
        Left(
          Diagnostics.one(
            Diagnostic(
              "invalid-utf8",
              "command output is not valid UTF-8",
              Map("detail" -> error.getClass.getSimpleName)
            )
          )
        )

object SbatchParsable:
  def parse(stdout: BoundedEvidence): Either[Diagnostics, JobRef] =
    EvidenceText.decode(stdout.bytes).flatMap { raw =>
      val line = raw.stripSuffix("\n").stripSuffix("\r")
      line.split(";", -1).toVector match
        case Vector(jobText)                                      => job(jobText, None)
        case Vector(jobText, clusterText) if clusterText.nonEmpty =>
          ClusterName.from(clusterText).left.map(validation("invalid-cluster", _)).flatMap {
            cluster =>
              job(jobText, Some(cluster))
          }
        case _ =>
          Left(
            Diagnostics
              .one(Diagnostic("invalid-sbatch-response", "expected job-id or job-id;cluster"))
          )
    }

  private def job(raw: String, cluster: Option[ClusterName]): Either[Diagnostics, JobRef] =
    JobId
      .from(raw)
      .left
      .map(validation("invalid-job-id", _))
      .map(JobRef(_, cluster, None))

  private def validation(code: String, failure: ValidationFailure): Diagnostics =
    Diagnostics.one(Diagnostic(code, failure.reason, Map("field" -> failure.field)))

object SqueueJsonV0043:
  def parse(
      stdout: BoundedEvidence,
      expected: NonEmptyVector[JobRef]
  ): Either[Diagnostics, Vector[JobObservation]] =
    for
      text <- EvidenceText.decode(stdout.bytes)
      json <- parser
        .parse(text)
        .left
        .map(error => diagnostics("invalid-squeue-json", error.message))
      jobs <- json.hcursor
        .downField("jobs")
        .focus
        .flatMap(_.asArray)
        .toRight(diagnostics("invalid-squeue-json", "jobs must be an array"))
      parsed <- jobs.toVector.traverse(parseJob(_, stdout, expected))
    yield parsed.flatten

  private def parseJob(
      json: Json,
      stdout: BoundedEvidence,
      expected: NonEmptyVector[JobRef]
  ): Either[Diagnostics, Vector[JobObservation]] =
    for
      fields <- json.asObject.toRight(
        diagnostics("invalid-squeue-job", "job entry must be an object")
      )
      jobText <- stringOrNumber(fields, "job_id")
        .orElse(stringOrNumber(fields, "job_id_raw"))
        .toRight(diagnostics("invalid-squeue-job", "job_id is required"))
      parsed <- parseJobIdentity(jobText)
      arrayJobText <- noValUnsigned(fields, "array_job_id")
      arrayTaskText <- noValUnsigned(fields, "array_task_id")
      baseText = arrayJobText.filter(_ != "0").getOrElse(parsed._1)
      explicitIndex = arrayTaskText.flatMap(_.toIntOption)
      arrayIndex <- explicitIndex
        .traverse(raw =>
          ArrayIndex
            .from(raw)
            .left
            .map(problem => diagnostics("invalid-array-index", problem.reason))
        )
        .map(_.orElse(parsed._2))
      jobId <- JobId
        .from(baseText)
        .left
        .map(problem => diagnostics("invalid-squeue-job-id", problem.reason))
      matchedJobs <- matchedJobRefs(
        jobId,
        arrayIndex,
        fields("array_task_string").flatMap(_.asString),
        expected
      )
      observations <-
        if matchedJobs.isEmpty then Right(Vector.empty)
        else
          state(fields)
            .toRight(diagnostics("invalid-squeue-job", "job_state is required"))
            .map { stateText =>
              val parsedState = SlurmStateParser.parse(stateText)
              val reportedCluster = fields("cluster")
                .flatMap(_.asString)
                .flatMap(ClusterName.from(_).toOption)
              val reason = fields("state_reason")
                .flatMap(_.asString)
                .orElse(fields("reason").flatMap(_.asString))
              val timing = SlurmTiming.fromJson(fields, parsedState)
              val raw = rawFields(fields)

              matchedJobs.map { matched =>
                JobObservation(
                  job = matched.copy(cluster = reportedCluster.orElse(matched.cluster)),
                  state = parsedState,
                  freshness = Freshness.Current(stdout.observedAt),
                  reason = reason,
                  rawFields = raw,
                  evidence = EvidenceBundle(stdout),
                  timing = timing
                )
              }
            }
    yield observations

  private def stringOrNumber(fields: JsonObject, name: String): Option[String] =
    fields(name).flatMap(value => value.asString.orElse(value.asNumber.map(_.toString)))

  private def noValUnsigned(
      fields: JsonObject,
      name: String
  ): Either[Diagnostics, Option[String]] =
    fields(name) match
      case None                                            => Right(None)
      case Some(value) if value.isNull                     => Right(None)
      case Some(value) if value.isString || value.isNumber =>
        Right(value.asString.orElse(value.asNumber.map(_.toString)))
      case Some(value) =>
        value.asObject match
          case None => Left(diagnostics("invalid-squeue-job", s"$name has an invalid shape"))
          case Some(objectValue) =>
            val set = objectValue("set").flatMap(_.asBoolean).getOrElse(false)
            val infinite = objectValue("infinite").flatMap(_.asBoolean).getOrElse(false)
            if infinite then Left(diagnostics("invalid-squeue-job", s"$name cannot be infinite"))
            else if !set then Right(None)
            else
              objectValue("number")
                .flatMap(number => number.asString.orElse(number.asNumber.map(_.toString)))
                .toRight(diagnostics("invalid-squeue-job", s"$name is set without a number"))
                .map(Some(_))

  private def parseJobIdentity(raw: String): Either[Diagnostics, (String, Option[ArrayIndex])] =
    SlurmJobIdentity.parse(raw).left.map(problem => diagnostics("invalid-squeue-job-id", problem))

  private def matchedJobRefs(
      jobId: JobId,
      explicitIndex: Option[ArrayIndex],
      taskExpression: Option[String],
      expected: NonEmptyVector[JobRef]
  ): Either[Diagnostics, Vector[JobRef]] =
    explicitIndex match
      case Some(index) =>
        Right(
          expected.toVector.filter(job => job.jobId == jobId && job.arrayIndex.contains(index))
        )
      case None =>
        taskExpression.filter(_.nonEmpty) match
          case None =>
            Right(
              expected.toVector.filter(job => job.jobId == jobId && job.arrayIndex.isEmpty)
            )
          case Some(expression) =>
            expected.toVector
              .filter(_.jobId == jobId)
              .filterA(job =>
                job.arrayIndex
                  .traverse(index => ArrayTaskExpression.contains(expression, index.value))
                  .map(_.contains(true))
              )
              .left
              .map(problem => diagnostics("invalid-array-task-expression", problem))

  private def state(fields: JsonObject): Option[String] =
    fields("job_state").flatMap { value =>
      value.asString.orElse(value.asArray.flatMap(_.flatMap(_.asString).headOption))
    }

  private def rawFields(fields: JsonObject): Map[String, String] =
    fields.toIterable.map { case (name, value) => name -> value.noSpaces }.toMap

  private def diagnostics(code: String, detail: String): Diagnostics =
    Diagnostics.one(
      Diagnostic(code, "unable to parse structured Slurm output", Map("detail" -> detail.take(512)))
    )

object VersionedSqueueParsers:
  def supports(version: DataParserVersion): Boolean = version.value == "v0.0.43"

  def parse(
      version: DataParserVersion,
      stdout: BoundedEvidence,
      expected: NonEmptyVector[JobRef]
  ): Either[Diagnostics, Vector[JobObservation]] =
    version.value match
      case "v0.0.43"   => SqueueJsonV0043.parse(stdout, expected)
      case unsupported =>
        Left(
          Diagnostics.one(
            Diagnostic(
              "unsupported-data-parser",
              "no squeue codec is registered for the requested data_parser",
              Map("requested" -> unsupported)
            )
          )
        )

object SacctParsable2:
  /** One accounting row's contribution to the batch.
    *
    * A row that is legitimately not an allocation record for a requested job -- a job step, a
    * grouped array summary, or a job the caller did not ask about -- is skipped. A row we cannot
    * interpret is a parse failure: `sacct` output is machine generated against a fixed `--format`,
    * so an uninterpretable row means our understanding of the format is wrong and every other row
    * is equally suspect. It must never be reported as the requested job being absent.
    */
  private enum RowOutcome:
    case Record(value: AccountingRecord)
    case NotAnAllocation
    case Malformed(problem: Diagnostic)

  def parse(
      stdout: BoundedEvidence,
      expected: NonEmptyVector[JobRef]
  ): Either[Diagnostics, Vector[AccountingRecord]] =
    EvidenceText.decode(stdout.bytes).flatMap { raw =>
      val rows = raw.linesIterator
        .filter(_.nonEmpty)
        .toVector
        .map(line => parseLine(line, stdout, expected))
      val malformed = rows.collect { case RowOutcome.Malformed(problem) => problem }
      // `fromVector` fails exactly when nothing was malformed, which is the success case here.
      Diagnostics.fromVector(malformed) match
        case Right(problems) => Left(problems)
        case Left(_)         => Right(rows.collect { case RowOutcome.Record(value) => value })
    }

  private def parseLine(
      line: String,
      stdout: BoundedEvidence,
      expected: NonEmptyVector[JobRef]
  ): RowOutcome =
    // Reason is the final `--format` field, so any further delimiter belongs to its text rather
    // than starting a fifth field. Splitting with a limit keeps a reason containing '|' readable
    // instead of misreading a legitimate row as malformed.
    line.split("\\|", 4).toVector match
      case Vector(jobText, stateText, exitText, reason) =>
        SlurmJobIdentity.classifyAllocation(jobText.trim) match
          case AllocationRow.NotAnAllocation  => RowOutcome.NotAnAllocation
          case AllocationRow.Invalid(problem) =>
            RowOutcome.Malformed(rowProblem("invalid-accounting-job-id", problem, line))
          case AllocationRow.Allocation(base, arrayIndex) =>
            JobId.from(base) match
              case Left(problem) =>
                RowOutcome.Malformed(
                  rowProblem("invalid-accounting-job-id", problem.reason, line)
                )
              case Right(jobId) =>
                expected.toVector.find(job =>
                  job.jobId == jobId && job.arrayIndex == arrayIndex
                ) match
                  case None          => RowOutcome.NotAnAllocation
                  case Some(matched) =>
                    parseExit(exitText.trim) match
                      case Left(_) =>
                        RowOutcome.Malformed(
                          rowProblem(
                            "invalid-accounting-exit-code",
                            "exit code must have numeric status:signal form",
                            line
                          )
                        )
                      case Right(exit) =>
                        val state = SlurmStateParser.parse(stateText.trim)
                        RowOutcome.Record(
                          AccountingRecord(
                            job = matched,
                            state = state,
                            exitStatus = exit,
                            outcome = outcome(state, exit, reason.trim),
                            freshness = Freshness.Current(stdout.observedAt),
                            rawFields = Map(
                              "JobID" -> jobText.trim,
                              "State" -> stateText.trim,
                              "ExitCode" -> exitText.trim,
                              "Reason" -> reason.trim
                            ),
                            evidence = EvidenceBundle(stdout)
                          )
                        )
      case fields =>
        RowOutcome.Malformed(
          rowProblem(
            "invalid-accounting-row",
            s"expected 4 delimited fields, received ${fields.size}",
            line
          )
        )

  private def rowProblem(code: String, detail: String, line: String): Diagnostic =
    Diagnostic(
      code,
      "unable to parse Slurm accounting output",
      Map("detail" -> detail.take(512), "row" -> line.take(512))
    )

  private def parseExit(raw: String): Either[Diagnostics, Option[ExitStatus]] =
    if raw.isEmpty || raw.equalsIgnoreCase("Unknown") then Right(None)
    else
      raw.split(":", -1).toVector match
        case Vector(code, signal) =>
          (code.toIntOption, signal.toIntOption) match
            case (Some(exitCode), Some(signalCode)) =>
              Right(Some(ExitStatus(exitCode, Option.when(signalCode != 0)(signalCode))))
            case _ =>
              Left(
                diagnostics("invalid-exit-code", "exit code must have numeric status:signal form")
              )
        case _ => Left(diagnostics("invalid-exit-code", "exit code must have status:signal form"))

  private def outcome(
      state: SlurmState,
      exit: Option[ExitStatus],
      reason: String
  ): Option[WorkloadOutcome] =
    state match
      case SlurmState.Completed if exit.forall(_.code == 0) =>
        Some(WorkloadOutcome.Completed(0))
      case SlurmState.Completed   => Some(failedOutcome(exit, reason, "non-zero-exit"))
      case SlurmState.Failed      => Some(failedOutcome(exit, reason, "workload-failed"))
      case SlurmState.Preempted   => Some(failedOutcome(exit, reason, "workload-preempted"))
      case SlurmState.OutOfMemory => Some(WorkloadOutcome.OutOfMemory)
      case SlurmState.TimedOut    => Some(WorkloadOutcome.TimeLimitExceeded)
      case SlurmState.Cancelled   => Some(WorkloadOutcome.Cancelled)
      case SlurmState.NodeFailure => Some(WorkloadOutcome.NodeFailure)
      case SlurmState.Pending | SlurmState.Running | SlurmState.Completing | SlurmState.Requeued |
          SlurmState.RequeueHeld | SlurmState.RequeueFederation | SlurmState.SpecialExit |
          SlurmState.Unknown(_) =>
        None

  private def failedOutcome(
      exit: Option[ExitStatus],
      reason: String,
      code: String
  ): WorkloadOutcome =
    WorkloadOutcome.Failed(
      exit.map(_.code),
      Diagnostics.one(
        Diagnostic(
          code,
          "Slurm recorded a non-success terminal state",
          Map("reason" -> reason)
        )
      )
    )

  private def diagnostics(code: String, detail: String): Diagnostics =
    Diagnostics.one(
      Diagnostic(code, "unable to parse Slurm accounting output", Map("detail" -> detail.take(512)))
    )

private enum AllocationRow derives CanEqual:
  case Allocation(base: String, arrayIndex: Option[ArrayIndex])
  case NotAnAllocation
  case Invalid(problem: String)

private object SlurmJobIdentity:
  def parse(raw: String): Either[String, (String, Option[ArrayIndex])] =
    val separator = raw.lastIndexOf('_')
    if separator < 0 then Right(raw -> None)
    else
      val base = raw.take(separator)
      val indexText = raw.drop(separator + 1)
      indexText.toIntOption match
        case None        => Left("array job id must end in a numeric task index")
        case Some(value) =>
          ArrayIndex.from(value).left.map(_.reason).map(index => base -> Some(index))

  /** Parses an allocation identity from sacct's `JobID` field. Grouped array summaries and step
    * rows are intentionally excluded: neither identifies one requested allocation.
    */
  def parseAllocation(raw: String): Either[String, (String, Option[ArrayIndex])] =
    classifyAllocation(raw) match
      case AllocationRow.Allocation(base, index) => Right(base -> index)
      case AllocationRow.NotAnAllocation         =>
        Left("job-step rows and grouped array summaries are not allocation records")
      case AllocationRow.Invalid(problem) => Left(problem)

  /** Separates "this row is not an allocation record" from "this row is unreadable".
    *
    * Only the second is a parse failure. Conflating them lets a row we failed to understand look
    * like the requested job simply being absent from accounting.
    */
  def classifyAllocation(raw: String): AllocationRow =
    if raw.contains(".") then AllocationRow.NotAnAllocation
    else if raw.endsWith("]") && raw.contains("_[") then AllocationRow.NotAnAllocation
    else
      parse(raw) match
        case Right((base, index)) => AllocationRow.Allocation(base, index)
        case Left(problem)        => AllocationRow.Invalid(problem)

private object ArrayTaskExpression:
  private val Range = "([0-9]+)-([0-9]+)(?::([0-9]+))?".r

  def contains(raw: String, index: Int): Either[String, Boolean] =
    val expression = raw.stripPrefix("[").stripSuffix("]").takeWhile(_ != '%')
    Either
      .cond(
        raw.length <= 4096 && expression.nonEmpty,
        expression.split(",", -1).toVector,
        "array task expression is empty or exceeds 4096 characters"
      )
      .flatMap(_.traverse(segmentContains(_, index)).map(_.contains(true)))

  private def segmentContains(segment: String, index: Int): Either[String, Boolean] =
    segment match
      case Range(firstText, lastText, stepText) =>
        for
          first <- number(firstText)
          last <- number(lastText)
          step <- Option(stepText).traverse(number).map(_.getOrElse(1))
          _ <- Either.cond(first <= last, (), "array task range is descending")
          _ <- Either.cond(step > 0, (), "array task range step must be positive")
        yield index >= first && index <= last && (index - first) % step == 0
      case value => number(value).map(_ == index)

  private def number(raw: String): Either[String, Int] =
    raw.toIntOption.filter(_ >= 0).toRight("array task expression contains a non-numeric index")

object ScontrolOneliner:
  private val FieldStart = "(?:^|\\s)([A-Za-z][A-Za-z0-9_./:-]*)=".r

  def parse(stdout: BoundedEvidence): Either[Diagnostics, Map[String, String]] =
    EvidenceText.decode(stdout.bytes).flatMap { raw =>
      val matches = FieldStart.findAllMatchIn(raw).toVector
      matches.headOption match
        case None =>
          Left(
            Diagnostics.one(
              Diagnostic("invalid-scontrol-response", "expected at least one key=value field")
            )
          )
        case Some(first) if raw.substring(0, first.start).trim.nonEmpty =>
          Left(
            Diagnostics.one(
              Diagnostic("invalid-scontrol-response", "unexpected text before the first field")
            )
          )
        case Some(_) =>
          Right(
            matches.zipWithIndex.map { case (current, index) =>
              val end = matches.lift(index + 1).fold(raw.length)(_.start)
              current.group(1) -> raw.substring(current.end, end).trim
            }.toMap
          )
    }

  def timing(fields: Map[String, String]): JobTiming =
    val state = fields
      .get("JobState")
      .fold[SlurmState](SlurmState.Unknown("missing JobState"))(SlurmStateParser.parse)
    SlurmTiming.fromText(fields, state)

object SlurmStateParser:
  def parse(raw: String): SlurmState =
    raw.trim
      .toUpperCase(Locale.ROOT)
      .takeWhile(character => character != '+' && !character.isWhitespace) match
      case "PENDING" | "PD"        => SlurmState.Pending
      case "RUNNING" | "R"         => SlurmState.Running
      case "COMPLETING" | "CG"     => SlurmState.Completing
      case "COMPLETED" | "CD"      => SlurmState.Completed
      case "FAILED" | "F"          => SlurmState.Failed
      case "CANCELLED" | "CA"      => SlurmState.Cancelled
      case "OUT_OF_MEMORY" | "OOM" => SlurmState.OutOfMemory
      case "TIMEOUT" | "TO"        => SlurmState.TimedOut
      case "NODE_FAIL" | "NF"      => SlurmState.NodeFailure
      case "PREEMPTED" | "PR"      => SlurmState.Preempted
      case "REQUEUED" | "RQ"       => SlurmState.Requeued
      case "REQUEUE_HOLD" | "RH"   => SlurmState.RequeueHeld
      case "REQUEUE_FED" | "RF"    => SlurmState.RequeueFederation
      case "SPECIAL_EXIT" | "SE"   => SlurmState.SpecialExit
      case _                       => SlurmState.Unknown(raw)

private object SlurmTiming:
  private val MissingText = Set("", "NONE", "N/A", "NA", "UNKNOWN", "(NULL)", "NULL")
  private val DayTime = "([0-9]+)-([0-9]+):([0-9]{2}):([0-9]{2})".r
  private val ClockTime = "([0-9]+):([0-9]{2}):([0-9]{2})".r

  def fromJson(fields: JsonObject, state: SlurmState): JobTiming =
    JobTiming(
      start = fields
        .apply("start_time")
        .flatMap(timestamp)
        .map(value => classifyStart(state, value)),
      projectedEndAt = fields.apply("end_time").flatMap(timestamp),
      timeLimit = fields("time_limit").fold[ObservedTimeLimit](
        ObservedTimeLimit.Unknown(None)
      )(timeLimit)
    )

  def fromText(fields: Map[String, String], state: SlurmState): JobTiming =
    JobTiming(
      start = fields
        .get("StartTime")
        .flatMap(textTimestamp)
        .map(value => classifyStart(state, value)),
      projectedEndAt = fields.get("EndTime").flatMap(textTimestamp),
      timeLimit = fields
        .get("TimeLimit")
        .fold[ObservedTimeLimit](ObservedTimeLimit.Unknown(None))(textTimeLimit)
    )

  private def classifyStart(state: SlurmState, value: SchedulerTimestamp): JobStart =
    state match
      case SlurmState.Pending => JobStart.Expected(value)
      case SlurmState.Running | SlurmState.Completing | SlurmState.Completed | SlurmState.Failed |
          SlurmState.Cancelled | SlurmState.OutOfMemory | SlurmState.TimedOut |
          SlurmState.NodeFailure | SlurmState.Preempted =>
        JobStart.Actual(value)
      case SlurmState.Requeued | SlurmState.RequeueHeld | SlurmState.RequeueFederation |
          SlurmState.SpecialExit | SlurmState.Unknown(_) =>
        JobStart.Reported(value)

  private def timestamp(value: Json): Option[SchedulerTimestamp] =
    value.asNumber
      .flatMap(_.toLong)
      .flatMap(epoch)
      .orElse(value.asString.flatMap(textTimestamp))
      .orElse(
        value.asObject.flatMap { fields =>
          val set = fields("set").flatMap(_.asBoolean).getOrElse(false)
          val infinite = fields("infinite").flatMap(_.asBoolean).getOrElse(false)
          Option
            .when(set && !infinite)(
              fields("number").flatMap(number =>
                number.asNumber.flatMap(_.toLong).orElse(number.asString.flatMap(_.toLongOption))
              )
            )
            .flatten
            .flatMap(epoch)
        }
      )

  private def epoch(seconds: Long): Option[SchedulerTimestamp] =
    Option
      .when(seconds > 0L)(seconds)
      .flatMap(value => Try(Instant.ofEpochSecond(value)).toOption)
      .map(SchedulerTimestamp.Absolute.apply)

  private def textTimestamp(raw: String): Option[SchedulerTimestamp] =
    val normalized = raw.trim
    if MissingText.contains(normalized.toUpperCase(Locale.ROOT)) then None
    else
      Try(Instant.parse(normalized)).toOption
        .map(SchedulerTimestamp.Absolute.apply)
        .orElse(
          Try(OffsetDateTime.parse(normalized).toInstant).toOption
            .map(SchedulerTimestamp.Absolute.apply)
        )
        .orElse(
          Try(LocalDateTime.parse(normalized)).toOption
            .map(SchedulerTimestamp.SiteLocal.apply)
        )

  private def timeLimit(value: Json): ObservedTimeLimit =
    value.asNumber
      .flatMap(_.toLong)
      .map(minutes)
      .orElse(value.asString.map(textTimeLimit))
      .orElse(
        value.asObject.map { fields =>
          val set = fields("set").flatMap(_.asBoolean).getOrElse(false)
          val infinite = fields("infinite").flatMap(_.asBoolean).getOrElse(false)
          if infinite then ObservedTimeLimit.Unlimited
          else if set then
            fields("number")
              .flatMap(number =>
                number.asNumber.flatMap(_.toLong).orElse(number.asString.flatMap(_.toLongOption))
              )
              .fold[ObservedTimeLimit](ObservedTimeLimit.Unknown(Some(value.noSpaces)))(minutes)
          else ObservedTimeLimit.Unknown(None)
        }
      )
      .getOrElse(ObservedTimeLimit.Unknown(Some(value.noSpaces.take(512))))

  private def textTimeLimit(raw: String): ObservedTimeLimit =
    val normalized = raw.trim
    normalized.toUpperCase(Locale.ROOT) match
      case "UNLIMITED" | "INFINITE"              => ObservedTimeLimit.Unlimited
      case "PARTITION_LIMIT" | "PARTITION-LIMIT" =>
        ObservedTimeLimit.PartitionDefault
      case missing if MissingText.contains(missing) =>
        ObservedTimeLimit.Unknown(Option(normalized).filter(_.nonEmpty))
      case _ =>
        val totalMinutes = normalized match
          case DayTime(days, hours, minutes, seconds) =>
            durationMinutes(days, hours, minutes, seconds)
          case ClockTime(hours, minutes, seconds) =>
            durationMinutes("0", hours, minutes, seconds)
          case value => value.toLongOption.filter(_ > 0L)
        totalMinutes.fold[ObservedTimeLimit](
          ObservedTimeLimit.Unknown(Some(normalized.take(512)))
        )(minutes)

  private def durationMinutes(
      days: String,
      hours: String,
      minutes: String,
      seconds: String
  ): Option[Long] =
    val totalSeconds =
      BigInt(days) * 86400 + BigInt(hours) * 3600 + BigInt(minutes) * 60 + BigInt(seconds)
    val roundedMinutes = (totalSeconds + 59) / 60
    Option.when(roundedMinutes > 0 && roundedMinutes.isValidLong)(roundedMinutes.longValue)

  private def minutes(value: Long): ObservedTimeLimit =
    WallTimeMinutes
      .from(value)
      .fold(
        _ => ObservedTimeLimit.Unknown(Some(value.toString)),
        ObservedTimeLimit.Limited.apply
      )
