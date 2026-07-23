package io.github.bbuchsbaum.scalaslurm.cli

import cats.data.NonEmptyVector
import cats.syntax.all.*
import io.circe.Json
import io.circe.JsonObject
import io.circe.parser
import io.github.bbuchsbaum.scalaslurm.core.*

import java.nio.ByteBuffer
import java.nio.charset.CharacterCodingException
import java.nio.charset.CodingErrorAction
import java.nio.charset.StandardCharsets

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
      reportedCluster = fields("cluster")
        .flatMap(_.asString)
        .flatMap(ClusterName.from(_).toOption)
      stateText <- state(fields).toRight(diagnostics("invalid-squeue-job", "job_state is required"))
      reason = fields("state_reason")
        .flatMap(_.asString)
        .orElse(fields("reason").flatMap(_.asString))
      raw = selectedRaw(
        fields,
        Vector(
          "job_id",
          "job_id_raw",
          "array_job_id",
          "array_task_id",
          "array_task_string",
          "job_state",
          "state_reason",
          "reason",
          "cluster"
        )
      )
    yield matchedJobs.map { matched =>
      JobObservation(
        job = matched.copy(cluster = reportedCluster.orElse(matched.cluster)),
        state = SlurmStateParser.parse(stateText),
        freshness = Freshness.Current(stdout.observedAt),
        reason = reason,
        rawFields = raw,
        evidence = EvidenceBundle(stdout)
      )
    }

  private def stringOrNumber(fields: JsonObject, name: String): Option[String] =
    fields(name).flatMap(value => value.asString.orElse(value.asNumber.map(_.toString)))

  private def noValUnsigned(
      fields: JsonObject,
      name: String
  ): Either[Diagnostics, Option[String]] =
    fields(name) match
      case None | Some(Json.Null)                          => Right(None)
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
          Vector(
            expected.toVector
              .find(job => job.jobId == jobId && job.arrayIndex.contains(index))
              .getOrElse(JobRef(jobId, None, Some(index)))
          )
        )
      case None =>
        taskExpression.filter(_.nonEmpty) match
          case None =>
            Right(
              Vector(
                expected.toVector
                  .find(job => job.jobId == jobId && job.arrayIndex.isEmpty)
                  .getOrElse(JobRef(jobId, None, None))
              )
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

  private def selectedRaw(fields: JsonObject, names: Vector[String]): Map[String, String] =
    names.flatMap(name => fields(name).map(value => name -> value.noSpaces)).toMap

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
  def parse(
      stdout: BoundedEvidence,
      expected: NonEmptyVector[JobRef]
  ): Either[Diagnostics, Vector[AccountingRecord]] =
    EvidenceText.decode(stdout.bytes).flatMap { raw =>
      val lines = raw.linesIterator.filter(_.nonEmpty).toVector
      lines.traverse(parseLine(_, stdout, expected))
    }

  private def parseLine(
      line: String,
      stdout: BoundedEvidence,
      expected: NonEmptyVector[JobRef]
  ): Either[Diagnostics, AccountingRecord] =
    line.split("\\|", -1).toVector match
      case Vector(jobText, stateText, exitText, reason) =>
        for
          identity <- SlurmJobIdentity
            .parse(jobText)
            .left
            .map(problem => diagnostics("invalid-accounting-job-id", problem))
          jobId <- JobId
            .from(identity._1)
            .left
            .map(problem => diagnostics("invalid-accounting-job-id", problem.reason))
          exit <- parseExit(exitText)
          matched = expected.toVector.find(job =>
            job.jobId == jobId && job.arrayIndex == identity._2
          )
          job = JobRef(jobId, matched.flatMap(_.cluster), identity._2)
          state = SlurmStateParser.parse(stateText)
        yield AccountingRecord(
          job = job,
          state = state,
          exitStatus = exit,
          outcome = outcome(state, exit, reason),
          freshness = Freshness.Current(stdout.observedAt),
          rawFields = Map(
            "JobIDRaw" -> jobText,
            "State" -> stateText,
            "ExitCode" -> exitText,
            "Reason" -> reason
          ),
          evidence = EvidenceBundle(stdout)
        )
      case _ =>
        Left(diagnostics("invalid-accounting-row", "expected exactly four pipe-delimited fields"))

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
      case SlurmState.Pending | SlurmState.Running | SlurmState.Completing |
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
  def parse(stdout: BoundedEvidence): Either[Diagnostics, Map[String, String]] =
    EvidenceText.decode(stdout.bytes).flatMap { raw =>
      val tokens = raw.trim.split("\\s+").toVector.filter(_.nonEmpty)
      tokens
        .traverse { token =>
          token.split("=", 2).toVector match
            case Vector(key, value) if key.nonEmpty => Right(key -> value)
            case _                                  =>
              Left(
                Diagnostics
                  .one(Diagnostic("invalid-scontrol-response", "expected key=value tokens"))
              )
        }
        .map(_.toMap)
    }

object SlurmStateParser:
  def parse(raw: String): SlurmState =
    raw.trim.toUpperCase.takeWhile(character => character != '+' && !character.isWhitespace) match
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
      case _                       => SlurmState.Unknown(raw)
