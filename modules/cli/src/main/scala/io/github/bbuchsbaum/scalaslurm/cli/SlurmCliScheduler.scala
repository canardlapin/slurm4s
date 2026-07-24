package io.github.bbuchsbaum.scalaslurm.cli

import cats.Monad
import cats.data.NonEmptyVector
import cats.syntax.all.*
import io.github.bbuchsbaum.scalaslurm.core.*

final class SlurmCliScheduler[F[_]: Monad](
    executor: CommandExecutor[F],
    planner: SubmissionPlanner[F],
    settings: SlurmCliSettings
) extends Scheduler[F]:

  def capabilities: F[SchedulerQueryResult[SchedulerCapabilities]] =
    for
      version <- executor.execute(SlurmCommands.versionProbe, settings.commandPolicy)
      parsers <- executor.execute(SlurmCommands.parserProbe, settings.commandPolicy)
      accounting <- executor.execute(SlurmCommands.accountingProbe, settings.commandPolicy)
      config <- executor.execute(SlurmCommands.configProbe, settings.commandPolicy)
    yield CapabilityParser.parse(version, parsers, accounting, config, settings.dataParser)

  def submit[A](request: JobRequest[A]): F[SubmissionAttempt] =
    planner.prepare(request).flatMap {
      case Left(diagnostics) => SubmissionAttempt.PreparationFailed(diagnostics).pure[F]
      case Right(prepared)   => submitPrepared(prepared)
    }

  def submitAt[A](
      request: JobRequest[A],
      profile: SiteProfile,
      intent: SiteIntent
  ): F[SiteSubmissionResult] =
    profile.resolve(request, intent) match
      case Left(failures)    => SiteSubmissionResult.PreflightRejected(failures).pure[F]
      case Right(resolution) =>
        planner.prepare(request).flatMap {
          case Left(diagnostics) =>
            SiteSubmissionResult
              .Attempted(resolution, SubmissionAttempt.PreparationFailed(diagnostics))
              .pure[F]
          case Right(prepared) =>
            submitPrepared(prepared.copy(siteResolution = Some(resolution)))
              .map(result => SiteSubmissionResult.Attempted(resolution, result))
        }

  def submitPrepared[A](prepared: PreparedSubmission[A]): F[SubmissionAttempt] =
    executor.execute(SlurmCommands.submit(prepared), settings.commandPolicy).map {
      case InvocationResult.Exited(0, stdout, stderr) =>
        val evidence = EvidenceBundle(stdout, Vector(stderr))
        SbatchParsable.parse(stdout) match
          case Right(job) => SubmissionAttempt.Completed(Submission.Accepted(job, evidence))
          case Left(_)    =>
            SubmissionAttempt.Completed(
              Submission.AcceptanceUnknown(AcceptanceUncertainty.ResponseUnparseable, evidence)
            )
      case InvocationResult.Exited(exitCode, stdout, stderr) =>
        SubmissionAttempt.Completed(
          Submission.Rejected(
            Diagnostics.one(
              Diagnostic(
                "sbatch-rejected",
                "sbatch exited without accepting the request",
                Map("exitCode" -> exitCode.toString)
              )
            ),
            EvidenceBundle(stderr, Vector(stdout))
          )
        )
      case InvocationResult.TimedOut(_, stdout, stderr) =>
        SubmissionAttempt.Completed(
          Submission.AcceptanceUnknown(
            AcceptanceUncertainty.TransportInterrupted,
            EvidenceBundle(stdout, Vector(stderr))
          )
        )
      case failed: InvocationResult.SpawnFailed => SubmissionAttempt.InvocationFailed(failed)
    }

  def observe(jobs: NonEmptyVector[JobRef]): F[SchedulerQueryResult[ObservationBatch]] =
    executor.execute(SlurmCommands.observe(jobs, settings.dataParser), settings.commandPolicy).map {
      result =>
        queryResult(
          result,
          jobs,
          (stdout, expected) => VersionedSqueueParsers.parse(settings.dataParser, stdout, expected)
        ) match
          case SchedulerQueryResult.Succeeded(observations) =>
            NonEmptyVector.fromVector(observations) match
              case Some(values) =>
                val returned = observations.map(value => identity(value.job)).toSet
                val missing = jobs.toVector
                  .filterNot(job => returned.contains(identity(job)))
                  .map { job =>
                    ObservationResult.NotFound(
                      job,
                      Freshness.Current(evidenceOf(result).primary.observedAt),
                      evidenceOf(result)
                    )
                  }
                SchedulerQueryResult.Succeeded(
                  ObservationBatch(
                    NonEmptyVector.fromVectorUnsafe(
                      values.toVector.map(ObservationResult.Observed(_)) ++ missing
                    )
                  )
                )
              case None =>
                SchedulerQueryResult.Empty(
                  observedAt = evidenceOf(result).primary.observedAt,
                  evidence = evidenceOf(result)
                )
          case SchedulerQueryResult.InvocationFailed(failure) =>
            SchedulerQueryResult.InvocationFailed(failure)
          case SchedulerQueryResult.ParseFailed(diagnostics, evidence) =>
            SchedulerQueryResult.ParseFailed(diagnostics, evidence)
          case SchedulerQueryResult.Empty(observedAt, evidence) =>
            SchedulerQueryResult.Empty(observedAt, evidence)
    }

  def accounting(jobs: NonEmptyVector[JobRef]): F[SchedulerQueryResult[AccountingBatch]] =
    executor.execute(SlurmCommands.accounting(jobs), settings.commandPolicy).map { result =>
      queryResult(result, jobs, SacctParsable2.parse) match
        case SchedulerQueryResult.Succeeded(records) =>
          NonEmptyVector.fromVector(records) match
            case Some(values) =>
              val returned = records.map(value => identity(value.job)).toSet
              val missing = jobs.toVector.filterNot(job => returned.contains(identity(job)))
              SchedulerQueryResult.Succeeded(AccountingBatch(values, missing))
            case None =>
              SchedulerQueryResult.Empty(evidenceOf(result).primary.observedAt, evidenceOf(result))
        case SchedulerQueryResult.InvocationFailed(failure) =>
          SchedulerQueryResult.InvocationFailed(failure)
        case SchedulerQueryResult.ParseFailed(diagnostics, evidence) =>
          SchedulerQueryResult.ParseFailed(diagnostics, evidence)
        case SchedulerQueryResult.Empty(observedAt, evidence) =>
          SchedulerQueryResult.Empty(observedAt, evidence)
    }

  def focused(job: JobRef): F[SchedulerQueryResult[FocusedJobDiagnostic]] =
    executor.execute(SlurmCommands.focused(job), settings.commandPolicy).map { result =>
      querySingle(result, ScontrolOneliner.parse) match
        case SchedulerQueryResult.Succeeded(fields) =>
          SchedulerQueryResult.Succeeded(
            FocusedJobDiagnostic(job, fields, evidenceOf(result), ScontrolOneliner.timing(fields))
          )
        case SchedulerQueryResult.InvocationFailed(failure) =>
          SchedulerQueryResult.InvocationFailed(failure)
        case SchedulerQueryResult.ParseFailed(diagnostics, evidence) =>
          SchedulerQueryResult.ParseFailed(diagnostics, evidence)
        case SchedulerQueryResult.Empty(observedAt, evidence) =>
          SchedulerQueryResult.Empty(observedAt, evidence)
    }

  def cancel(job: JobRef): F[CancellationAttempt] =
    executor.execute(SlurmCommands.cancel(job), settings.commandPolicy).map {
      case InvocationResult.Exited(0, stdout, stderr) =>
        CancellationAttempt.Completed(
          CancellationResult.Acknowledged(EvidenceBundle(stdout, Vector(stderr)))
        )
      case InvocationResult.Exited(exitCode, stdout, stderr) =>
        CancellationAttempt.Completed(
          CancellationResult.Rejected(
            Diagnostics.one(
              Diagnostic(
                "scancel-rejected",
                "Slurm did not acknowledge the cancellation request",
                Map("exitCode" -> exitCode.toString)
              )
            ),
            EvidenceBundle(stderr, Vector(stdout))
          )
        )
      case InvocationResult.TimedOut(_, stdout, stderr) =>
        CancellationAttempt.Completed(
          CancellationResult.Unknown(
            Diagnostics.one(
              Diagnostic(
                "cancellation-acknowledgement-unknown",
                "the scancel response deadline elapsed after process start"
              )
            ),
            EvidenceBundle(stdout, Vector(stderr))
          )
        )
      case failed: InvocationResult.SpawnFailed => CancellationAttempt.InvocationFailed(failed)
    }

  private def identity(job: JobRef): (JobId, Option[ArrayIndex]) =
    job.jobId -> job.arrayIndex

  private def queryResult[A](
      result: InvocationResult,
      expected: NonEmptyVector[JobRef],
      parse: (BoundedEvidence, NonEmptyVector[JobRef]) => Either[Diagnostics, Vector[A]]
  ): SchedulerQueryResult[Vector[A]] =
    result match
      case InvocationResult.Exited(0, stdout, stderr) =>
        parse(stdout, expected) match
          case Right(values)     => SchedulerQueryResult.Succeeded(values)
          case Left(diagnostics) =>
            SchedulerQueryResult.ParseFailed(diagnostics, EvidenceBundle(stdout, Vector(stderr)))
      case failed @ InvocationResult.Exited(_, _, _) =>
        SchedulerQueryResult.InvocationFailed(failed)
      case failed => SchedulerQueryResult.InvocationFailed(failed)

  private def querySingle[A](
      result: InvocationResult,
      parse: BoundedEvidence => Either[Diagnostics, A]
  ): SchedulerQueryResult[A] =
    result match
      case InvocationResult.Exited(0, stdout, stderr) =>
        parse(stdout) match
          case Right(value)      => SchedulerQueryResult.Succeeded(value)
          case Left(diagnostics) =>
            SchedulerQueryResult.ParseFailed(diagnostics, EvidenceBundle(stdout, Vector(stderr)))
      case failed => SchedulerQueryResult.InvocationFailed(failed)

  private def evidenceOf(result: InvocationResult): EvidenceBundle =
    result match
      case InvocationResult.Exited(_, stdout, stderr)   => EvidenceBundle(stdout, Vector(stderr))
      case InvocationResult.SpawnFailed(_, _, evidence) => evidence
      case InvocationResult.TimedOut(_, stdout, stderr) => EvidenceBundle(stdout, Vector(stderr))

private object CapabilityParser:
  def parse(
      version: InvocationResult,
      parsers: InvocationResult,
      accounting: InvocationResult,
      config: InvocationResult,
      requestedParser: DataParserVersion
  ): SchedulerQueryResult[SchedulerCapabilities] =
    version match
      case failed: InvocationResult.SpawnFailed => SchedulerQueryResult.InvocationFailed(failed)
      case failed: InvocationResult.TimedOut    => SchedulerQueryResult.InvocationFailed(failed)
      case InvocationResult.Exited(code, _, _) if code != 0 =>
        SchedulerQueryResult.InvocationFailed(version)
      case InvocationResult.Exited(_, versionOut, versionErr) =>
        val parserText = successfulText(parsers)
        val accountingAvailable = isSuccess(accounting)
        val configFields = successfulText(config).map(parseConfig).getOrElse(Map.empty)
        val parserAvailable = parserText.exists(_.contains(requestedParser.value)) &&
          VersionedSqueueParsers.supports(requestedParser)
        val evidence = Vector(versionOut, versionErr) ++ allEvidence(parsers) ++ allEvidence(
          accounting
        ) ++ allEvidence(config)
        val unknownParser = CapabilitySupport.Unknown(
          Diagnostics.one(
            Diagnostic(
              "data-parser-unavailable",
              "requested Slurm data_parser was not advertised",
              Map("requested" -> requestedParser.value)
            )
          )
        )
        SchedulerQueryResult.Succeeded(
          SchedulerCapabilities(
            slurmVersion = EvidenceText.decode(versionOut.bytes).toOption.flatMap(parseVersion),
            cluster = configFields.get("ClusterName").flatMap(ClusterName.from(_).toOption),
            structuredQueue =
              if parserAvailable then CapabilitySupport.Supported else unknownParser,
            structuredAccounting =
              if parserAvailable && accountingAvailable then CapabilitySupport.Supported
              else unknownParser,
            accounting = if accountingAvailable then CapabilitySupport.Supported
            else CapabilitySupport.Unsupported,
            arrays = configFields
              .get("MaxArraySize")
              .flatMap(_.toIntOption)
              .filter(_ > 1)
              .fold[CapabilitySupport](CapabilitySupport.Unsupported)(_ =>
                CapabilitySupport.Supported
              ),
            rawEvidence = evidence
          )
        )

  private def isSuccess(result: InvocationResult): Boolean = result match
    case InvocationResult.Exited(0, _, _) => true
    case _                                => false

  private def successfulText(result: InvocationResult): Option[String] = result match
    case InvocationResult.Exited(0, stdout, _) => EvidenceText.decode(stdout.bytes).toOption
    case _                                     => None

  private def parseVersion(raw: String): Option[String] =
    raw.trim.split("\\s+").toVector.lastOption.filter(_.exists(_.isDigit))

  private def parseConfig(raw: String): Map[String, String] =
    raw.linesIterator.flatMap { line =>
      line.split("=", 2).toVector match
        case Vector(key, value) => Some(key.trim -> value.trim)
        case _                  => None
    }.toMap

  private def allEvidence(result: InvocationResult): Vector[BoundedEvidence] = result match
    case InvocationResult.Exited(_, stdout, stderr)   => Vector(stdout, stderr)
    case InvocationResult.SpawnFailed(_, _, evidence) => evidence.all
    case InvocationResult.TimedOut(_, stdout, stderr) => Vector(stdout, stderr)
