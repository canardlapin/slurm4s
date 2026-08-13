package io.github.bbuchsbaum.slurm4s.cli

import cats.Monad
import cats.data.NonEmptyVector
import cats.syntax.all.*
import io.github.bbuchsbaum.slurm4s.core.*

import java.util.Locale

final class SlurmCliScheduler[F[_]: Monad](
    executor: CommandExecutor[F],
    planner: SubmissionPlanner[F],
    settings: SlurmCliSettings
) extends Scheduler[F]
    with QueueReader[F]:

  def capabilities: F[SchedulerQueryResult[SchedulerCapabilities]] =
    for
      version <- executor.execute(SlurmCommands.versionProbe, settings.commandPolicy)
      parsers <- executor.execute(SlurmCommands.parserProbe, settings.commandPolicy)
      accounting <- executor.execute(SlurmCommands.accountingProbe, settings.commandPolicy)
      config <- executor.execute(SlurmCommands.configProbe, settings.commandPolicy)
    yield CapabilityParser.parse(version, parsers, accounting, config, settings.dataParser)

  def submit(spec: LaunchSpec): F[SubmissionAttempt] =
    planner.prepare(spec).flatMap {
      case Left(diagnostics) => SubmissionAttempt.PreparationFailed(diagnostics).pure[F]
      case Right(prepared)   => submitPrepared(prepared)
    }

  def submitAt(
      spec: LaunchSpec,
      profile: SiteProfile,
      intent: SiteIntent
  ): F[SiteSubmissionResult] =
    profile.resolve(spec, intent) match
      case Left(failures)    => SiteSubmissionResult.PreflightRejected(failures).pure[F]
      case Right(resolution) =>
        planner.prepare(spec).flatMap {
          case Left(diagnostics) =>
            SiteSubmissionResult
              .Attempted(resolution, SubmissionAttempt.PreparationFailed(diagnostics))
              .pure[F]
          case Right(prepared) =>
            submitPrepared(prepared.copy(siteResolution = Some(resolution)))
              .map(result => SiteSubmissionResult.Attempted(resolution, result))
        }

  def submitPrepared(prepared: PreparedSubmission): F[SubmissionAttempt] =
    executor.execute(SlurmCommands.submit(prepared), settings.commandPolicy).map {
      case InvocationResult.Exited(0, stdout, stderr) =>
        val evidence = retainedBundle(stdout, stderr)
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
            retainedBundle(stderr, stdout)
          )
        )
      case InvocationResult.TimedOut(_, stdout, stderr) =>
        SubmissionAttempt.Completed(
          Submission.AcceptanceUnknown(
            AcceptanceUncertainty.TransportInterrupted,
            retainedBundle(stdout, stderr)
          )
        )
      case failed: InvocationResult.SpawnFailed => SubmissionAttempt.InvocationFailed(failed)
    }

  def observe(jobs: NonEmptyVector[JobRef]): F[SchedulerQueryResult[ObservationBatch]] =
    executor.execute(SlurmCommands.observe(jobs, settings.dataParser), settings.commandPolicy).map {
      result =>
        result match
          case InvocationResult.Exited(code, stdout, stderr)
              if code != 0 && namesAbsentJobs(stderr) =>
            // Leaving the queue is how every job ends, and `squeue` reports it by refusing the
            // query. Absence may only be claimed for jobs the response actually accounts for.
            VersionedSqueueParsers.parse(settings.dataParser, stdout, jobs) match
              case Right(parsed) =>
                // A readable response names everything still queued, so whatever it omitted is
                // genuinely gone -- including the case where it lists nothing at all.
                observationBatch(parsed, jobs, result, absentWhenEmpty = true)
              case Left(_) if jobs.length == 1 =>
                // Exactly one job was named, so it is the one squeue rejected.
                observationBatch(Vector.empty, jobs, result, absentWhenEmpty = true)
              case Left(_) =>
                // Several jobs were named and none of the queue state came back readable. It is
                // not knowable which of them left the queue, and some may still be running, so
                // reporting them all absent would be a fabricated answer rather than a cautious
                // one. Say that the response could not be interpreted instead.
                SchedulerQueryResult.ParseFailed(
                  Diagnostics.one(
                    Diagnostic(
                      "squeue-absence-ambiguous",
                      "squeue reported an invalid job id without readable queue state, so which of the requested jobs left the queue is unknown",
                      Map("requestedJobs" -> jobs.length.toString)
                    )
                  ),
                  evidenceOf(result)
                )
          case _ =>
            queryResult(
              result,
              jobs,
              (stdout, expected) =>
                VersionedSqueueParsers.parse(settings.dataParser, stdout, expected)
            ) match
              case SchedulerQueryResult.Succeeded(parsed) =>
                observationBatch(parsed, jobs, result, absentWhenEmpty = false)
              case SchedulerQueryResult.InvocationFailed(failure) =>
                SchedulerQueryResult.InvocationFailed(failure)
              case SchedulerQueryResult.ParseFailed(diagnostics, evidence) =>
                SchedulerQueryResult.ParseFailed(diagnostics, evidence)
              case SchedulerQueryResult.Empty(observedAt, evidence) =>
                SchedulerQueryResult.Empty(observedAt, evidence)
    }

  def listJobs(query: QueueQuery, page: Page): F[SchedulerQueryResult[QueuePage]] =
    executor
      .execute(SlurmCommands.listJobs(query, settings.dataParser), settings.commandPolicy)
      .map { result =>
        result match
          case InvocationResult.Exited(0, stdout, stderr) =>
            VersionedSqueueParsers.list(settings.dataParser, stdout) match
              case Right(Vector()) =>
                SchedulerQueryResult.Empty(
                  stdout.observedAt,
                  retainedBundle(stdout, stderr)
                )
              case Right(jobs) =>
                val evidence = retainedBundle(stdout, stderr)
                SchedulerQueryResult.Succeeded(
                  QueuePage.from(
                    jobs,
                    page,
                    Freshness.Current(stdout.observedAt),
                    evidence
                  )
                )
              case Left(diagnostics) =>
                SchedulerQueryResult.ParseFailed(
                  diagnostics,
                  retainedBundle(stdout, stderr)
                )
          case failed => SchedulerQueryResult.InvocationFailed(failed)
      }

  /** Combine parsed observations with the requested jobs the response did not mention.
    *
    * `absentWhenEmpty` distinguishes the two ways a response can carry no observations. A
    * successful query that simply listed nothing is `Empty` -- the scheduler had nothing to say. A
    * query the scheduler refused because the named jobs are no longer queued is a batch of
    * `NotFound`, which is a statement about those jobs rather than about the query.
    */
  private def observationBatch(
      parsed: Vector[JobObservation],
      jobs: NonEmptyVector[JobRef],
      result: InvocationResult,
      absentWhenEmpty: Boolean
  ): SchedulerQueryResult[ObservationBatch] =
    // The parser embedded the full captured response in every observation so that it could
    // interpret it; narrow that to the retention bound now that interpretation is done.
    val observations = parsed.map(value => value.copy(evidence = retained(value.evidence)))
    val evidence = evidenceOf(result)
    val returned = observations.map(value => identity(value.job)).toSet
    val missing = jobs.toVector
      .filterNot(job => returned.contains(identity(job)))
      .map(job =>
        ObservationResult.NotFound(job, Freshness.Current(evidence.primary.observedAt), evidence)
      )
    val results = observations.map(ObservationResult.Observed(_)) ++ missing
    if observations.isEmpty && !absentWhenEmpty then
      SchedulerQueryResult.Empty(evidence.primary.observedAt, evidence)
    else
      NonEmptyVector.fromVector(results) match
        case Some(values) => SchedulerQueryResult.Succeeded(ObservationBatch(values))
        case None         => SchedulerQueryResult.Empty(evidence.primary.observedAt, evidence)

  /** Recognize `squeue` reporting that a requested job is no longer in the queue.
    *
    * Recognition is deliberately conservative, mirroring the SSH transport's exit-255 policy: only
    * the documented `slurm_load_jobs error: Invalid job id specified` diagnostic is read as
    * absence. Every other non-zero exit stays an invocation failure rather than being reported as a
    * job that quietly finished.
    */
  private def namesAbsentJobs(stderr: BoundedEvidence): Boolean =
    EvidenceText
      .decode(stderr.bytes)
      .map(_.toLowerCase(Locale.ROOT))
      .exists(_.contains("invalid job id specified"))

  def accounting(jobs: NonEmptyVector[JobRef]): F[SchedulerQueryResult[AccountingBatch]] =
    executor.execute(SlurmCommands.accounting(jobs), settings.commandPolicy).map { result =>
      queryResult(result, jobs, SacctParsable2.parse) match
        case SchedulerQueryResult.Succeeded(parsed) =>
          val records = parsed.map(value => value.copy(evidence = retained(value.evidence)))
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
          CancellationResult.Acknowledged(retainedBundle(stdout, stderr))
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
            retainedBundle(stderr, stdout)
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
            retainedBundle(stdout, stderr)
          )
        )
      case failed: InvocationResult.SpawnFailed => CancellationAttempt.InvocationFailed(failed)
    }

  private def identity(job: JobRef): JobRef = job

  /** Bundle command streams for retention.
    *
    * Parsers always read the full captured bytes; this narrows only what is carried forward, so a
    * large structured response stays interpretable while its retained evidence stays bounded.
    */
  private def retainedBundle(primary: BoundedEvidence, related: BoundedEvidence): EvidenceBundle =
    val limit = settings.commandPolicy.evidenceLimit
    EvidenceBundle(primary.retained(limit), Vector(related.retained(limit)))

  private def retained(bundle: EvidenceBundle): EvidenceBundle =
    val limit = settings.commandPolicy.evidenceLimit
    EvidenceBundle(bundle.primary.retained(limit), bundle.related.map(_.retained(limit)))

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
            SchedulerQueryResult.ParseFailed(diagnostics, retainedBundle(stdout, stderr))
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
            SchedulerQueryResult.ParseFailed(diagnostics, retainedBundle(stdout, stderr))
      case failed => SchedulerQueryResult.InvocationFailed(failed)

  private def evidenceOf(result: InvocationResult): EvidenceBundle =
    result match
      case InvocationResult.Exited(_, stdout, stderr)   => retainedBundle(stdout, stderr)
      case InvocationResult.SpawnFailed(_, _, evidence) => evidence
      case InvocationResult.TimedOut(_, stdout, stderr) => retainedBundle(stdout, stderr)

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
