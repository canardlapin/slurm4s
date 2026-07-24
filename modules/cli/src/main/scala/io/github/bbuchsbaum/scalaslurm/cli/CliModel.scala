package io.github.bbuchsbaum.scalaslurm.cli

import cats.data.NonEmptyChain
import io.github.bbuchsbaum.scalaslurm.core.Diagnostics
import io.github.bbuchsbaum.scalaslurm.core.JobRef
import io.github.bbuchsbaum.scalaslurm.core.JobRequest
import io.github.bbuchsbaum.scalaslurm.core.JobTiming
import io.github.bbuchsbaum.scalaslurm.core.SitePolicyViolation
import io.github.bbuchsbaum.scalaslurm.core.SiteResolution
import io.github.bbuchsbaum.scalaslurm.core.SubmissionAttempt
import io.github.bbuchsbaum.scalaslurm.core.ValidationFailure

object DataParserVersion:
  opaque type Type = String
  private val Pattern = "v[0-9]+\\.[0-9]+\\.[0-9]+".r

  def from(raw: String): Either[ValidationFailure, Type] =
    Either.cond(
      raw != null && Pattern.matches(raw),
      raw,
      ValidationFailure("dataParserVersion", "must have form vN.N.N")
    )

  extension (version: Type) def value: String = version
type DataParserVersion = DataParserVersion.Type

final case class SlurmCliSettings(
    dataParser: DataParserVersion,
    commandPolicy: CommandPolicy
) derives CanEqual

final case class PreparedSubmission[A](
    request: JobRequest[A],
    scriptPath: String,
    stdoutPath: String,
    stderrPath: String,
    siteResolution: Option[io.github.bbuchsbaum.scalaslurm.core.SiteResolution] = None
)

trait SubmissionPlanner[F[_]]:
  def prepare[A](request: JobRequest[A]): F[Either[Diagnostics, PreparedSubmission[A]]]

enum SiteSubmissionResult derives CanEqual:
  case PreflightRejected(failures: NonEmptyChain[SitePolicyViolation])
  case Attempted(resolution: SiteResolution, result: SubmissionAttempt)

final case class FocusedJobDiagnostic(
    job: JobRef,
    fields: Map[String, String],
    evidence: io.github.bbuchsbaum.scalaslurm.core.EvidenceBundle,
    timing: JobTiming = JobTiming.unknown
) derives CanEqual
