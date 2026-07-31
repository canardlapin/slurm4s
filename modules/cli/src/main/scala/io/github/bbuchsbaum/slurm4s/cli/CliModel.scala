package io.github.bbuchsbaum.slurm4s.cli

import cats.Order
import cats.Show
import cats.data.NonEmptyChain
import io.github.bbuchsbaum.slurm4s.core.Diagnostics
import io.github.bbuchsbaum.slurm4s.core.JobRef
import io.github.bbuchsbaum.slurm4s.core.LaunchSpec
import io.github.bbuchsbaum.slurm4s.core.JobTiming
import io.github.bbuchsbaum.slurm4s.core.SitePolicyViolation
import io.github.bbuchsbaum.slurm4s.core.SiteResolution
import io.github.bbuchsbaum.slurm4s.core.SubmissionAttempt
import io.github.bbuchsbaum.slurm4s.core.ValidationFailure

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
  given CanEqual[Type, Type] = CanEqual.derived
  given Order[Type] = Order.from((left, right) => left.compareTo(right))
  given Ordering[Type] = summon[Order[Type]].toOrdering
  given Show[Type] = Show.show(identity)
type DataParserVersion = DataParserVersion.Type

final case class SlurmCliSettings(
    dataParser: DataParserVersion,
    commandPolicy: CommandPolicy
) derives CanEqual

final case class PreparedSubmission(
    spec: LaunchSpec,
    scriptPath: String,
    stdoutPath: String,
    stderrPath: String,
    siteResolution: Option[io.github.bbuchsbaum.slurm4s.core.SiteResolution] = None
)

trait SubmissionPlanner[F[_]]:
  def prepare(spec: LaunchSpec): F[Either[Diagnostics, PreparedSubmission]]

enum SiteSubmissionResult derives CanEqual:
  case PreflightRejected(failures: NonEmptyChain[SitePolicyViolation])
  case Attempted(resolution: SiteResolution, result: SubmissionAttempt)

final case class FocusedJobDiagnostic(
    job: JobRef,
    fields: Map[String, String],
    evidence: io.github.bbuchsbaum.slurm4s.core.EvidenceBundle,
    timing: JobTiming = JobTiming.unknown
) derives CanEqual
