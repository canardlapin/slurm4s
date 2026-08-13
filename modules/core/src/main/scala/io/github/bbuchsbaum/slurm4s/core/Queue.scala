package io.github.bbuchsbaum.slurm4s.core

/** Active queue states that `squeue` can filter without inventing a base-state interpretation.
  *
  * `Completing` is deliberately here rather than in [[SlurmState]]: SchedMD defines it as a state
  * flag, but accepts it as an `squeue --states` filter. An empty filter means Slurm's ordinary
  * active-job default (pending, running, and completing).
  */
enum QueueStateFilter(val slurmName: String) derives CanEqual:
  case Pending extends QueueStateFilter("PENDING")
  case Running extends QueueStateFilter("RUNNING")
  case Suspended extends QueueStateFilter("SUSPENDED")
  case Completing extends QueueStateFilter("COMPLETING")

/** A bounded query over jobs owned by the Unix user running the Slurm interpreter.
  *
  * The ownership scope is fixed to the current user in protocol v1. This keeps the agent-facing
  * operation useful for an ordinary coding agent without silently turning it into a cluster-wide
  * administrative query. Names, partitions, and states are optional Slurm-side filters.
  */
final case class QueueQuery private (
    names: Vector[JobName],
    partitions: Vector[PartitionName],
    states: Vector[QueueStateFilter]
) derives CanEqual

object QueueQuery:
  private val MaximumFiltersPerField = 64

  val currentUser: QueueQuery = QueueQuery(Vector.empty, Vector.empty, Vector.empty)

  def currentUser(
      names: Vector[JobName],
      partitions: Vector[PartitionName],
      states: Vector[QueueStateFilter]
  ): Either[ValidationFailure, QueueQuery] =
    for
      checkedNames <- checked("queueNames", names, _.value)
      checkedPartitions <- checked("queuePartitions", partitions, _.value)
      checkedStates <- checked("queueStates", states, _.slurmName)
    yield QueueQuery(checkedNames, checkedPartitions, checkedStates)

  private def checked[A](
      field: String,
      values: Vector[A],
      render: A => String
  ): Either[ValidationFailure, Vector[A]] =
    if values.size > MaximumFiltersPerField then
      Left(
        ValidationFailure(
          field,
          s"must contain at most $MaximumFiltersPerField filters"
        )
      )
    else if values.distinct.size != values.size then
      Left(ValidationFailure(field, "must not contain duplicate filters"))
    else Right(values.sortBy(render))

/** A response item ceiling, not a snapshot offset or cursor.
  *
  * `squeue` does not expose stable snapshot pagination. A page therefore returns the first
  * deterministically ordered items from one bounded command result and says explicitly whether
  * additional matches were omitted. Callers refine the query rather than pretending that a later
  * call continues the same queue snapshot.
  */
object Page:
  opaque type Type = Int

  val MaximumItems: Int = 1000
  val default: Type = 100

  def from(maximumItems: Int): Either[ValidationFailure, Type] =
    Either.cond(
      maximumItems >= 1 && maximumItems <= MaximumItems,
      maximumItems,
      ValidationFailure("page", s"must be between 1 and $MaximumItems items")
    )

  extension (page: Type) def maximumItems: Int = page

  given CanEqual[Type, Type] = CanEqual.derived
type Page = Page.Type

/** One compact queue row.
  *
  * Command evidence and freshness belong to the enclosing page so the same captured `squeue`
  * response is not repeated once per job on the wire.
  */
final case class QueueJob(
    job: JobRef,
    name: Option[JobName],
    user: Option[UserName],
    partition: Option[PartitionName],
    state: SlurmState,
    flags: Vector[SlurmStateFlag],
    reason: Option[String],
    timing: JobTiming,
    reportedCluster: Option[ClusterName],
    stateExpressionCompleteness: StateExpressionCompleteness
) derives CanEqual

enum QueuePageCompleteness derives CanEqual:
  case Complete
  case Truncated(totalMatched: Int)

final case class QueuePage(
    jobs: Vector[QueueJob],
    completeness: QueuePageCompleteness,
    freshness: Freshness,
    evidence: EvidenceBundle
) derives CanEqual

object QueuePage:
  def from(
      jobs: Vector[QueueJob],
      page: Page,
      freshness: Freshness,
      evidence: EvidenceBundle
  ): QueuePage =
    val ordered = jobs.sortBy { job =>
      (
        job.reportedCluster.map(_.value).getOrElse(""),
        job.job.jobId.value,
        job.job.arrayIndex.map(_.value).getOrElse(-1)
      )
    }
    val retained = ordered.take(page.maximumItems)
    val completeness =
      if retained.size == ordered.size then QueuePageCompleteness.Complete
      else QueuePageCompleteness.Truncated(ordered.size)
    QueuePage(retained, completeness, freshness, evidence)

/** Slurm queue discovery is separate from lifecycle control over known job references.
  *
  * A scheduler implementation is not required to enumerate a queue. Local CLI and negotiated
  * remote-agent assemblies expose this additional capability where `squeue` listing is available.
  */
trait QueueReader[F[_]]:
  def listJobs(query: QueueQuery, page: Page): F[SchedulerQueryResult[QueuePage]]
