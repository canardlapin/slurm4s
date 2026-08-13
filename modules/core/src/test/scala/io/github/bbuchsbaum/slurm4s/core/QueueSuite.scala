package io.github.bbuchsbaum.slurm4s.core

import scodec.bits.ByteVector

import java.time.Instant

class QueueSuite extends munit.FunSuite:
  test("page is a positive item ceiling with a public maximum") {
    assert(Page.from(0).isLeft)
    assertEquals(Page.from(1).map(_.maximumItems), Right(1))
    assertEquals(Page.from(Page.MaximumItems).map(_.maximumItems), Right(Page.MaximumItems))
    assert(Page.from(Page.MaximumItems + 1).isLeft)
  }

  test("current-user queue filters are bounded, unique, and canonical") {
    val alpha = JobName.unsafeFrom("alpha")
    val beta = JobName.unsafeFrom("beta")
    val query = QueueQuery
      .currentUser(
        Vector(beta, alpha),
        Vector(PartitionName.unsafeFrom("gpu"), PartitionName.unsafeFrom("cpu")),
        Vector(QueueStateFilter.Running, QueueStateFilter.Pending)
      )
      .toOption
      .get

    assertEquals(query.names.map(_.value), Vector("alpha", "beta"))
    assertEquals(query.partitions.map(_.value), Vector("cpu", "gpu"))
    assertEquals(query.states, Vector(QueueStateFilter.Pending, QueueStateFilter.Running))
    assert(QueueQuery.currentUser(Vector(alpha, alpha), Vector.empty, Vector.empty).isLeft)
  }

  test("a page sorts one captured result and reports omitted matches") {
    val page = QueuePage.from(
      Vector(job("20", Some(2)), job("10", None), job("20", Some(1))),
      Page.from(2).toOption.get,
      Freshness.Current(observedAt),
      evidence
    )

    assertEquals(
      page.jobs.map(value => value.job.jobId.value -> value.job.arrayIndex.map(_.value)),
      Vector("10" -> None, "20" -> Some(1))
    )
    assertEquals(page.completeness, QueuePageCompleteness.Truncated(3))
    assertEquals(page.evidence, evidence)
  }

  private val observedAt = Instant.parse("2026-08-13T12:00:00Z")
  private val evidence = EvidenceBundle(
    BoundedEvidence.capture(EvidenceSource.CommandStdout("squeue"), observedAt, ByteVector.empty)
  )

  private def job(id: String, arrayIndex: Option[Int]): QueueJob =
    QueueJob(
      JobRef(JobId.unsafeFrom(id), arrayIndex.map(ArrayIndex.unsafeFrom)),
      None,
      None,
      None,
      SlurmState.Running,
      Vector.empty,
      None,
      JobTiming.unknown,
      None,
      StateExpressionCompleteness.Complete
    )
