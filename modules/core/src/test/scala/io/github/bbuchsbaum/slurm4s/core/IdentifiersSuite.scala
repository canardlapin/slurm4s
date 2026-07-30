package io.github.bbuchsbaum.slurm4s.core

import cats.Order
import cats.Show

class IdentifiersSuite extends munit.FunSuite:
  test("opaque identifiers reject empty, whitespace, and invalid numeric values") {
    assert(SubmissionKey.from("").isLeft)
    assert(JobId.from("123 4").isLeft)
    assert(AttemptEpoch.from(0L).isLeft)
    assert(EventCursor.from(-1L).isLeft)
    assert(ByteLimit.from(0).isLeft)
  }

  test("different identifier kinds retain their validated values") {
    val submission = SubmissionKey.from("submit-01").toOption.get
    val job = JobId.from("12345_7").toOption.get

    assertEquals(submission.value, "submit-01")
    assertEquals(job.value, "12345_7")
  }

  test("identifier companions provide ordering, equality, show, and controlled constants") {
    val earlier = SubmissionKey.unsafeFrom("submit-01")
    val later = SubmissionKey.unsafeFrom("submit-02")

    assert(Order[SubmissionKey].lt(earlier, later))
    assert(Order[SubmissionKey].eqv(earlier, earlier))
    assertEquals(Show[SubmissionKey].show(later), "submit-02")
    intercept[IllegalArgumentException](SubmissionKey.unsafeFrom("not valid"))
  }

  test("bounded evidence reports truncation without losing original size") {
    val limit = ByteLimit.from(3).toOption.get
    val evidence = BoundedEvidence.capture(
      EvidenceSource.CommandStdout("squeue"),
      java.time.Instant.EPOCH,
      Vector[Byte](1, 2, 3, 4, 5),
      limit
    )

    assertEquals(evidence.bytes, Vector[Byte](1, 2, 3))
    assertEquals(evidence.originalByteCount, 5L)
    assert(evidence.truncated)
  }
