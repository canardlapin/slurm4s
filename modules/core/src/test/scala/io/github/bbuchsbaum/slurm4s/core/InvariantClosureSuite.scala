package io.github.bbuchsbaum.slurm4s.core

import io.github.bbuchsbaum.remoteexec.kernel.ContentDigest

/** P8.A5: invariants that were stated in documentation but not enforced by construction. */
class InvariantClosureSuite extends munit.FunSuite:

  test("a relative output path cannot spell one file two ways") {
    assert(
      RelativeOutputPath.from("results/./out.txt").isLeft,
      "'.' segments let a/./b and a/b be distinct values naming the same file"
    )
    assert(RelativeOutputPath.from("results/../out.txt").isLeft)
    assert(RelativeOutputPath.from("/results/out.txt").isLeft)
    assert(RelativeOutputPath.from("results/out.txt").isRight)
  }

  test("a content digest must be structurally a sha256 digest") {
    assert(ContentDigest.from("x").isLeft, "an arbitrary label is not content identity")
    assert(ContentDigest.from("sha256:worker").isLeft, "a plausible label is still not a digest")
    assert(ContentDigest.from("sha256:" + "a" * 63).isLeft, "63 hex characters is not sha256")
    assert(ContentDigest.from("sha256:" + "A" * 64).isLeft, "uppercase hex is not canonical")
    assert(ContentDigest.from("md5:" + "a" * 32).isLeft)
    assert(ContentDigest.from("sha256:" + "0f" * 32).isRight)
  }

  test("event cursors and store revisions refuse to wrap past their own invariant") {
    val cursor = EventCursor.unsafeFrom(Long.MaxValue)
    assert(cursor.next.isLeft, "advancing past Long.MaxValue would wrap to a negative cursor")
    assertEquals(EventCursor.origin.next.map(_.value), Right(1L))
  }
