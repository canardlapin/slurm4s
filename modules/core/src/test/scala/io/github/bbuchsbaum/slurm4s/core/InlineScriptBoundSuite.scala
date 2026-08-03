package io.github.bbuchsbaum.slurm4s.core

import scodec.bits.ByteVector

/** The one bound on an inline script (P7.4).
  *
  * Three paths used to answer this differently: the remote script-program decoder bounded inline
  * bytes at four mebibytes, the opaque submit decoder did not bound them at all and inherited
  * whatever frame size a deployment configured, and the worker refused them against its own
  * configurable limit. So whether a script was acceptable depended on which path carried it.
  *
  * These tests cover the whole surface rather than a sample of it, because `ScriptSource.Inline`
  * has a private constructor: there is no way to obtain one except through the factory tested here,
  * so every decoder and planner downstream necessarily holds a value that passed this check. That
  * is why there is no separate test for the submit decoder's bound — the compiler, not a fixture,
  * is what rules out an oversize inline script arriving that way.
  */
class InlineScriptBoundSuite extends munit.FunSuite:

  private val limit: Int = ByteLimit.maximumInlineScript.value

  test("a script at the limit is accepted, and its bytes survive intact") {
    val bytes = ByteVector.fill(limit.toLong)(0x61)
    ScriptSource.inlineScript("at-limit.sh", bytes) match
      case Left(problem) =>
        fail(s"a script of exactly $limit bytes was refused: ${problem.reason}")
      case Right(ScriptSource.Inline(name, kept)) =>
        assertEquals(name, "at-limit.sh")
        assertEquals(kept.size, limit.toLong)
      case Right(other) =>
        fail(s"expected an inline source, got $other")
  }

  test("a script one byte past the limit is refused, and the failure names the field") {
    ScriptSource.inlineScript("too-big.sh", ByteVector.fill(limit.toLong + 1L)(0x61)) match
      case Right(_)      => fail(s"a script of ${limit + 1} bytes was accepted")
      case Left(problem) =>
        assertEquals(problem.field, "inlineScript")
        assert(
          problem.reason.contains(limit.toString),
          s"the refusal did not say what the limit is: ${problem.reason}"
        )
  }

  test("an empty script is accepted, so the bound is an upper bound only") {
    assert(
      ScriptSource.inlineScript("empty.sh", ByteVector.empty).isRight,
      "an empty inline script was refused"
    )
  }

  test("the unsafe constructor refuses the same scripts, by throwing") {
    val script = ScriptSource.unsafeInlineScript("fine.sh", ByteVector(1, 2, 3))
    assertEquals(script, ScriptSource.inlineScript("fine.sh", ByteVector(1, 2, 3)).toOption.get)
    intercept[IllegalArgumentException] {
      ScriptSource.unsafeInlineScript("too-big.sh", ByteVector.fill(limit.toLong + 1L)(0x61))
    }
  }
