package io.github.bbuchsbaum.slurm4s.protocol

import io.github.bbuchsbaum.slurm4s.core.*

import org.scalacheck.Gen

/** Proves the round-trip laws can actually see a dropped field (P6f.34).
  *
  * A passing round-trip law is weak evidence on its own. If a generator leaves a defaulted field at
  * its default, the law still passes against a decoder that never reads that field — which is
  * exactly how `JobObservation.reportedCluster` survived review (P8.A7): the field defaults to
  * `None`, so the decoder omitted it, still typechecked under `-Werror`, and still round-tripped
  * every value anyone had thought to write down.
  *
  * Each case here models the observable effect of that defect as a `collapse` function: what a
  * lossy codec returns when it forgets a field and lets the default stand. Asserting that the
  * generator produces values the collapse changes is asserting that `CodecRoundTripLawSuite` would
  * fail against such a codec. Production codecs are never modified to check this.
  *
  * This suite is the reason the round-trip laws mean something, so it fails loudly if a generator
  * is ever weakened back to always-default.
  */
class CodecLawStrengthSuite extends munit.FunSuite:
  import ProtocolGenerators.*

  private val Samples = 200

  /** Fails when no sampled value distinguishes itself from its collapsed form, which is the state
    * in which the corresponding round-trip law has gone blind.
    */
  private def detects[A](field: String, generator: Gen[A], collapse: A => A)(using
      CanEqual[A, A],
      munit.Location
  ): Unit =
    val samples = Vector.fill(Samples)(generator.sample).flatten
    assert(samples.sizeIs > 0, s"generator for $field produced no values at all")
    assert(
      samples.exists(value => collapse(value) != value),
      s"no generated value populates $field, so a codec that drops it would still satisfy the " +
        s"round-trip law; strengthen the generator rather than relaxing this assertion"
    )

  test("the observation law would catch a decoder that drops reportedCluster, the P8.A7 defect") {
    detects[JobObservation](
      "JobObservation.reportedCluster",
      jobObservation,
      _.copy(reportedCluster = None)
    )
  }

  test("the observation law would catch a decoder that drops the state flags") {
    detects[JobObservation]("JobObservation.flags", jobObservation, _.copy(flags = Vector.empty))
  }

  test("the observation law would catch a decoder that drops state-expression truncation") {
    detects[JobObservation](
      "JobObservation.stateExpressionCompleteness",
      jobObservation,
      _.copy(stateExpressionCompleteness = StateExpressionCompleteness.Unreported)
    )
  }

  test("the observation law would catch a decoder that drops the timing block") {
    detects[JobObservation](
      "JobObservation.timing",
      jobObservation,
      _.copy(timing = JobTiming.unknown)
    )
  }

  test("the evidence law would catch a codec that carries only the primary capture") {
    detects[EvidenceBundle](
      "EvidenceBundle.related",
      evidenceBundle,
      _.copy(related = Vector.empty)
    )
  }

  test("the accounting law would catch a codec that drops the missing-job list") {
    detects[AccountingBatch](
      "AccountingBatch.missing",
      accountingBatch,
      _.copy(missing = Vector.empty)
    )
  }

  test("the accounting law distinguishes disclosed and undisclosed completed exits") {
    val samples = org.scalacheck.Gen.listOfN(Samples, workloadOutcome).sample.toVector.flatten
    assert(
      samples.exists {
        case WorkloadOutcome.Completed(CompletionExitStatus.Undisclosed) => true
        case _                                                           => false
      },
      "the generator never produced an undisclosed completed exit"
    )
    assert(
      samples.exists {
        case WorkloadOutcome.Completed(CompletionExitStatus.ReportedZero) => true
        case _                                                            => false
      },
      "the generator never produced a disclosed completed exit"
    )
  }

  test("the diagnostic fields survive generation, so a codec dropping them is visible") {
    detects[Diagnostic]("Diagnostic.fields", diagnostic, _.copy(fields = Map.empty))
  }

  test("the launch-spec law reaches termination notices instead of only the absent default") {
    val samples = Vector.fill(Samples)(exitOnlyLaunchSpec.sample).flatten
    assert(
      samples.exists(_.terminationNotice.nonEmpty),
      "no generated launch spec requested notice"
    )
    assert(samples.exists(_.terminationNotice.isEmpty), "no generated launch spec omitted notice")
  }

  /** `retrySafety` defaults on `DurableResultHandle.from`, which is the same shape of hazard as
    * reportedCluster: a persisted codec could omit it and still construct a valid handle.
    *
    * These types have private constructors, so the collapse rebuilds through the public factory
    * rather than copying — which is also how a decoder would have to build them, and therefore how
    * a decoder would drop the field.
    */
  test("the handle law would catch a persisted codec that drops retrySafety") {
    detects[DurableResultHandle](
      "DurableResultHandle.retrySafety",
      durableResultHandle,
      handle =>
        DurableResultHandle
          .from(
            handle.submissionKey,
            handle.attemptId,
            handle.attemptEpoch,
            handle.job,
            handle.operation,
            handle.resultSchema,
            handle.maximumResultBytes,
            handle.maximumEnvelopeBytes,
            handle.declaredOutputs,
            handle.workerRelease
          )
          .toOption
          .get
    )
  }

  test("evidence bytes set the high bit, so a codec that assumes text cannot pass by luck") {
    val samples = Vector.fill(Samples)(Generators.evidenceBytes.sample).flatten
    assert(
      // Negative as a signed Byte means the high bit is set: not ASCII, and not valid UTF-8 on its
      // own. A codec routing bytes through a string would corrupt these and must fail the law.
      samples.exists(_.toArray.exists(_ < 0)),
      "generated evidence bytes never leave the ASCII range"
    )
  }

  test("the strength check is not vacuous: an always-default generator is reported as blind") {
    val alwaysDefault = jobObservation.map(_.copy(reportedCluster = None))
    val failure = intercept[AssertionError] {
      detects[JobObservation](
        "JobObservation.reportedCluster",
        alwaysDefault,
        _.copy(reportedCluster = None)
      )
    }
    assert(
      failure.getMessage.contains("no generated value populates"),
      s"expected the blindness message, got: ${failure.getMessage}"
    )
  }
