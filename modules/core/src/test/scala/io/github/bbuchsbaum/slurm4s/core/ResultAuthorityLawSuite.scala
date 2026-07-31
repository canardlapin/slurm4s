package io.github.bbuchsbaum.slurm4s.core

import io.github.bbuchsbaum.remoteexec.kernel.ContentDigest
import org.scalacheck.Gen
import org.scalacheck.Prop.forAll

import java.time.Instant

/** P8.B2 law family 4: result authority.
  *
  * ADR 0001 names the validated envelope as the authority for a typed result, and explicitly denies
  * that authority to "exit zero, stdout, or the Scala type expected by a caller". These properties
  * hold the envelope's own constructors to that: a failed envelope carries no value to mistake for
  * a result, and a succeeded one always carries the value it claims.
  */
class ResultAuthorityLawSuite extends munit.ScalaCheckSuite:
  import Generators.*

  private val schema: ResultSchemaId = ResultSchemaId.unsafeFrom("law.result.v1")
  private val release: WorkerRelease =
    WorkerRelease(
      WorkerReleaseId.unsafeFrom("law-worker"),
      ContentDigest.unsafeFrom("sha256:" + "ab" * 32)
    )
  private val operation: WorkloadOperation =
    WorkloadOperation.Registered(
      OperationId.unsafeFrom("law.operation"),
      OperationVersion.unsafeFrom("1")
    )

  private val bytes: Gen[Vector[Byte]] =
    Gen.listOf(Gen.choose(Byte.MinValue, Byte.MaxValue)).map(_.toVector)

  private val failureCode: Gen[String] = Gen.oneOf("outputs", "codec", "worker", "timeout")

  private def key(index: Int): SubmissionKey = SubmissionKey.unsafeFrom(s"law-$index")
  private def attempt(index: Int): AttemptId = AttemptId.unsafeFrom(s"law-attempt-$index")

  property("a failed envelope never carries a value that could be read as a result") {
    forAll(Gen.choose(0, 1000), failureCode, Gen.alphaStr, Gen.option(jobRef)) {
      (index, code, message, job) =>
        val envelope = ResultEnvelope.failed(
          key(index),
          attempt(index),
          AttemptEpoch.initial,
          job,
          operation,
          schema,
          code,
          message,
          OutputManifest.empty,
          release,
          Instant.EPOCH
        )
        envelope.value.isEmpty && envelope.status.isInstanceOf[ResultEnvelopeStatus.Failed]
    }
  }

  property("a succeeded envelope always carries exactly the value it was given") {
    forAll(Gen.choose(0, 1000), bytes, Gen.option(jobRef)) { (index, value, job) =>
      val envelope = ResultEnvelope.succeeded(
        key(index),
        attempt(index),
        AttemptEpoch.initial,
        job,
        operation,
        schema,
        value,
        OutputManifest.empty,
        release,
        Instant.EPOCH
      )
      envelope.value.contains(value) && envelope.status == ResultEnvelopeStatus.Succeeded
    }
  }

  property("an envelope is bound to the attempt and epoch that produced it") {
    forAll(Gen.choose(0, 1000), Gen.choose(1L, 10000L), bytes) { (index, epoch, value) =>
      val bound = AttemptEpoch.unsafeFrom(epoch)
      val envelope = ResultEnvelope.succeeded(
        key(index),
        attempt(index),
        bound,
        None,
        operation,
        schema,
        value,
        OutputManifest.empty,
        release,
        Instant.EPOCH
      )
      envelope.attemptEpoch == bound && envelope.attemptId == attempt(index)
    }
  }

  property("an output manifest never admits one path twice") {
    forAll(Gen.listOf(Gen.oneOf("a.txt", "b.txt", "c/d.txt"))) { paths =>
      val entries = paths.map { raw =>
        OutputEntry
          .from(
            RelativeOutputPath.unsafeFrom(raw),
            1L,
            ContentDigest.unsafeFrom("sha256:" + "cd" * 32)
          )
          .toOption
          .get
      }.toVector
      val manifest = OutputManifest.from(entries)
      manifest.isRight == (paths.distinct.size == paths.size)
    }
  }
