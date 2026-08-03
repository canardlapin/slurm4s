package io.github.bbuchsbaum.slurm4s.core

import org.scalacheck.Arbitrary
import org.scalacheck.Gen

import scodec.bits.ByteVector

import java.time.Instant

/** P8.B2: the shared generator surface.
  *
  * ScalaCheck has been a declared dependency of every module since the build was written and had
  * never been used. These generators exist so law families state properties over the real domain
  * rather than over a handful of hand-picked values.
  */
object Generators:

  val jobId: Gen[JobId] =
    Gen.choose(1L, 9999999L).map(value => JobId.unsafeFrom(value.toString))

  val clusterName: Gen[ClusterName] =
    Gen.oneOf("alpha", "beta", "gamma", "delta").map(ClusterName.unsafeFrom)

  val arrayIndex: Gen[ArrayIndex] =
    Gen.choose(0, 4096).map(ArrayIndex.unsafeFrom)

  val jobRef: Gen[JobRef] =
    for
      id <- jobId
      index <- Gen.option(arrayIndex)
    yield JobRef(id, index)

  /** Every base state plus an unknown one, so exhaustive-match assumptions are exercised. */
  val slurmState: Gen[SlurmState] =
    Gen.oneOf(
      Gen.const(SlurmState.Pending),
      Gen.const(SlurmState.Running),
      Gen.const(SlurmState.Completed),
      Gen.const(SlurmState.Failed),
      Gen.const(SlurmState.Cancelled),
      Gen.const(SlurmState.OutOfMemory),
      Gen.const(SlurmState.TimedOut),
      Gen.const(SlurmState.NodeFailure),
      Gen.const(SlurmState.Preempted),
      Gen.const(SlurmState.BootFail),
      Gen.const(SlurmState.Deadline),
      Gen.const(SlurmState.Suspended),
      Gen.alphaUpperStr.suchThat(_.nonEmpty).map(SlurmState.Unknown.apply)
    )

  /** Textual state expressions as the CLI surfaces actually emit them. */
  val stateExpression: Gen[String] =
    val base = Gen.oneOf(
      "PENDING",
      "RUNNING",
      "COMPLETED",
      "FAILED",
      "CANCELLED",
      "BOOT_FAIL",
      "DEADLINE",
      "SUSPENDED",
      "TIMEOUT",
      "NODE_FAIL",
      "FUTURE_STATE"
    )
    val flag = Gen.oneOf("POWER_UP_NODE", "CONFIGURING", "RESIZING", "REVOKED", "FUTURE_FLAG")
    for
      head <- base
      flags <- Gen.listOfN(2, flag).flatMap(values => Gen.choose(0, 2).map(values.take))
      truncated <- Gen.prob(0.25)
    yield (head +: flags).mkString("+") + (if truncated then "+" else "")

  val instant: Gen[Instant] =
    Gen.choose(0L, 4_000_000_000L).map(Instant.ofEpochSecond)

  val evidenceBundle: Gen[EvidenceBundle] =
    for
      at <- instant
      bytes <- Gen.listOf(Gen.choose(Byte.MinValue, Byte.MaxValue)).map(v => ByteVector(v*))
    yield EvidenceBundle(BoundedEvidence.capture(EvidenceSource.DurableJournal, at, bytes))

  val acceptanceUncertainty: Gen[AcceptanceUncertainty] =
    Gen.oneOf(
      AcceptanceUncertainty.ResponseLost,
      AcceptanceUncertainty.TransportInterrupted,
      AcceptanceUncertainty.ResponseUnparseable,
      AcceptanceUncertainty.PersistenceInterrupted,
      AcceptanceUncertainty.Unclassified
    )

  given Arbitrary[JobRef] = Arbitrary(jobRef)
  given Arbitrary[SlurmState] = Arbitrary(slurmState)
  given Arbitrary[EvidenceBundle] = Arbitrary(evidenceBundle)
  given Arbitrary[AcceptanceUncertainty] = Arbitrary(acceptanceUncertainty)
