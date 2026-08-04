package io.github.bbuchsbaum.slurm4s.core

import io.github.bbuchsbaum.remoteexec.kernel.ContentDigest
import org.scalacheck.Gen
import org.scalacheck.Prop.forAll

/** P8.D2: one persisted result plan.
  *
  * `DurableResultHandle` used to be constructed beside the plan that reads the result, in two
  * places, from overlapping facts. Nothing prevented the handle from advertising one schema, limit
  * or output set while the plan used another. Deriving it removes the opportunity.
  */
class PreparedAttemptSuite extends munit.ScalaCheckSuite:
  import Generators.*

  private val schema = ResultSchemaId.unsafeFrom("plan.result.v1")
  private val release =
    WorkerRelease(
      WorkerReleaseId.unsafeFrom("plan-worker"),
      ContentDigest.unsafeFrom("sha256:" + "ef" * 32)
    )

  private def plan(
      outputs: Vector[String],
      resultBytes: Int,
      envelopeBytes: Int
  ): ResultPlan[NoResult] =
    ResultPlan(
      schema,
      ResultContract.ExitOnly,
      ByteLimit.from(resultBytes).toOption.get,
      ByteLimit.from(envelopeBytes).toOption.get,
      outputs.map(RelativeOutputPath.unsafeFrom)
    )

  private def attempt(
      resultPlan: ResultPlan[NoResult],
      key: String,
      retrySafety: RetrySafety
  ): PreparedAttempt[NoResult] =
    PreparedAttempt(
      PreparedJob(
        LaunchSpec(
          SubmissionKey.unsafeFrom(key),
          JobName.unsafeFrom("plan-job"),
          ScriptSource.ExistingRemote("/work/job.sh"),
          Vector.empty,
          ResultContract.ExitOnly.descriptor,
          ResourceRequest.validate(1, 1, None, None, None).toEither.toOption.get,
          retrySafety = retrySafety
        ),
        resultPlan,
        ContentDigest.unsafeFrom("sha256:" + "12" * 32)
      ),
      AttemptId.unsafeFrom(s"$key-attempt"),
      AttemptEpoch.initial,
      WorkloadOperation.Registered(
        OperationId.unsafeFrom("plan.operation"),
        OperationVersion.unsafeFrom("1")
      ),
      release
    )

  property("the durable handle always agrees with the plan it projects") {
    val outputs = Gen.listOf(Gen.oneOf("a.txt", "b.txt", "c/d.txt")).map(_.distinct.toVector)
    forAll(outputs, Gen.choose(1, 1_000_000), Gen.choose(1, 1_000_000), Gen.option(jobRef)) {
      (paths, resultBytes, envelopeBytes, bound) =>
        val resultPlan = plan(paths, resultBytes, envelopeBytes)
        val handle = attempt(resultPlan, "agree", RetrySafety.Unknown).durableHandle(bound)

        handle.resultSchema == resultPlan.schema &&
        handle.maximumResultBytes == resultPlan.maximumResultBytes &&
        handle.maximumEnvelopeBytes == resultPlan.maximumEnvelopeBytes &&
        handle.declaredOutputs == resultPlan.declaredOutputs &&
        handle.job == bound
    }
  }

  property("the durable handle carries the attempt identity that fences it") {
    forAll(Gen.choose(1L, 5000L)) { epoch =>
      val bound = AttemptEpoch.unsafeFrom(epoch)
      val prepared = attempt(plan(Vector.empty, 1024, 2048), "fence", RetrySafety.Unknown)
      val handle = prepared.copy(epoch = bound).durableHandle(None)

      handle.attemptEpoch == bound && handle.attemptId == prepared.attemptId
    }
  }

  test("retry safety comes from the launch, not from a second source that could disagree") {
    val safe = attempt(
      plan(Vector.empty, 1024, 2048),
      "retry",
      RetrySafety.SafeForAutomaticRetry
    ).durableHandle(None)

    assertEquals(safe.retrySafety, RetrySafety.SafeForAutomaticRetry)
  }

  test("a prepared job carries no attempt identity, so a retry reuses it unchanged") {
    val first = attempt(plan(Vector.empty, 1024, 2048), "reuse", RetrySafety.Unknown)
    val retried = first.copy(
      attemptId = AttemptId.unsafeFrom("reuse-attempt-2"),
      epoch = AttemptEpoch.unsafeFrom(2L)
    )

    assertEquals(retried.job, first.job)
    assertEquals(retried.job.digest, first.job.digest)
  }
