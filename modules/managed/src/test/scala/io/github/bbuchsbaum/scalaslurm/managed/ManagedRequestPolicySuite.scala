package io.github.bbuchsbaum.scalaslurm.managed

import cats.data.NonEmptyVector
import cats.effect.IO
import io.github.bbuchsbaum.scalaslurm.core.*

class ManagedRequestPolicySuite extends munit.CatsEffectSuite:
  import ManagedTestSupport.*

  test("default managed admission rejects environment values before durable state exists") {
    val secret = "must-never-appear"
    val requested = request("secret-default").copy(
      environment = Map("AWS_SECRET_ACCESS_KEY" -> secret)
    )

    for
      store <- InMemoryControlStore.create[IO]()
      controller = ManagedController[IO](store, inertScheduler)
      result <- controller.submit(requested)
      snapshot <- store.snapshot
    yield
      result match
        case ManagedSubmitResult.Failed(ControlFailure.RequestRejected(diagnostics)) =>
          assertEquals(
            diagnostics.toVector.map(_.code),
            Vector("durable-environment-values-forbidden")
          )
          assert(!diagnostics.toString.contains(secret))
        case other => fail(s"expected request rejection, received $other")
      assertEquals(snapshot, ControlState.empty)
  }

  test("explicit public policy admits only its validated non-secret names") {
    val policy = ManagedRequestPolicy
      .publicEnvironment(Set("LANG", "LC_ALL"))
      .fold(problem => fail(problem.toString), identity)
    val requested = request("public-environment").copy(
      environment = Map("LANG" -> "C.UTF-8")
    )

    for
      store <- InMemoryControlStore.create[IO]()
      controller = ManagedController[IO](store, inertScheduler, policy)
      result <- controller.submit(requested)
      snapshot <- store.snapshot
    yield
      assert(result.isInstanceOf[ManagedSubmitResult.Created])
      val persisted = snapshot.attempts.values.head.intent.request.decode
      assertEquals(persisted.map(_.environment), Right(Map("LANG" -> "C.UTF-8")))
  }

  test("public policy rejects undeclared values without exposing their content") {
    val policy = ManagedRequestPolicy
      .publicEnvironment(Set("LANG"))
      .fold(problem => fail(problem.toString), identity)
    val secret = "private-token-value"
    val requested = request("undeclared-environment").copy(
      environment = Map("TOKEN" -> secret)
    )

    for
      store <- InMemoryControlStore.create[IO]()
      result <- ManagedController[IO](store, inertScheduler, policy).submit(requested)
      snapshot <- store.snapshot
    yield
      result match
        case ManagedSubmitResult.Failed(ControlFailure.RequestRejected(diagnostics)) =>
          assert(diagnostics.toVector.exists(_.code == "durable-environment-name-not-public"))
          assert(!diagnostics.toString.contains(secret))
        case other => fail(s"expected request rejection, received $other")
      assertEquals(snapshot, ControlState.empty)
  }

  test("canonical replay validates admitted bytes without current admission policy") {
    val policy = ManagedRequestPolicy
      .publicEnvironment(Set("LANG"))
      .fold(problem => fail(problem.toString), identity)
    val requested = request("public-replay").copy(environment = Map("LANG" -> "C"))
    val canonical = CanonicalRequest
      .from(requested, policy)
      .fold(problem => fail(problem.toString), identity)

    assertEquals(
      CanonicalRequest.validated(canonical.bytes, canonical.digest),
      Right(canonical)
    )
  }

  test("public policy construction rejects invalid process-variable names") {
    assert(ManagedRequestPolicy.publicEnvironment(Set("NOT VALID")).isLeft)
    assert(ManagedRequestPolicy.publicEnvironment(Set("VALID_NAME")).isRight)
  }

  private val inertScheduler: Scheduler[IO] = new Scheduler[IO]:
    def capabilities: IO[SchedulerQueryResult[SchedulerCapabilities]] =
      unexpected("capabilities")
    def submit[A](request: JobRequest[A]): IO[SubmissionAttempt] =
      unexpected(s"submit $request")
    def observe(
        jobs: NonEmptyVector[JobRef]
    ): IO[SchedulerQueryResult[ObservationBatch]] =
      unexpected(s"observe $jobs")
    def accounting(
        jobs: NonEmptyVector[JobRef]
    ): IO[SchedulerQueryResult[AccountingBatch]] =
      unexpected(s"accounting $jobs")
    def cancel(job: JobRef): IO[CancellationAttempt] =
      unexpected(s"cancel $job")

  private def unexpected[A](operation: String): IO[A] =
    IO.raiseError(new AssertionError(s"scheduler must not run during intent admission: $operation"))
