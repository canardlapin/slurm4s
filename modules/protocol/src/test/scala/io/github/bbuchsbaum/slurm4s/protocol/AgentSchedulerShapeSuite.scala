package io.github.bbuchsbaum.slurm4s.protocol

import io.circe.Json
import io.github.bbuchsbaum.slurm4s.core.*

import java.time.Instant

/** Byte-exact coverage of the scheduler wire shapes that are still derived rather than owned.
  *
  * `AgentDomainJson` derives the encoders for `Submission`, `SubmissionAttempt`,
  * `InvocationResult`, `CancellationResult`, `CancellationAttempt`, `AcceptanceUncertainty`,
  * `SpawnFailureKind`, `CapabilitySupport`, `SchedulerCapabilities`, and `SchedulerQueryResult`.
  * Derivation puts Scala case names on the wire, so renaming a case is a protocol change. Nothing
  * else in the tree observes that: `sbt test`, `mimaReportBinaryIssues`, and `scalafmtCheckAll` all
  * pass through a rename unchanged.
  *
  * This fixture closes that window until the codecs are explicitly owned (P6f.12). Every case of
  * every listed type appears exactly once, so any rename, reshaping, or field change fails here
  * with a byte diff instead of reaching a deployed agent.
  */
class AgentSchedulerShapeSuite extends munit.FunSuite:
  private val observedAt = Instant.parse("2026-07-25T12:00:00Z")

  test("every derived scheduler wire shape matches the golden fixture") {
    // Regenerating is a protocol decision, never a way to make a failing build pass. Run
    // `sbt -Dslurm4s.regenerateShapeFixture=true protocol/test` only alongside a deliberate,
    // reviewed wire change, and check the resulting diff into the compatibility record.
    if sys.props.get("slurm4s.regenerateShapeFixture").contains("true") then
      java.nio.file.Files.writeString(
        java.nio.file.Path
          .of("modules/protocol/src/test/resources/fixtures/agent-scheduler-shapes-v1.json"),
        shapes.spaces2 + "\n"
      )
      fail("regenerated the shape fixture; re-run without the flag to verify")

    val expected = io.circe.parser
      .parse(resource("/fixtures/agent-scheduler-shapes-v1.json"))
      .fold(problem => fail(s"unreadable fixture: $problem"), identity)

    assertEquals(shapes.spaces2, expected.spaces2)
  }

  test("every derived scheduler wire shape survives a decode round trip") {
    submissions.foreach { case (label, value) =>
      assertEquals(
        AgentDomainJson.decodeSubmission(AgentDomainJson.encodeSubmission(value)),
        Right(value),
        s"submission round trip failed for $label"
      )
    }
    cancellations.foreach { case (label, value) =>
      assertEquals(
        AgentDomainJson.decodeCancellation(AgentDomainJson.encodeCancellation(value)),
        Right(value),
        s"cancellation round trip failed for $label"
      )
    }
    capabilities.foreach { case (label, value) =>
      assertEquals(
        AgentDomainJson.decodeCapabilities(AgentDomainJson.encodeCapabilities(value)),
        Right(value),
        s"capabilities round trip failed for $label"
      )
    }
  }

  private def shapes: Json =
    Json.obj(
      "submissions" -> Json.obj(
        submissions.map { case (label, value) =>
          label -> AgentDomainJson.encodeSubmission(value)
        }*
      ),
      "cancellations" -> Json.obj(
        cancellations.map { case (label, value) =>
          label -> AgentDomainJson.encodeCancellation(value)
        }*
      ),
      "capabilities" -> Json.obj(
        capabilities.map { case (label, value) =>
          label -> AgentDomainJson.encodeCapabilities(value)
        }*
      )
    )

  /** Covers Submission, SubmissionAttempt, AcceptanceUncertainty, InvocationResult, and
    * SpawnFailureKind.
    */
  private def submissions: Vector[(String, SubmissionAttempt)] =
    val uncertainties = Vector(
      "response-lost" -> AcceptanceUncertainty.ResponseLost,
      "transport-interrupted" -> AcceptanceUncertainty.TransportInterrupted,
      "response-unparseable" -> AcceptanceUncertainty.ResponseUnparseable,
      "persistence-interrupted" -> AcceptanceUncertainty.PersistenceInterrupted,
      "unclassified" -> AcceptanceUncertainty.Unclassified
    )
    val spawnKinds = Vector(
      "executable-missing" -> SpawnFailureKind.ExecutableMissing,
      "permission-denied" -> SpawnFailureKind.PermissionDenied,
      "working-directory-missing" -> SpawnFailureKind.WorkingDirectoryMissing,
      "environment-invalid" -> SpawnFailureKind.EnvironmentInvalid,
      "resource-unavailable" -> SpawnFailureKind.ResourceUnavailable,
      "spawn-unknown" -> SpawnFailureKind.Unknown
    )
    Vector(
      "accepted" -> SubmissionAttempt.Completed(Submission.Accepted(job, bundle)),
      "rejected" -> SubmissionAttempt.Completed(Submission.Rejected(diagnostics, bundle)),
      "preparation-failed" -> SubmissionAttempt.PreparationFailed(diagnostics),
      "invocation-exited" -> SubmissionAttempt.InvocationFailed(
        InvocationResult.Exited(3, evidence, evidence)
      ),
      "invocation-timed-out" -> SubmissionAttempt.InvocationFailed(
        InvocationResult.TimedOut(DurationMillis.unsafeFrom(1500L), evidence, evidence)
      )
    ) ++ uncertainties.map { case (label, reason) =>
      s"acceptance-unknown-$label" -> SubmissionAttempt.Completed(
        Submission.AcceptanceUnknown(reason, bundle)
      )
    } ++ spawnKinds.map { case (label, kind) =>
      s"invocation-spawn-$label" -> SubmissionAttempt.InvocationFailed(
        InvocationResult.SpawnFailed(kind, diagnostics, bundle)
      )
    }

  /** Covers CancellationResult and CancellationAttempt. */
  private def cancellations: Vector[(String, CancellationAttempt)] =
    Vector(
      "acknowledged" -> CancellationAttempt.Completed(CancellationResult.Acknowledged(bundle)),
      "not-found" -> CancellationAttempt.Completed(CancellationResult.NotFound(bundle)),
      "rejected" -> CancellationAttempt.Completed(
        CancellationResult.Rejected(diagnostics, bundle)
      ),
      "unknown" -> CancellationAttempt.Completed(
        CancellationResult.Unknown(diagnostics, bundle)
      ),
      "invocation-failed" -> CancellationAttempt.InvocationFailed(
        InvocationResult.Exited(1, evidence, evidence)
      )
    )

  /** Covers SchedulerQueryResult, SchedulerCapabilities, and CapabilitySupport. */
  private def capabilities: Vector[(String, SchedulerQueryResult[SchedulerCapabilities])] =
    Vector(
      "succeeded" -> SchedulerQueryResult.Succeeded(
        SchedulerCapabilities(
          slurmVersion = Some("25.05.6"),
          cluster = Some(ClusterName.from("alpha").toOption.get),
          structuredQueue = CapabilitySupport.Supported,
          structuredAccounting = CapabilitySupport.Unsupported,
          accounting = CapabilitySupport.Unknown(diagnostics),
          arrays = CapabilitySupport.Supported,
          rawEvidence = Vector(evidence)
        )
      ),
      "empty" -> SchedulerQueryResult.Empty(observedAt, bundle),
      "invocation-failed" -> SchedulerQueryResult.InvocationFailed(
        InvocationResult.Exited(2, evidence, evidence)
      ),
      "parse-failed" -> SchedulerQueryResult.ParseFailed(diagnostics, bundle)
    )

  private def job: JobRef =
    JobRef(
      JobId.from("4242").toOption.get,
      Some(ArrayIndex.from(7).toOption.get)
    )

  private def evidence: BoundedEvidence =
    BoundedEvidence.capture(
      EvidenceSource.CommandStdout("squeue"),
      observedAt,
      // Non-ASCII and control bytes must survive base64 without reinterpretation.
      Vector[Byte](0, 127, -128, -1)
    )

  private def bundle: EvidenceBundle = EvidenceBundle(evidence, Vector(evidence))

  private def diagnostics: Diagnostics =
    Diagnostics
      .fromVector(
        Vector(
          Diagnostic("shape-fixture", "a stable diagnostic", Map("field" -> "value")),
          Diagnostic("shape-fixture-second", "another stable diagnostic")
        )
      )
      .toOption
      .get

  private def resource(path: String): String =
    scala.io.Source
      .fromInputStream(
        Option(getClass.getResourceAsStream(path))
          .getOrElse(throw new IllegalStateException(s"missing fixture: $path"))
      )
      .mkString
