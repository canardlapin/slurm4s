package io.github.bbuchsbaum.slurm4s.core

import scodec.bits.ByteVector

class FailureModelSuite extends munit.FunSuite:
  test("scheduler rejection and unknown acceptance are distinct values") {
    val captured = BoundedEvidence.capture(
      EvidenceSource.CommandStderr("sbatch"),
      java.time.Instant.EPOCH,
      ByteVector.empty
    )
    val evidence = EvidenceBundle(captured)
    val diagnostics = Diagnostics.one(Diagnostic("invalid-account", "account is required"))
    val rejection = Submission.Rejected(diagnostics, evidence)
    val unknown = Submission.AcceptanceUnknown(AcceptanceUncertainty.ResponseLost, evidence)

    assert(rejection.isInstanceOf[Submission.Rejected])
    assert(unknown.isInstanceOf[Submission.AcceptanceUnknown])
  }

  test("diagnostic collections cannot be empty") {
    assert(Diagnostics.fromVector(Vector.empty).isLeft)
    assert(
      Diagnostics.fromVector(Vector(Diagnostic("invalid-account", "account is required"))).isRight
    )
  }
