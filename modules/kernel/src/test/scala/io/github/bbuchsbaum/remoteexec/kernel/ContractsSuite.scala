package io.github.bbuchsbaum.remoteexec.kernel

import scodec.bits.ByteVector

import java.nio.charset.StandardCharsets
import java.time.Instant

class ContractsSuite extends munit.FunSuite:
  private val utf8Codec = new InputCodec[String]:
    val schemaId: SchemaId = SchemaId.from("text/utf8-v1").toOption.get

    def encode(value: String): Either[ResultCodecFailure, ByteVector] =
      val bytes = ByteVector.view(value.getBytes(StandardCharsets.UTF_8))
      Either.cond(
        bytes.size <= 32,
        bytes,
        ResultCodecFailure("input-too-large", s"${bytes.size} bytes exceeds 32")
      )

    def decode(bytes: ByteVector): Either[ResultCodecFailure, String] =
      Either.cond(
        bytes.size <= 32,
        new String(bytes.toArray, StandardCharsets.UTF_8),
        ResultCodecFailure("input-too-large", s"${bytes.size} bytes exceeds 32")
      )

  test("validated identifiers reject null, whitespace, emptiness, and excessive length") {
    assert(SubmissionKey.from(null).isLeft)
    assert(AttemptId.from("").isLeft)
    assert(OperationId.from("contains space").isLeft)
    assert(OperationVersion.from("v" * 101).isLeft)
    assertEquals(SchemaId.from("schema-v1").map(_.value), Right("schema-v1"))
  }

  test("positive bounds reject their invalid frontier values") {
    assert(ByteLimit.from(0).isLeft)
    assert(DurationMillis.from(-1L).isLeft)
    assert(PositiveInt.from("replicas", 0).isLeft)
    assert(WallTimeMinutes.from(0L).isLeft)
    assertEquals(ByteLimit.from(1).map(_.value), Right(1))
    assertEquals(DurationMillis.from(0L).map(_.value), Right(0L))
  }

  test("attempt epochs preserve retry provenance and refuse overflow") {
    val first = AttemptEpoch.initial
    val second = first.next.toOption.get
    val maximum = AttemptEpoch.from(Long.MaxValue).toOption.get
    assertEquals(first.value, 1L)
    assertEquals(second.value, 2L)
    assert(maximum.next.isLeft)
    assertEquals(
      RetrySafety.values.toVector,
      Vector(
        RetrySafety.Unknown,
        RetrySafety.NoAutomaticRetry,
        RetrySafety.SafeForAutomaticRetry
      )
    )
  }

  test("codec contracts round-trip within bounds and return typed failures beyond them") {
    val samples = Vector("", "ascii", "λ-calculus", "x" * 32)
    samples.foreach { value =>
      val roundTrip = utf8Codec.encode(value).flatMap(utf8Codec.decode)
      assertEquals(roundTrip, Right(value))
    }
    assert(utf8Codec.encode("x" * 33).left.exists(_.code == "input-too-large"))
    assert(
      utf8Codec.decode(ByteVector.fill(33L)(0.toByte)).left.exists(_.code == "input-too-large")
    )
  }

  test("diagnostics and freshness retain structured evidence") {
    val diagnostic = Diagnostic("probe-failed", "site probe failed", Map("site" -> "alpha"))
    val diagnostics = Diagnostics.one(diagnostic)
    val attemptedAt = Instant.parse("2026-01-02T03:04:05Z")
    assert(Diagnostics.fromVector(Vector.empty).isLeft)
    assertEquals(diagnostics.toVector, Vector(diagnostic))
    assertEquals(
      Freshness.Unknown(attemptedAt, diagnostics),
      Freshness.Unknown(attemptedAt, diagnostics)
    )
  }

  test("failure diagnosis preserves primary and contributor provenance") {
    val diagnosis = FailureDiagnosis(
      FailureCause.WorkerFailure("worker-crash"),
      Vector(FailureCause.ProgramFailed(Some(7), Vector("exit-7"))),
      Vector(FailureCause.NodeFailure)
    )
    assertEquals(diagnosis.primary, FailureCause.WorkerFailure("worker-crash"))
    assertEquals(diagnosis.confirmedContributors.size, 1)
    assertEquals(diagnosis.suspectedContributors, Vector(FailureCause.NodeFailure))
  }

  test("atomic digest is the shared content identity and is byte-sensitive") {
    val left = AtomicFiles.digestOf(ByteVector.view("payload-a".getBytes(StandardCharsets.UTF_8)))
    val same = AtomicFiles.digestOf(ByteVector.view("payload-a".getBytes(StandardCharsets.UTF_8)))
    val different =
      AtomicFiles.digestOf(ByteVector.view("payload-b".getBytes(StandardCharsets.UTF_8)))
    assertEquals(left, same)
    assertNotEquals(left, different)
    assert(left.value.matches("sha256:[0-9a-f]{64}"))
  }
