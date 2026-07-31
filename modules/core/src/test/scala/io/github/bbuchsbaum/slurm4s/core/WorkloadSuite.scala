package io.github.bbuchsbaum.slurm4s.core

class WorkloadSuite extends munit.FunSuite:
  test("opaque scripts make exit-only meaning explicit") {
    val payload = Payload.Script(
      source = ScriptSource.Inline("analysis.R", "quit(status = 0)".getBytes.toVector),
      arguments = Vector("--vanilla"),
      resultContract = ResultContract.ExitOnly
    )

    assertEquals(payload.resultContract.descriptor.mode, ResultMode.ExitOnly)
    assertEquals(payload.resultContract.descriptor.schema, None)
  }

  test("declared output contracts require at least one safe relative path") {
    val limit = ByteLimit.from(65536).toOption.get
    val output = RelativeOutputPath.from("results/model.rds").toOption.get

    assert(RelativeOutputPath.from("../secret").isLeft)
    assert(ResultContract.DeclaredOutputs.from(Vector.empty, limit).isLeft)
    assert(ResultContract.DeclaredOutputs.from(Vector(output), limit).isRight)
    assert(ResultContract.DeclaredOutputs.from(Vector(output, output), limit).isLeft)

    val codec = new ResultCodec[String]:
      val schemaId: ResultSchemaId = ResultSchemaId.from("example.text.v1").toOption.get
      def encode(_value: String): Either[ResultCodecFailure, Vector[Byte]] = Right(Vector.empty)
      def decode(_bytes: Vector[Byte]): Either[ResultCodecFailure, String] = Right("")
    assert(ResultContract.Structured.from(codec, limit, Vector(output, output)).isLeft)
  }

  test("output validation distinguishes missing, extra, size, and digest failures") {
    val expected = RelativeOutputPath.from("results/model.rds").toOption.get
    val extra = RelativeOutputPath.from("results/debug.txt").toOption.get
    val firstDigest = ContentDigest
      .from("sha256:a7937b64b8caa58f03721bb6bacf5c78cb235febe0e70b1b84cd99541461a08e")
      .toOption
      .get
    val secondDigest = ContentDigest
      .from("sha256:16367aacb67a4a017c8da8ab95682ccb390863780f7114dda0a0e0c55644c7c4")
      .toOption
      .get
    val reported = OutputEntry.from(expected, 12L, firstDigest).toOption.get
    val actual = OutputEntry.from(expected, 13L, secondDigest).toOption.get
    val unexpected = OutputEntry.from(extra, 1L, firstDigest).toOption.get

    val failures = OutputValidation
      .verify(Vector(expected), Vector(reported, unexpected), Vector(actual, unexpected))
      .left
      .toOption
      .get
      .toVector

    assert(failures.exists(_.isInstanceOf[OutputValidationFailure.Unexpected]))
    assert(failures.exists(_.isInstanceOf[OutputValidationFailure.SizeMismatch]))
    assert(failures.exists(_.isInstanceOf[OutputValidationFailure.DigestMismatch]))
    assert(
      OutputValidation
        .verify(Vector(expected), Vector.empty, Vector.empty)
        .left
        .exists(_.toVector.contains(OutputValidationFailure.Missing(expected)))
    )
  }
