package io.github.bbuchsbaum.slurm4s.core

import scodec.bits.ByteVector

/** P8.D1: lowering refuses what it cannot carry instead of quietly rewriting it.
  *
  * The SSH adapter used to accept any `JobRequest[A]`, replace its result contract with `ExitOnly`,
  * and submit successfully — so a caller who asked for a structured result got a job that had
  * silently discarded the request. `Scheduler.submit` no longer claims to honour `A` at all, and
  * the lowering that produces a `LaunchSpec` either preserves the contract or fails loudly.
  */
class LaunchSpecSuite extends munit.FunSuite:

  private val resources = ResourceRequest.validate(1, 1, None, None, None).toEither.toOption.get

  private def script[A](contract: ResultContract[A]): JobRequest[A] =
    JobRequest(
      SubmissionKey.unsafeFrom("launch-spec"),
      JobName.unsafeFrom("launch-spec"),
      Payload.Script(ScriptSource.ExistingRemote("/work/job.sh"), Vector("--flag"), contract),
      resources
    )

  test("lowering preserves the declared result contract rather than replacing it") {
    val outputs = ResultContract.DeclaredOutputs
      .from(Vector(RelativeOutputPath.unsafeFrom("results/out.txt")))
      .toOption
      .get
    val spec = LaunchSpec.fromRequest(script(outputs)).toOption.get

    assertEquals(spec.resultContract, outputs.descriptor)
    assertEquals(spec.resultContract.mode, ResultMode.DeclaredOutputs)
    assertEquals(spec.arguments, Vector("--flag"))
  }

  test("an exit-only request lowers to an exit-only specification") {
    val spec = LaunchSpec.fromRequest(script(ResultContract.ExitOnly)).toOption.get

    assertEquals(spec.resultContract, ResultContract.ExitOnly.descriptor)
  }

  test("a registered task is refused rather than lowered to a bare script") {
    val task = JobRequest(
      SubmissionKey.unsafeFrom("registered"),
      JobName.unsafeFrom("registered"),
      Payload.RegisteredTask(
        OperationRef(
          OperationId.unsafeFrom("example.increment"),
          OperationVersion.unsafeFrom("1"),
          SchemaId.unsafeFrom("example.in.v1"),
          ResultSchemaId.unsafeFrom("example.out.v1")
        ),
        1,
        intCodec,
        ResultContract.ExitOnly
      ),
      resources
    )

    LaunchSpec.fromRequest(task) match
      case Left(diagnostics) =>
        assertEquals(diagnostics.values.head.code, "launch-spec-not-lowerable")
      case Right(spec) =>
        fail(s"a registered task must not be silently rendered as a script: $spec")
  }

  private val intCodec: InputCodec[Int] = new InputCodec[Int]:
    def schemaId: SchemaId = SchemaId.unsafeFrom("example.in.v1")
    def encode(value: Int): Either[ResultCodecFailure, ByteVector] =
      Right(ByteVector.view(value.toString.getBytes("UTF-8")))
    def decode(bytes: ByteVector): Either[ResultCodecFailure, Int] =
      Right(new String(bytes.toArray, "UTF-8").toInt)
