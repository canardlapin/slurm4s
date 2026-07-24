package io.github.bbuchsbaum.scalaslurm.worker

import cats.effect.IO
import io.github.bbuchsbaum.scalaslurm.core.*

import java.nio.charset.StandardCharsets
import scala.util.Try

class SlurmTaskSuite extends munit.FunSuite:
  test("a task call lowers to a typed request without exposing payload construction") {
    val resources = ResourceRequest
      .validate(1, 1, None, None, None)
      .toEither
      .fold(problem => fail(problem.toString), identity)
    val key = SubmissionKey.from("increment-41").fold(problem => fail(problem.toString), identity)
    val name = JobName.from("increment").fold(problem => fail(problem.toString), identity)

    val request =
      Increment(41).request(key, name, resources, ByteLimit.defaultEvidence)

    assertEquals(request.retrySafety, RetrySafety.SafeForAutomaticRetry)
    request.payload match
      case Payload.RegisteredTask(operation, input, inputCodec, contract) =>
        assertEquals(operation.descriptor, Increment.operation.descriptor)
        assertEquals(
          inputCodec.encode(input),
          Right("41".getBytes(StandardCharsets.UTF_8).toVector)
        )
        assertEquals(contract.descriptor.schema, Some(Increment.outputCodec.schemaId))
      case other => fail(s"expected a registered task, received $other")
  }

  private object Increment extends SlurmTask[Int, Int]:
    val operation: OperationRef[Int, Int] = OperationRef(
      OperationId.from("example.increment").fold(problem => fail(problem.toString), identity),
      OperationVersion.from("1").fold(problem => fail(problem.toString), identity),
      SchemaId.from("example.int-input.v1").fold(problem => fail(problem.toString), identity),
      ResultSchemaId
        .from("example.int-result.v1")
        .fold(problem => fail(problem.toString), identity)
    )

    val inputCodec: InputCodec[Int] = new InputCodec[Int]:
      val schemaId: SchemaId = operation.inputSchema
      def encode(value: Int): Either[ResultCodecFailure, Vector[Byte]] =
        Right(value.toString.getBytes(StandardCharsets.UTF_8).toVector)
      def decode(bytes: Vector[Byte]): Either[ResultCodecFailure, Int] =
        decodeInt(bytes)

    val outputCodec: ResultCodec[Int] = new ResultCodec[Int]:
      val schemaId: ResultSchemaId = operation.outputSchema
      def encode(value: Int): Either[ResultCodecFailure, Vector[Byte]] =
        inputCodec.encode(value)
      def decode(bytes: Vector[Byte]): Either[ResultCodecFailure, Int] =
        decodeInt(bytes)

    override val retrySafety: RetrySafety = RetrySafety.SafeForAutomaticRetry

    def run(input: Int, context: TaskContext[IO]): IO[Int] =
      IO.pure(input + 1)

    private def decodeInt(bytes: Vector[Byte]): Either[ResultCodecFailure, Int] =
      Try(new String(bytes.toArray, StandardCharsets.UTF_8).toInt).toEither.left.map(error =>
        ResultCodecFailure("invalid-int", error.getMessage)
      )
