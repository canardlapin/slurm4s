package io.github.bbuchsbaum.remoteexec.kernel

/** P8.G1: a literal identifier is checked where it is written.
  *
  * The flagship typed-task guide opened with four consecutive `.from(...).toOption.get` calls,
  * teaching partial-function unwrapping as the idiom for values that are known at compile time.
  */
class LiteralIdentifierSuite extends munit.FunSuite:

  test("a valid literal constructs without ceremony") {
    val operation = OperationId("example.increment")
    val version = OperationVersion("1")
    val schema = SchemaId("example.int-input.v1")
    val result = ResultSchemaId("example.int-result.v1")

    assertEquals(operation.value, "example.increment")
    assertEquals(version.value, "1")
    assertEquals(schema.value, "example.int-input.v1")
    assertEquals(result.value, "example.int-result.v1")
  }

  test("a literal agrees with the runtime constructor") {
    assertEquals(
      OperationId("example.increment"),
      OperationId.from("example.increment").toOption.get
    )
  }

  test("an invalid literal is a compile error, not a runtime throw") {
    assert(
      compileErrors("""OperationId("has space")""").contains("invalid operationId literal"),
      compileErrors("""OperationId("has space")""")
    )
    assert(compileErrors("""SchemaId("")""").nonEmpty)
  }

  test("a dynamic value is still an Either, because it can still fail") {
    val dynamic = "runtime value"

    assert(compileErrors("OperationId(dynamic)").contains("only a string literal"))
    assert(OperationId.from(dynamic).isLeft)
  }
